"""日志导入 · 校验与发布阶段（独立 worker 执行，可恢复）。

validate：
- 以 1MB 块流式解析落盘文件，内存只持有当前块/当前行/一个落库批次。
- 坏行（字段数、引号、UTF-8、业务校验）写入隔离区，保留原行与原因，不阻断整批。
- 同会话同路径重复行去重计数；同会话异路径、库内同会话异路径使整批失败（failed）。
- 重新校验先清空旧暂存/隔离数据，天然幂等，重试不新增样本。

publish：
- 单事务把暂存样本批量写入 reading_sessions，研究只见整批或完全不见。
- 发布前再次核对库内冲突；任何异常整体回滚，作业保持 failed/validated 可重试。
"""
import logging
from sqlalchemy import select,func
from ..config.database import Session
from ..config.settings import CONFIG,IMPORT_DB_BATCH
from ..models.import_job import ImportJob
from ..models.import_bad_row import ImportBadRow
from ..models.import_staged import ImportStagedRecord
from ..models.record import Record
from ..models.source import Source
from ..utils.serialization import now,row
from ..utils.db_json import json_text
from .audit import log
from .domain_validation import validate_record
from .import_parser import iter_logical_lines,parse_csv_line,looks_like_header

FIELD_KEYS=[f[0] for f in CONFIG['fields']]

def _set(job,**fields):
    for k,v in fields.items():setattr(job,k,v)
    job.updated_at=now()

def _reset_job_outputs(db,job_id):
    db.query(ImportStagedRecord).filter(ImportStagedRecord.job_id==job_id).delete(synchronize_session=False)
    db.query(ImportBadRow).filter(ImportBadRow.job_id==job_id).delete(synchronize_session=False)
    db.flush()

def run_validate(job_id):
    """流式解析+校验。返回最终作业字典。每个阶段独立事务，中断后可重跑。"""
    with Session() as db:
        job=db.get(ImportJob,job_id)
        if not job:
            logging.error('import job %s missing',job_id);return None
        if job.state=='validated':
            return row(job)  # 已完成的校验不重复执行
        if job.state not in ('received','validating'):
            logging.info('job %s state %s skip validate',job_id,job.state);return row(job)
        _set(job,state='validating',error='');db.commit()
    # 清空上次校验产物（幂等重试的关键）
    with Session() as db:
        _reset_job_outputs(db,job_id);db.commit()

    staged_buf=[];bad_buf=[]
    total=valid=bad=duplicate=0
    seen={}  # session_key -> (line_no, normalized_path)
    header_skipped=False
    fail_reasons=[]
    conflict_count=0
    path_file=_data_file(job_id)

    def flush_bad():
        nonlocal bad_buf
        if bad_buf:
            db.add_all([ImportBadRow(job_id=job_id,line_no=ln,raw_line=raw[:60000],reason=rs[:500],created_at=now()) for ln,raw,rs in bad_buf])
            db.commit();bad_buf=[]

    def flush_staged():
        nonlocal staged_buf
        if staged_buf:
            db.add_all([ImportStagedRecord(job_id=job_id,line_no=ln,reference=ref,session_key=sk,values=vals,created_at=now()) for ln,ref,sk,vals in staged_buf])
            db.commit();staged_buf=[]

    with Session() as db:
        for line_no,raw_text,line_error in iter_logical_lines(path_file):
            total+=1
            if line_error:
                bad+=1;bad_buf.append((line_no,raw_text,line_error))
                if len(bad_buf)>=IMPORT_DB_BATCH:flush_bad()
                continue
            if not raw_text.strip():
                continue  # 空行不计入样本也不算坏行
            if not header_skipped:
                header_skipped=True
                probe,perr=parse_csv_line(raw_text.encode('utf-8'))
                if perr is None and looks_like_header(probe,FIELD_KEYS):
                    total-=1;continue
            fields,perr=parse_csv_line(raw_text.encode('utf-8'))
            if perr:
                bad+=1;bad_buf.append((line_no,raw_text,'字段解析失败：'+perr))
                if len(bad_buf)>=IMPORT_DB_BATCH:flush_bad()
                continue
            if len(fields)!=len(FIELD_KEYS):
                bad+=1;bad_buf.append((line_no,raw_text,'字段数应为%d（%s），实际%d'%(len(FIELD_KEYS),','.join(FIELD_KEYS),len(fields))))
                if len(bad_buf)>=IMPORT_DB_BATCH:flush_bad()
                continue
            values={FIELD_KEYS[i]:fields[i] for i in range(len(FIELD_KEYS))}
            try:
                clean=validate_record(values)
            except ValueError as e:
                bad+=1;bad_buf.append((line_no,raw_text,'业务校验失败：'+str(e)[:400]))
                if len(bad_buf)>=IMPORT_DB_BATCH:flush_bad()
                continue
            session_key=clean['session'];normalized_path=clean['path']
            if session_key in seen:
                prev_line,prev_path=seen[session_key]
                if prev_path==normalized_path:
                    duplicate+=1;continue  # 批内完全重复：去重，不重复写入
                conflict_count+=1
                reason='同会话异路径冲突：会话%s 在第%d行路径为「%s」，本行路径为「%s」'%(session_key,prev_line,prev_path,normalized_path)
                bad+=1;bad_buf.append((line_no,raw_text,reason));fail_reasons.append(reason)
                if len(bad_buf)>=IMPORT_DB_BATCH:flush_bad()
                continue
            seen[session_key]=(line_no,normalized_path)
            reference='IMP-%d-L%d'%(job_id,line_no)
            staged_buf.append((line_no,reference,session_key,clean));valid+=1
            if len(staged_buf)>=IMPORT_DB_BATCH:flush_staged()
        flush_staged();flush_bad()

        # 库内同会话异路径检查（与手工追加/其他来源已入库样本对照）
        job=db.get(ImportJob,job_id)
        source=db.get(Source,job.source_id)
        session_expr=json_text('values','session');path_expr=json_text('values','path')
        existing_rows=db.execute(select(session_expr,path_expr).select_from(Record).where(Record.source_id==job.source_id)).all()
        existing={s:p for s,p in existing_rows if s}
        for sk,(ln,p) in seen.items():
            if sk in existing and existing[sk]!=p:
                conflict_count+=1
                reason='库内同会话异路径冲突：会话%s 已存在路径「%s」，导入第%d行路径为「%s」'%(sk,existing[sk],ln,p)
                fail_reasons.append(reason)
                bad+=1
                db.add(ImportBadRow(job_id=job_id,line_no=ln,raw_line='',reason=reason[:500],created_at=now()))
        db.flush()

        _set(job,total_rows=total,valid_rows=valid,bad_rows=bad,duplicate_rows=duplicate,conflict_rows=conflict_count)
        if conflict_count:
            detail='；'.join(fail_reasons[:3])
            if len(fail_reasons)>3:detail+=' 等%d处冲突'%len(fail_reasons)
            _set(job,state='failed',error=('整批失败：存在同会话异路径冲突，已全部隔离，未写入任何样本。'+detail)[:2000])
            log(db,'worker','import.validate_failed',job_id,'%d 冲突 / %d 坏行'%(conflict_count,bad))
        elif valid==0:
            _set(job,state='failed',error='整批失败：没有任何合格样本（总行%d，坏行%d，重复%d）'%(total,bad,duplicate))
            log(db,'worker','import.validate_failed',job_id,'no valid rows')
        else:
            _set(job,state='validated',error='')
            log(db,'worker','import.validated',job_id,'总%d 合格%d 坏%d 重复%d'%(total,valid,bad,duplicate))
        db.commit()
    return get_job_row(job_id)

def _data_file(job_id):
    from .imports import data_path
    return data_path(job_id)

def get_job_row(job_id):
    with Session() as db:
        job=db.get(ImportJob,job_id)
        return row(job) if job else None

def run_publish(job_id):
    """单事务发布暂存样本。崩溃回滚；重跑先清理残留，保证不新增重复样本。"""
    with Session() as db:
        job=db.get(ImportJob,job_id)
        if not job:return None
        if job.state=='published':
            return row(job)
        if job.state!='validated':
            logging.info('job %s state %s skip publish',job_id,job.state);return row(job)
        _set(job,state='publishing',error='');db.commit()

    with Session() as db:
        try:
            job=db.get(ImportJob,job_id)
            # 崩溃重跑兜底：清理本作业此前可能部分写入的样本（同前缀引用）
            db.query(Record).filter(Record.source_id==job.source_id,
                                    Record.reference.like('IMP-%d-L%%'%job_id)).delete(synchronize_session=False)
            # 发布前再次核对库内冲突（导入期间允许手工追加）。库内映射规模与现有样本同量级，
            # 仅保留两列短文本；暂存侧用键集分页，每批 IMPORT_DB_BATCH 行，内存与文件总行数无关。
            session_expr=json_text('values','session');path_expr=json_text('values','path')
            existing_rows=db.execute(select(session_expr,path_expr).select_from(Record).where(Record.source_id==job.source_id)).all()
            existing={s:p for s,p in existing_rows if s}
            conflicts=[];inserted=0;last_id=0
            while True:
                batch=db.scalars(select(ImportStagedRecord)
                                 .where(ImportStagedRecord.job_id==job_id,ImportStagedRecord.id>last_id)
                                 .order_by(ImportStagedRecord.id).limit(IMPORT_DB_BATCH)).all()
                if not batch:break
                payload=[]
                for st in batch:
                    last_id=st.id
                    sk=st.session_key;p=st.values['path']
                    if sk in existing and existing[sk]!=p:
                        conflicts.append((st.line_no,sk,existing[sk],p));continue
                    if sk in existing:
                        continue  # 导入期间手工追加了同会话同路径：跳过，不重复写
                    payload.append(Record(source_id=job.source_id,reference=st.reference,values=st.values))
                    existing[sk]=p
                if payload:
                    db.add_all(payload);db.flush();inserted+=len(payload)
                db.expire_all()  # 释放已处理批次的 ORM 实体，保持内存恒定
            if conflicts:
                for ln,sk,ep,np_ in conflicts:
                    db.add(ImportBadRow(job_id=job_id,line_no=ln,raw_line='',
                                        reason=('发布时库内同会话异路径冲突：会话%s 库内「%s」导入「%s」'%(sk,ep,np_))[:500],created_at=now()))
                job.conflict_rows=job.conflict_rows+len(conflicts)
                _set(job,state='failed',error=('整批失败：发布前发现库内同会话异路径冲突 %d 处，事务已回滚，未写入样本'%len(conflicts))[:2000])
                log(db,'worker','import.publish_failed',job_id,'%d conflicts'%len(conflicts))
                db.commit();return row(job)
            # 全部批次在同一事务内提交：研究只见整批或完全不见
            _set(job,state='published',valid_rows=inserted)
            log(db,'worker','import.published',job_id,'发布 %d 条样本到来源#%d'%(inserted,job.source_id))
            db.commit()
        except Exception as e:
            db.rollback()
            logging.exception('publish failed')
            with Session() as db2:
                j2=db2.get(ImportJob,job_id);_set(j2,state='failed',error=('发布失败已回滚：'+str(e))[:2000])
                log(db2,'worker','import.publish_failed',job_id,str(e)[:1800]);db2.commit()
    return get_job_row(job_id)

def sweep_interrupted():
    """worker 启动清扫：validating/publishing 中断的作业退回可执行状态并重新入队。"""
    commands=[]
    with Session() as db:
        stuck=db.scalars(select(ImportJob).where(ImportJob.state.in_(('validating','publishing')))).all()
        for job in stuck:
            job.state='received' if job.state=='validating' else 'validated'
            stage='validate' if job.state=='received' else 'publish'
            job.updated_at=now();commands.append((job.id,stage))
            log(db,'worker','import.sweep',job.id,'中断恢复 -> '+stage)
        db.commit()
    for job_id,stage in commands:
        try:
            from .import_queue import queue
            queue.rpush('import:queue','%d:%s'%(job_id,stage))
        except Exception:
            logging.exception('requeue failed job %s',job_id)
    return commands
