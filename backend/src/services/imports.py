"""日志导入 · 接收阶段服务。

职责（全部在 128MB 内存内）：
- 作业创建与状态查询；分片按文件偏移 pwrite 落盘，内存只持有 1MB 写循环缓冲。
- 块台账保证幂等：重复块直接返回已存在；同偏移异值拒收（409），作业标记失败。
- 接收完成后校验覆盖连续性与整文件 SHA256，然后投递校验队列。
"""
import hashlib,os,tempfile
from pathlib import Path
from fastapi import HTTPException
from sqlalchemy import select,func
from ..config.database import Session
from ..config.settings import IMPORT_DIR,RECEIVE_CHUNK_BYTES
from ..models.import_job import ImportJob
from ..models.import_chunk import ImportChunk
from ..models.source import Source
from ..utils.serialization import now,row
from .audit import log

IMPORT_QUEUE='import:queue'
# 仅接收态允许继续追加分片
RECEIVING_STATES={'receiving'}

def job_dir(job_id):
    d=IMPORT_DIR/('job-%d'%job_id);d.mkdir(parents=True,exist_ok=True);return d
def data_path(job_id):
    return job_dir(job_id)/'data.log'

def job_or_404(db,job_id):
    job=db.get(ImportJob,job_id)
    if not job:raise HTTPException(404,'未找到导入作业')
    return job

def _touch(job,**fields):
    for k,v in fields.items():setattr(job,k,v)
    job.updated_at=now()

def create_job(db,user,data):
    source=db.get(Source,data.source_id)
    if not source:raise HTTPException(404,'未找到来源文集，无法创建导入作业')
    job=ImportJob(source_id=data.source_id,filename=data.filename,state='receiving',
                  total_size=data.total_size,total_chunks=data.total_chunks,
                  checksum_sha256=data.checksum_sha256.strip().lower(),
                  received_bytes=0,received_chunks=0,created_by=user.email,
                  created_at=now(),updated_at=now())
    db.add(job);db.flush();job_dir(job.id)
    log(db,user,'import.created',job.id,'%s / %d 字节 / %d 片'%(data.filename,data.total_size,data.total_chunks))
    db.commit()
    return row(job)

def list_jobs(db):
    jobs=db.scalars(select(ImportJob).order_by(ImportJob.id.desc())).all()
    return [row(j) for j in jobs]

def get_job(db,job_id):
    return row(job_or_404(db,job_id))

def list_chunks(db,job_id):
    job_or_404(db,job_id)
    return [row(c) for c in db.scalars(select(ImportChunk).where(ImportChunk.job_id==job_id).order_by(ImportChunk.chunk_index))]

def list_bad_rows(db,job_id,limit=200,offset=0):
    from ..models.import_bad_row import ImportBadRow
    job_or_404(db,job_id)
    total=db.scalar(select(func.count()).select_from(ImportBadRow).where(ImportBadRow.job_id==job_id))
    rows=db.scalars(select(ImportBadRow).where(ImportBadRow.job_id==job_id)
                    .order_by(ImportBadRow.line_no).limit(limit).offset(offset)).all()
    return {'total':total,'limit':limit,'offset':offset,'items':[row(r) for r in rows]}

def list_staged(db,job_id,limit=100,offset=0):
    from ..models.import_staged import ImportStagedRecord
    job_or_404(db,job_id)
    rows=db.scalars(select(ImportStagedRecord).where(ImportStagedRecord.job_id==job_id)
                    .order_by(ImportStagedRecord.line_no).limit(limit).offset(offset)).all()
    return [row(r) for r in rows]

def put_chunk(db,user,job_id,chunk_index,offset,payload):
    """按偏移写入一个分片。payload 为 bytes。返回 (job, 状态描述)。"""
    job=job_or_404(db,job_id)
    if job.state not in RECEIVING_STATES:
        raise HTTPException(409,'作业当前为 %s 状态，不能再追加分片'%job.state)
    if chunk_index<0 or chunk_index>=job.total_chunks:
        raise HTTPException(422,'分片序号超出 0..%d 范围'%(job.total_chunks-1))
    if offset<0 or (job.total_size and offset>=job.total_size and payload):
        raise HTTPException(422,'分片偏移超出文件总大小')
    if job.total_size and offset+len(payload)>job.total_size:
        raise HTTPException(422,'分片末端超出文件声明总大小')
    # 先算服务端摘要，内存中只有当前请求的分片
    server_hash=hashlib.sha256(payload).hexdigest()
    existing=db.scalar(select(ImportChunk).where(ImportChunk.job_id==job_id,ImportChunk.chunk_index==chunk_index))
    if existing:
        if existing.server_sha256==server_hash and existing.offset==offset and existing.length==len(payload):
            return job,'duplicate'
        _fail_job(db,user,job,'同偏移/同序号分片内容不一致，已拒收（序号%d）'%chunk_index)
        db.commit()
        raise HTTPException(409,'同偏移分片内容不一致：该作业已拒收并终止')
    # 同偏移异序号同样属于同偏移异值
    same_offset=db.scalar(select(ImportChunk).where(ImportChunk.job_id==job_id,ImportChunk.offset==offset))
    if same_offset:
        _fail_job(db,user,job,'偏移%d 已存在不同序号分片，已拒收'%offset)
        db.commit()
        raise HTTPException(409,'同一文件偏移已被其他分片占用，作业终止')
    target=data_path(job_id)
    tmp=tempfile.NamedTemporaryFile(dir=job_dir(job_id),delete=False)
    try:
        # 流式落临时文件（防御超大请求体，分块写）
        mv=memoryview(payload);pos=0
        while pos<len(mv):
            tmp.write(mv[pos:pos+RECEIVE_CHUNK_BYTES]);pos+=RECEIVE_CHUNK_BYTES
        tmp.flush();os.fsync(tmp.fileno());tmp.close()
        # 校验落盘后的临时文件摘要，再按偏移拼接到数据文件
        h=hashlib.sha256()
        with open(tmp.name,'rb') as fh:
            while True:
                block=fh.read(RECEIVE_CHUNK_BYTES)
                if not block:break
                h.update(block)
        if h.hexdigest()!=server_hash:
            raise HTTPException(500,'分片落盘校验失败，请重试该分片')
        # 按偏移拼接到数据文件（r+b 保留已写区域，不存在则新建）
        mode='r+b' if target.exists() else 'wb'
        with open(target,mode) as dfh:
            dfh.seek(offset)
            with open(tmp.name,'rb') as sfh:
                while True:
                    block=sfh.read(RECEIVE_CHUNK_BYTES)
                    if not block:break
                    dfh.write(block)
            dfh.flush();os.fsync(dfh.fileno())
    finally:
        try:os.unlink(tmp.name)
        except OSError:pass
    chunk=ImportChunk(job_id=job_id,chunk_index=chunk_index,offset=offset,length=len(payload),
                      sha256=server_hash,server_sha256=server_hash,created_at=now())
    db.add(chunk)
    job.received_chunks+=1
    job.received_bytes=db.scalar(select(func.coalesce(func.sum(ImportChunk.length),0)).where(ImportChunk.job_id==job_id))
    _touch(job)
    log(db,user,'import.chunk',job_id,'#%d @%d %dB'%(chunk_index,offset,len(payload)))
    db.commit()
    return job,'stored'

def _fail_job(db,user,job,reason):
    _touch(job,state='failed',error=reason[:2000])
    log(db,user,'import.failed',job.id,reason[:1800])

def complete_receiving(db,user,job_id,client_checksum=''):
    """接收完成：校验覆盖连续性、文件大小与摘要，通过后投递校验队列。

    接收阶段的失败（校验和/大小不符）允许带正确参数重新完成；
    分片内容冲突造成的 failed 不允许复活（其数据已不可信）。
    """
    job=job_or_404(db,job_id)
    if job.state=='failed':
        if job.received_chunks!=job.total_chunks or not data_path(job_id).exists():
            raise HTTPException(409,'作业因分片内容冲突终止，不能重新完成；请新建导入作业')
        # 接收完整但摘要/大小类失败：允许重试
        _touch(job,state='receiving',error='');db.commit();db.refresh(job)
    elif job.state!='receiving':
        raise HTTPException(409,'作业当前为 %s 状态，无需完成接收'%job.state)
    chunks=db.scalars(select(ImportChunk).where(ImportChunk.job_id==job_id).order_by(ImportChunk.offset)).all()
    if len(chunks)!=job.total_chunks:
        raise HTTPException(422,'分片不齐：应 %d 片，实收 %d 片'%(job.total_chunks,len(chunks)))
    # 偏移连续且无重叠覆盖到文件末尾
    cursor=0
    for c in chunks:
        if c.offset!=cursor:
            _fail_job(db,user,job,'分片偏移不连续：期望@%d 实际@%d'%(cursor,c.offset));db.commit()
            raise HTTPException(422,'分片偏移不连续，存在缺口或重叠，作业已失败')
        cursor=c.offset+c.length
    if job.total_size and cursor!=job.total_size:
        _fail_job(db,user,job,'文件大小不符：期望 %d 实际 %d'%(job.total_size,cursor));db.commit()
        raise HTTPException(422,'拼合文件大小与声明总大小不一致，作业已失败')
    target=data_path(job_id)
    if not target.exists() or target.stat().st_size!=cursor:
        _fail_job(db,user,job,'落盘文件缺失或大小异常');db.commit()
        raise HTTPException(422,'落盘文件缺失或大小异常，作业已失败')
    h=hashlib.sha256()
    with open(target,'rb') as fh:
        while True:
            block=fh.read(RECEIVE_CHUNK_BYTES)
            if not block:break
            h.update(block)
    actual=h.hexdigest()
    expected=(client_checksum or job.checksum_sha256).strip().lower()
    if expected and expected!=actual:
        _fail_job(db,user,job,'整文件SHA256不一致：期望 %s 实际 %s'%(expected,actual));db.commit()
        raise HTTPException(409,'整文件校验和不一致，作业已失败')
    job.file_sha256=actual
    _touch(job,state='received')
    log(db,user,'import.received',job.id,'%d 字节 sha256=%s'%(cursor,actual[:16]))
    db.commit()
    _enqueue(job.id,'validate')
    return row(job)

def _enqueue(job_id,stage):
    # 延迟导入便于测试 monkeypatch；队列不可用时作业仍可通过清扫/手动重试恢复
    from .import_queue import queue
    try:
        queue.rpush(IMPORT_QUEUE,'%d:%s'%(job_id,stage))
    except Exception:
        pass

def trigger_validate(db,user,job_id):
    job=job_or_404(db,job_id)
    if job.state not in ('received','failed'):
        raise HTTPException(409,'仅接收完成或失败的作业可以重新校验，当前 %s'%job.state)
    if job.state=='failed' and not data_path(job_id).exists():
        raise HTTPException(422,'落盘文件已缺失，无法重试')
    _touch(job,state='received',error='')
    log(db,user,'import.retry_validate',job.id)
    db.commit();_enqueue(job.id,'validate');return row(job)

def trigger_publish(db,user,job_id):
    job=job_or_404(db,job_id)
    if job.state!='validated':
        raise HTTPException(409,'仅校验通过的作业可以发布，当前 %s'%job.state)
    _enqueue(job.id,'publish')
    log(db,user,'import.publish_requested',job.id)
    db.commit();return row(job)
