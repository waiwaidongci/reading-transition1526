"""校验阶段边界用例：空文件、仅表头、引号未闭合、超大行、跨块同会话冲突。"""
import os,tempfile
_TMP=tempfile.mkdtemp(prefix='import-edge-')
os.environ['DATABASE_URL']='sqlite:///%s/edge.db'%_TMP
os.environ['ARTIFACT_DIR']=_TMP+'/art'
os.environ['IMPORT_DIR']=_TMP+'/art/imports'
os.environ['REDIS_URL']='redis://127.0.0.1:6399/0'
os.environ['SEED_DEMO']='false'
os.environ['MAX_LINE_BYTES']='200'  # 调小上限便于测试

import pytest
from src.config.database import Base,engine,Session
from src.config import settings
from src.models.user import User
from src.models.source import Source
from src.models.import_job import ImportJob
from src.utils.security import hash_password
from src.utils.serialization import now
from src.services.imports import data_path
from src.services.import_processing import run_validate

Base.metadata.create_all(engine)

def _job(content,job_id):
    with Session() as db:
        if not db.get(Source,1):
            db.add(User(email='a@b.c',password_hash='x',role='admin'))
            db.add(Source(id=1,key='K',name='n',filename='f',description='d'))
        db.add(ImportJob(id=job_id,source_id=1,filename='x',state='received',total_size=1,total_chunks=1,
                         received_bytes=1,received_chunks=1,created_by='a',created_at=now(),updated_at=now()))
        db.commit()
    p=data_path(job_id);p.parent.mkdir(parents=True,exist_ok=True)
    with open(p,'wb') as fh:fh.write(content)

@pytest.fixture(autouse=True)
def reset():
    Base.metadata.drop_all(engine);Base.metadata.create_all(engine)
    import shutil
    if settings.IMPORT_DIR.exists():shutil.rmtree(settings.IMPORT_DIR)
    settings.IMPORT_DIR.mkdir(parents=True)

def _state(job_id):
    run_validate(job_id)
    with Session() as db:
        j=db.get(ImportJob,job_id)
        return {k:getattr(j,k) for k in ('state','total_rows','valid_rows','bad_rows','conflict_rows','error')}

def test_empty_file_fails_batch():
    _job(b'',1)
    s=_state(1)
    assert s['state']=='failed' and '没有任何合格样本' in s['error']

def test_header_only_fails_batch():
    _job('session,path\n'.encode(),2)
    s=_state(2)
    assert s['state']=='failed' and s['total_rows']==0

def test_unclosed_quote_is_bad_row():
    _job(('S-1,序章>材料\nS-2,"未闭合引号\nS-3,案例>附录\n').encode(),3)
    s=_state(3)
    # 未闭合引号吞掉后续换行直到文件末，该行整体隔离；S-1 仍合格
    assert s['valid_rows']==1 and s['bad_rows']==1
    assert s['state']=='validated'

def test_oversized_line_isolated():
    big='S-%d,'%4+('章'*300)+'\n'  # 单字节计远超 200 字节
    _job(('S-1,序章>材料\n'+big).encode(),5)
    s=_state(5)
    assert s['bad_rows']==1 and s['valid_rows']==1

def test_conflict_split_across_blocks():
    # 同一会话的两条异路径记录相隔很远，必然跨越多个 1MB 解析块
    lines=['S-1,序章>材料\n']+['S-%08d,材料>工艺\n'%i for i in range(2,60000)]+['S-1,序章>冲突路径\n']
    _job(('session,path\n'+''.join(lines)).encode(),6)
    s=_state(6)
    assert s['state']=='failed' and s['conflict_rows']==1
