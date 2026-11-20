"""端到端测试：HTTP 接收 + 偏移续传 + 流式校验 + 事务发布 + 崩溃恢复。

运行（无 Redis/MySQL 依赖，SQLite + 内存假队列）：
  DATABASE_URL=sqlite:////tmp/e2e.db python -m pytest tests/test_import_flow.py -s
环境变量必须在导入 src 之前设置。
"""
import os,tempfile,shutil
_TMP=tempfile.mkdtemp(prefix='import-e2e-')
os.environ['DATABASE_URL']='sqlite:///%s/e2e.db'%_TMP
os.environ['ARTIFACT_DIR']=_TMP+'/art'
os.environ['IMPORT_DIR']=_TMP+'/art/imports'
os.environ['REDIS_URL']='redis://127.0.0.1:6399/0'
os.environ['SEED_DEMO']='false'

import hashlib
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from src.main import app
from src.config.database import Base,engine,Session
from src.models.user import User
from src.models.source import Source
from src.models.import_job import ImportJob
from src.models.import_chunk import ImportChunk
from src.models.import_bad_row import ImportBadRow
from src.models.import_staged import ImportStagedRecord
from src.models.record import Record
from src.utils.security import hash_password
import src.services.import_queue as iq
import src.services.imports as svc
import src.services.runs as runs

class FakeRedis:
    def __init__(self):self.items=[]
    def rpush(self,k,v):self.items.append((k,v))
    def blpop(self,keys,timeout=2):return None
    def ping(self):return True

fake=FakeRedis()
iq.queue=fake;runs.queue=fake
svc._enqueue=lambda job_id,stage:fake.rpush('import:queue','%d:%s'%(job_id,stage))

@pytest.fixture()
def client():
    Base.metadata.drop_all(engine);Base.metadata.create_all(engine)
    fake.items.clear()
    # 数据库重建后作业 id 从 1 开始，落盘目录也必须清空，否则旧 data.log 残留字节会污染新作业
    from src.config.settings import IMPORT_DIR
    if IMPORT_DIR.exists():shutil.rmtree(IMPORT_DIR)
    IMPORT_DIR.mkdir(parents=True,exist_ok=True)
    with Session() as db:
        db.add(User(email='admin@reading-transition.demo',password_hash=hash_password('Research2026!'),role='admin'))
        db.add(User(email='viewer@reading-transition.demo',password_hash=hash_password('Research2026!'),role='viewer'))
        db.add(Source(id=1,key='SY-T1',name='测试文集',filename='t.csv',description='d'))
        db.commit()
    c=TestClient(app)
    r=c.post('/auth/login',json={'email':'admin@reading-transition.demo','password':'Research2026!'})
    assert r.status_code==200
    c.headers['Authorization']='Bearer '+r.json()['token']
    c.fake=fake
    yield c

def create_and_upload(c,payload,chunk_size,out_of_order=False):
    total_size=len(payload)
    n=(total_size+chunk_size-1)//chunk_size
    r=c.post('/imports',json={'source_id':1,'filename':'reading.log','total_size':total_size,'total_chunks':n})
    assert r.status_code==200,r.text
    job_id=r.json()['id']
    order=list(range(n))
    if out_of_order:order=order[::-1]
    for idx in order:
        chunk=payload[idx*chunk_size:(idx+1)*chunk_size]
        resp=c.put('/imports/%d/chunks/%d?offset=%d'%(job_id,idx,idx*chunk_size),content=chunk,
                   headers={'Content-Type':'application/octet-stream'})
        assert resp.status_code==200,resp.text
    return job_id

def drain_queue():
    from src.services.import_processing import run_validate,run_publish
    jobs=[(int(raw.split(':')[0]),raw.split(':')[1]) for _,raw in list(fake.items)]
    fake.items.clear()
    for job_id,stage in jobs:
        if stage=='validate':run_validate(job_id)
        elif stage=='publish':run_publish(job_id)

def test_full_flow_resume_and_publish(client):
    c=client
    content=('session,path\n'
             'S-001,序章>材料\n'
             'S-002,"第一章\n第二章>附录"\n'
             'S-003,案例>附录\n'
             'S-003,案例>附录\n'
             'S-004-缺列\n'
             'S-005,>空章节\n').encode('utf-8')
    job_id=create_and_upload(c,content,7,out_of_order=True)
    assert c.post('/imports/%d/complete'%job_id).status_code==200
    drain_queue()
    job=c.get('/imports/%d'%job_id).json()
    assert job['state']=='validated',job
    assert job['total_rows']==6
    assert job['valid_rows']==3
    assert job['duplicate_rows']==1
    assert job['bad_rows']==2
    bad=c.get('/imports/%d/bad-rows'%job_id).json()['items']
    assert len(bad)==2 and all(b['raw_line'] and b['reason'] for b in bad)
    assert c.get('/sources/1/records').json()==[]
    assert c.post('/imports/%d/publish'%job_id).status_code==200
    drain_queue()
    assert c.get('/imports/%d'%job_id).json()['state']=='published'
    records=c.get('/sources/1/records').json()
    assert len(records)==3
    s2=[r for r in records if r['values']['session']=='S-002'][0]
    assert s2['values']['path']=='第一章\n第二章>附录'

def test_duplicate_chunk_idempotent_and_same_offset_conflict(client):
    c=client
    payload='S-100,序章>材料\nS-101,材料>工艺\n'.encode('utf-8')
    n=4;size=len(payload);cs=(size+n-1)//n
    r=c.post('/imports',json={'source_id':1,'filename':'a.log','total_size':size,'total_chunks':n})
    job_id=r.json()['id']
    chunks=[payload[i*cs:(i+1)*cs] for i in range(n)]
    for i,ch in enumerate(chunks):
        assert c.put('/imports/%d/chunks/%d?offset=%d'%(job_id,i,i*cs),content=ch).json()['status']=='stored'
    assert c.put('/imports/%d/chunks/0?offset=0'%job_id,content=chunks[0]).json()['status']=='duplicate'
    assert len(c.get('/imports/%d/chunks'%job_id).json())==n
    evil=bytes([chunks[1][0]^0x01])+chunks[1][1:]
    assert c.put('/imports/%d/chunks/1?offset=%d'%(job_id,cs),content=evil).status_code==409
    assert c.get('/imports/%d'%job_id).json()['state']=='failed'

def test_intra_file_session_path_conflict_fails_batch(client):
    c=client
    content=('S-200,序章>材料\nS-201,材料>工艺\nS-200,序章>不同路径\n').encode()
    job_id=create_and_upload(c,content,5)
    c.post('/imports/%d/complete'%job_id);drain_queue()
    job=c.get('/imports/%d'%job_id).json()
    assert job['state']=='failed' and job['conflict_rows']==1 and '同会话异路径' in job['error']
    assert c.get('/sources/1/records').json()==[]
    bad=c.get('/imports/%d/bad-rows'%job_id).json()['items']
    assert any('同会话异路径' in b['reason'] for b in bad)

def test_library_conflict_and_manual_append_during_import(client):
    c=client
    assert c.post('/sources/1/records',json={'records':[{'reference':'MANUAL-1',
        'values':{'session':'S-300','path':'序章>材料'}}]}).status_code==200
    content=('S-300,序章>材料\nS-301,案例>附录\n').encode()
    job_id=create_and_upload(c,content,4)
    c.post('/imports/%d/complete'%job_id);drain_queue()
    assert c.get('/imports/%d'%job_id).json()['state']=='validated'
    assert c.post('/sources/1/records',json={'records':[{'reference':'MANUAL-2',
        'values':{'session':'S-301','path':'不同>路径'}}]}).status_code==200
    c.post('/imports/%d/publish'%job_id);drain_queue()
    job=c.get('/imports/%d'%job_id).json()
    assert job['state']=='failed' and '库内同会话异路径' in job['error']
    assert {x['reference'] for x in c.get('/sources/1/records').json()}=={'MANUAL-1','MANUAL-2'}

def test_checksum_mismatch_rejected_and_retry_idempotent(client):
    c=client
    payload='S-400,序章>材料\nS-401,材料>工艺\n'.encode('utf-8')
    job_id=create_and_upload(c,payload,6)
    assert c.post('/imports/%d/complete'%job_id,json={'checksum_sha256':'f'*64}).status_code==409
    assert c.get('/imports/%d'%job_id).json()['state']=='failed'
    good=hashlib.sha256(payload).hexdigest()
    assert c.post('/imports/%d/complete'%job_id,json={'checksum_sha256':good}).status_code==200
    drain_queue()
    job=c.get('/imports/%d'%job_id).json()
    assert job['state']=='validated' and job['valid_rows']==2
    from src.services.import_processing import run_validate
    run_validate(job_id)
    assert c.get('/imports/%d'%job_id).json()['valid_rows']==2
    with Session() as db:
        assert db.query(ImportStagedRecord).count()==2

def test_crash_recovery_sweep(client):
    c=client
    payload='S-500,序章>材料\nS-501,材料>工艺\nS-502,案例\n'.encode('utf-8')
    job_id=create_and_upload(c,payload,5)
    c.post('/imports/%d/complete'%job_id)
    with Session() as db:
        db.get(ImportJob,job_id).state='validating';db.commit()
    from src.services.import_processing import sweep_interrupted
    assert (job_id,'validate') in sweep_interrupted()
    drain_queue()
    assert c.get('/imports/%d'%job_id).json()['state']=='validated'
    c.post('/imports/%d/publish'%job_id)
    with Session() as db:
        db.get(ImportJob,job_id).state='publishing';db.commit()
    assert (job_id,'publish') in sweep_interrupted()
    drain_queue()
    assert c.get('/imports/%d'%job_id).json()['state']=='published'
    drain_queue()
    assert len(c.get('/sources/1/records').json())==3

def test_viewer_cannot_upload(client):
    r=client.post('/auth/login',json={'email':'viewer@reading-transition.demo','password':'Research2026!'})
    viewer=TestClient(app)
    viewer.headers['Authorization']='Bearer '+r.json()['token']
    assert viewer.post('/imports',json={'source_id':1,'filename':'x','total_size':1,'total_chunks':1}).status_code==403
