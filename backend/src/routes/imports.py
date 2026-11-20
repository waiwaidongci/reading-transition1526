from fastapi import APIRouter,Depends,Request,HTTPException,Query
from ..config.database import db_session
from ..schemas.import_job import ImportJobCreate,ImportComplete
from ..services.auth import current_user,writer
from ..controllers.imports import ImportsController as C
router=APIRouter()

@router.get('/imports')
def jobs(db=Depends(db_session),u=Depends(current_user)):return C.jobs(db)

@router.post('/imports')
def create(data:ImportJobCreate,db=Depends(db_session),u=Depends(writer)):return C.create(db,u,data)

@router.get('/imports/{job_id}')
def detail(job_id:int,db=Depends(db_session),u=Depends(current_user)):return C.detail(db,job_id)

@router.get('/imports/{job_id}/chunks')
def chunks(job_id:int,db=Depends(db_session),u=Depends(current_user)):return C.chunks(db,job_id)

@router.put('/imports/{job_id}/chunks/{chunk_index}')
async def upload_chunk(job_id:int,chunk_index:int,request:Request,
                       offset:int=Query(...,ge=0),db=Depends(db_session),u=Depends(writer)):
    payload=await request.body()
    if not payload:raise HTTPException(422,'分片内容不能为空')
    job,status=C.upload(db,u,job_id,chunk_index,offset,payload)
    return {'chunk_index':chunk_index,'offset':offset,'length':len(payload),'status':status,
            'received_chunks':job.received_chunks,'received_bytes':job.received_bytes,'state':job.state}

@router.post('/imports/{job_id}/complete')
async def complete(job_id:int,request:Request,db=Depends(db_session),u=Depends(writer)):
    raw=await request.body()
    checksum=''
    if raw:
        checksum=(ImportComplete.model_validate_json(raw).checksum_sha256 or '')
    return C.complete(db,u,job_id,checksum)

@router.post('/imports/{job_id}/validate')
def validate(job_id:int,db=Depends(db_session),u=Depends(writer)):return C.validate(db,u,job_id)

@router.post('/imports/{job_id}/publish')
def publish(job_id:int,db=Depends(db_session),u=Depends(writer)):return C.publish(db,u,job_id)

@router.get('/imports/{job_id}/bad-rows')
def bad_rows(job_id:int,limit:int=Query(200,ge=1,le=1000),offset:int=Query(0,ge=0),
             db=Depends(db_session),u=Depends(current_user)):
    return C.bad_rows(db,job_id,limit,offset)

@router.get('/imports/{job_id}/staged')
def staged(job_id:int,limit:int=Query(100,ge=1,le=1000),offset:int=Query(0,ge=0),
           db=Depends(db_session),u=Depends(current_user)):
    return C.staged(db,job_id,limit,offset)
