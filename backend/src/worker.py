import json,time,logging
from redis import Redis
from sqlalchemy import select
from .config.database import Session
from .config.settings import REDIS_URL,ARTIFACT_DIR
from .models.run import Run
from .models import source,record,user,note,audit
from .services.algorithm import calculate
from .services.audit import log
from .services.export import csv_response
from .services.import_processing import run_validate,run_publish,sweep_interrupted
queue=Redis.from_url(REDIS_URL)
ARTIFACT_DIR.mkdir(parents=True,exist_ok=True)
def execute(id):
    with Session() as db:
        r=db.get(Run,id)
        if not r or r.state!='queued':return
        r.state='running';db.commit()
        try:
            result=calculate(r.snapshot['records'],r.parameters)
            json.dumps(result,allow_nan=False)
            r.result=result;r.state='completed'
            response=csv_response(result,'run.csv')
            (ARTIFACT_DIR/f'run-{r.id}.csv').write_bytes(response.body)
            log(db,'worker','run.completed',r.id)
        except Exception as e:
            logging.exception('analysis failed');r.state='failed';r.error=str(e)[:2000]
            log(db,'worker','run.failed',r.id,r.error)
        db.commit()
def execute_import(payload):
    """payload 形如 '12:validate' / '12:publish'。"""
    try:
        job_id,stage=payload.split(':',1);job_id=int(job_id)
    except ValueError:
        logging.error('bad import payload %r',payload);return
    try:
        if stage=='validate':run_validate(job_id)
        elif stage=='publish':run_publish(job_id)
        else:logging.error('unknown import stage %s',stage)
    except Exception:
        logging.exception('import job %s stage %s crashed',job_id,stage)
def main():
    # 启动清扫：上次进程被杀死时 validating/publishing 的作业自动恢复入队
    try:sweep_interrupted()
    except Exception:logging.exception('import sweep failed')
    while True:
        (ARTIFACT_DIR/'heartbeat').write_text(str(time.time()))
        value=queue.blpop(['analysis:queue','import:queue'],timeout=2)
        if not value:continue
        key,raw=value
        if key=='import:queue':execute_import(raw.decode())
        else:execute(int(raw))
if __name__=='__main__':main()
