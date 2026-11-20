from sqlalchemy import Column,Integer,BigInteger,String,JSON,ForeignKey,UniqueConstraint
from ..config.database import Base
class ImportStagedRecord(Base):
    """校验通过的暂存样本：仅在发布事务提交后才进入 reading_sessions，研究中途不可见。"""
    __tablename__='import_staged_records'
    __table_args__=(UniqueConstraint('job_id','line_no',name='uq_staged_line'),)
    id=Column(Integer,primary_key=True)
    job_id=Column(Integer,ForeignKey('import_jobs.id'),nullable=False,index=True)
    line_no=Column(BigInteger,nullable=False)
    reference=Column(String(120),nullable=False)
    session_key=Column(String(120),nullable=False,index=True)
    values=Column(JSON,nullable=False)
    created_at=Column(String(40),nullable=False)
