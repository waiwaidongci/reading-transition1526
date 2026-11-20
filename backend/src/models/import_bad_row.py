from sqlalchemy import Column,Integer,BigInteger,String,Text,ForeignKey
from ..config.database import Base
class ImportBadRow(Base):
    """坏行隔离区：保留原始行内容与拒收原因，整批失败的冲突行也在此留痕。"""
    __tablename__='import_bad_rows'
    id=Column(Integer,primary_key=True)
    job_id=Column(Integer,ForeignKey('import_jobs.id'),nullable=False,index=True)
    line_no=Column(BigInteger,nullable=False)
    raw_line=Column(Text,nullable=False)
    reason=Column(String(500),nullable=False)
    created_at=Column(String(40),nullable=False)
