from sqlalchemy import Column,Integer,BigInteger,String,Text,ForeignKey
from ..config.database import Base
class ImportJob(Base):
    __tablename__='import_jobs'
    id=Column(Integer,primary_key=True)
    source_id=Column(Integer,ForeignKey('reading_collections.id'),nullable=False)
    filename=Column(String(255),nullable=False)
    # receiving 接收中 / received 接收完成待校验 / validating 校验中 /
    # validated 校验通过待发布 / publishing 发布中 / published 已发布 / failed 整批失败
    state=Column(String(24),default='receiving',nullable=False)
    total_size=Column(BigInteger,nullable=True)
    total_chunks=Column(Integer,nullable=True)
    received_bytes=Column(BigInteger,default=0,nullable=False)
    received_chunks=Column(Integer,default=0,nullable=False)
    checksum_sha256=Column(String(64),default='')
    file_sha256=Column(String(64),default='')
    total_rows=Column(Integer,default=0,nullable=False)
    valid_rows=Column(Integer,default=0,nullable=False)
    bad_rows=Column(Integer,default=0,nullable=False)
    duplicate_rows=Column(Integer,default=0,nullable=False)
    conflict_rows=Column(Integer,default=0,nullable=False)
    error=Column(Text,default='')
    created_by=Column(String(120),nullable=False)
    created_at=Column(String(40),nullable=False)
    updated_at=Column(String(40),nullable=False)
