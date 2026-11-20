from sqlalchemy import Column,Integer,BigInteger,String,ForeignKey,UniqueConstraint
from ..config.database import Base
class ImportChunk(Base):
    __tablename__='import_chunks'
    __table_args__=(
        UniqueConstraint('job_id','chunk_index',name='uq_import_chunk_index'),
        UniqueConstraint('job_id','offset',name='uq_import_chunk_offset'),
    )
    id=Column(Integer,primary_key=True)
    job_id=Column(Integer,ForeignKey('import_jobs.id'),nullable=False)
    chunk_index=Column(Integer,nullable=False)
    offset=Column(BigInteger,nullable=False)
    length=Column(Integer,nullable=False)
    sha256=Column(String(64),nullable=False)
    server_sha256=Column(String(64),nullable=False)
    created_at=Column(String(40),nullable=False)
