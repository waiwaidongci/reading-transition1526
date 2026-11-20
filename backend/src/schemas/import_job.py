from pydantic import BaseModel,Field
class ImportJobCreate(BaseModel):
    source_id:int
    filename:str=Field(min_length=1,max_length=255)
    total_size:int=Field(ge=0)
    total_chunks:int=Field(ge=1,le=1000000)
    checksum_sha256:str=Field(default='',max_length=64)
class ImportComplete(BaseModel):
    checksum_sha256:str=Field(default='',max_length=64)
