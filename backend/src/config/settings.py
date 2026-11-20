import json, os
from pathlib import Path
CONFIG=json.loads((Path(__file__).parent / 'domain.json').read_text())
DATABASE_URL=os.getenv('DATABASE_URL','sqlite:///./dev.db')
REDIS_URL=os.getenv('REDIS_URL','redis://localhost:6379/0')
JWT_SECRET=os.getenv('JWT_SECRET','local-only-change-before-deployment')
ARTIFACT_DIR=Path(os.getenv('ARTIFACT_DIR','/artifacts'))
# 百万行日志断点导入：接收落盘目录与流式处理参数
IMPORT_DIR=Path(os.getenv('IMPORT_DIR',str(ARTIFACT_DIR/'imports')))
# 接收：每次请求写盘循环块大小（1MB，内存占用与请求大小解耦）
RECEIVE_CHUNK_BYTES=int(os.getenv('RECEIVE_CHUNK_BYTES',1024*1024))
# 解析：每次从磁盘读取的字节块（1MB，内存上限与文件总大小无关）
PARSE_BLOCK_BYTES=int(os.getenv('PARSE_BLOCK_BYTES',1024*1024))
# 暂存样本/坏行每批落库条数，配合 executemany 控制单事务规模
IMPORT_DB_BATCH=int(os.getenv('IMPORT_DB_BATCH',2000))
# 单逻辑行最大字节数，防止异常文件把内存撑爆
MAX_LINE_BYTES=int(os.getenv('MAX_LINE_BYTES',10*1024*1024))
