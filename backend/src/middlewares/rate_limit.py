import time
from collections import defaultdict,deque
from fastapi.responses import JSONResponse
buckets=defaultdict(deque)
async def rate_limit(request,call_next):
    if request.method in ('POST','PUT','DELETE'):
        # 分片追加上传按 600/分钟放行，其余写请求维持 90/分钟
        limit=600 if '/chunks/' in request.url.path else 90
        key=(request.client.host,request.url.path); t=time.monotonic();q=buckets[key]
        while q and t-q[0]>60:q.popleft()
        if len(q)>=limit:return JSONResponse({'detail':'一分钟内操作过多，请稍后重试'},status_code=429)
        q.append(t)
    return await call_next(request)
