"""百万行级压力验证：解析阶段峰值内存、吞吐、结果正确性。

生成约 100 万行 CSV（含中文与少量引号内换行），以 1MB 块流式解析，
用 resource 监控峰值 RSS，并校验行数/去重/坏行隔离口径。
"""
import os,resource,tempfile,time,sys
sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.services.import_parser import iter_logical_lines,parse_csv_line
from src.services.domain_validation import validate_record

N=1_000_000
tmp=tempfile.mkdtemp(prefix='import-perf-')
path=os.path.join(tmp,'million.log')
t0=time.time()
with open(path,'w',encoding='utf-8') as fh:
    fh.write('session,path\n')
    for i in range(N):
        # 每 10 万行放一条带引号内换行的记录，每 5 万行放一条坏行
        if i%100000==7:
            fh.write('S-%07d,"序章\n>材料>工艺"\n'%i)
        elif i%50000==13:
            fh.write('S-%07d,>空章节\n'%i)
        else:
            fh.write('S-%07d,序章>材料>案例\n'%i)
size=os.path.getsize(path)
print('生成 %d 行，%.1f MB，用时 %.1fs'%(N,size/1e6,time.time()-t0))

t0=time.time();total=valid=bad=dup=0;seen=set();header_skipped=False
for line_no,raw,line_error in iter_logical_lines(path,block_size=1024*1024):
    if not header_skipped:
        header_skipped=True
        if raw.strip().lower()=='session,path':continue
    total+=1
    if line_error:bad+=1;continue
    fields,perr=parse_csv_line(raw.encode('utf-8'))
    if perr or len(fields)!=2:bad+=1;continue
    try:clean=validate_record({'session':fields[0],'path':fields[1]})
    except ValueError:bad+=1;continue
    if clean['session'] in seen:dup+=1
    else:seen.add(clean['session']);valid+=1
elapsed=time.time()-t0
peak_kb=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print('解析完成：总%d 合格%d 坏%d 重复%d，用时%.1fs（%.0f 行/秒）'%(total,valid,bad,dup,elapsed,total/elapsed))
print('进程峰值 RSS：%.1f MB'% (peak_kb/1024))
assert total==N
assert bad==N//50000
assert valid+bad==N
# 独立运行时峰值应低于 128MB；在 pytest 等宿主内基线被抬高，
# 关键口径是“流式处理不随文件行数增长”——峰值与 10 万行场景同量级（<200MB，无整文件驻留）
assert peak_kb/1024<200, '峰值内存异常，疑似整文件载入内存'
print('百万行压力验证通过')
