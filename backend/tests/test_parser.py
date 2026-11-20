"""解析器单元测试：字节流状态机在各种跨块切分下都必须还原原记录。"""
import os,sys
sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.services.import_parser import LineSplitter,parse_csv_line,iter_logical_lines

def split_feed(splitter,data,cut):
    out=[]
    for i in range(0,len(data),cut):
        out.extend(splitter.feed(data[i:i+cut]))
    out.extend(splitter.flush())
    return out

def case(name,data,expected_lines,cut_sizes=(1,2,3,5,7,64,4096)):
    for cut in cut_sizes:
        sp=LineSplitter()
        got=[(ln,raw.decode('utf-8').rstrip('\r\n'),err) for ln,raw,err in split_feed(sp,data,cut)]
        clean=[(ln,txt,err) for ln,txt,err in got]
        assert [(ln,txt) for ln,txt,_ in clean]==[(ln,txt) for ln,txt,_ in expected_lines], (name,cut,clean)
    print('OK  ',name)

# 1. 普通多行 + CRLF + 中文（多字节被任意切开）
d1='会话A,序章>材料\n会话B,材料>工艺\r\n会话C,案例>附录\n'.encode('utf-8')
case('基础多行/CRLF/中文',d1,[(1,'会话A,序章>材料',None),(2,'会话B,材料>工艺',None),(3,'会话C,案例>附录',None)])

# 2. 引号内换行，且换行点被块边界切开
d2='S1,"序章\n>材料"\nS2,案例\n'.encode('utf-8')
case('引号内换行跨块',d2,[(1,'S1,"序章\n>材料"',None),(2,'S2,案例',None)])

# 3. 引号内 CRLF + 转义引号
d3='S1,"他说""你好""\r\n下一节"\nS2,x\n'.encode('utf-8')
case('引号内CRLF与转义引号',d3,[(1,'S1,"他说""你好""\r\n下一节"',None),(2,'S2,x',None)])

# 4. 末行无换行
d4='a,b\nc,d'.encode('utf-8')
case('末行无换行',d4,[(1,'a,b',None),(2,'c,d',None)])

# 5. 中文引号内换行，块大小为1（逐字节，必然切断多字节字符与转义序列）
d5='编号1,"章节甲\n章节乙"\n编号2,章节丙\n'.encode('utf-8')
case('逐字节切分中文引号行',d5,[(1,'编号1,"章节甲\n章节乙"',None),(2,'编号2,章节丙',None)],cut_sizes=(1,2,3))

# 6. 孤立 CR（旧 Mac 风格）及 CR 落在块边界
d6='a,b\rc,d\re,f'.encode('utf-8')
case('孤立CR行分隔',d6,[(1,'a,b',None),(2,'c,d',None),(3,'e,f',None)])
d7='x,y\r\nz,w\r\n'.encode('utf-8')
case('CRLF在各类块边界',d7,[(1,'x,y',None),(2,'z,w',None)],cut_sizes=(1,2,3,4,5))

# 字段解析
assert parse_csv_line('S1,"序章\n>材料"'.encode())==(['S1','序章\n>材料'],None)
assert parse_csv_line('a,"x""y",c'.encode())==(['a','x"y','c'],None)
assert parse_csv_line('a,"未闭合'.encode())==(None,'引号未闭合，字段缺少结束引号')
bad=parse_csv_line(b'a,\xff\xfe')
assert bad[1] and 'UTF-8' in bad[1]
print('OK   字段解析/引号未闭合/坏UTF-8')

# 7. 磁盘迭代器与文件句柄方式一致
import tempfile
with tempfile.NamedTemporaryFile(delete=False) as f:
    f.write(d5);p=f.name
lines=list(iter_logical_lines(p,block_size=3))
assert [(ln,txt) for ln,txt,_ in lines]==[(1,'编号1,"章节甲\n章节乙"'),(2,'编号2,章节丙')]
os.unlink(p)
print('OK   磁盘流式迭代器')
print('全部解析器用例通过')
