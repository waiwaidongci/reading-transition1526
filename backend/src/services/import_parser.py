"""百万行日志流式解析器。

关键约束：
- 全程以字节为单位处理，每次只持有一个 1MB 磁盘块 + 一条逻辑行，内存与文件总大小无关（128MB 内）。
- 引号状态机在字节流上工作：UTF-8 的多字节字符（含中文）首字节 >=0x80，
  不会与引号(0x22)、换行(0x0A)、回车(0x0D)冲突，因此多字节字符被任意分块切开也能正确还原。
- 引号内的换行属于同一条逻辑行；跨块的未闭合引号由 carry 缓冲区续接。
- 逻辑行攒齐后才整体 UTF-8 解码，坏字节以隔离行形式返回而非中断整批。
"""
from ..config.settings import MAX_LINE_BYTES

QUOTE=0x22; CR=0x0D; LF=0x0A

class LineSplitter:
    """把任意大小的字节块切成完整逻辑行，保留引号内换行。"""
    def __init__(self,max_line_bytes=MAX_LINE_BYTES):
        self.buf=bytearray()
        self.in_quotes=False
        self.line_no=0
        self.max_line_bytes=max_line_bytes
        self._oversized=False
        self.pending_cr=False  # 上一块结尾的孤立 CR，等待本块首字节判定是否 CRLF

    def _emit(self,piece):
        if self.buf:
            self.buf.extend(piece);line=bytes(self.buf);self.buf.clear()
        else:
            line=piece
        self.line_no+=1
        # 统一在出口判定：无论行尾落在块内还是块边界，超长都在这一处锁存
        oversized=self._oversized or len(line)>self.max_line_bytes
        self._oversized=False
        if len(line)>self.max_line_bytes:line=line[:self.max_line_bytes]
        return self.line_no,line,('单逻辑行超过%d字节上限'%self.max_line_bytes if oversized else None)

    def feed(self,block):
        """喂入一个字节块，产出 (line_no, 原始字节行含换行, 错误原因或None)。"""
        # 先处理上一块遗留的块尾 CR（引号态跨块延续时，CR 属于引号内容，不做行边界）
        if self.pending_cr and not self.in_quotes:
            self.pending_cr=False
            if block and block[0]==LF:
                block=block[1:]  # CRLF 合并，行在上一块已经产出
            # 孤立 CR 不额外产出空行：上一块已按行边界产出，直接继续解析本块
        start=0
        block_len=len(block)
        i=0
        while i<block_len:
            b=block[i]
            if b==QUOTE:
                # 成对引号是转义引号，状态不变；单个引号切换引用态
                if self.in_quotes and i+1<block_len and block[i+1]==QUOTE:
                    i+=2;continue
                self.in_quotes=not self.in_quotes
            elif b in (CR,LF) and not self.in_quotes:
                # CR 恰为块尾时延迟到下一块，判定它是否与后续 LF 组成 CRLF
                if b==CR and i+1>=block_len:
                    result=self._emit(block[start:i+1])  # 行先产出，含 CR
                    self.pending_cr=True
                    yield result
                    start=block_len;break
                # 逻辑行结束；CRLF 合并为单个行尾
                if b==CR and block[i+1]==LF:
                    result=self._emit(block[start:i+1]);start=i+2;i+=2
                    yield result;continue
                yield self._emit(block[start:i+1]);start=i+1
            i+=1
        # 块内剩余部分（含未闭合的引号行）留到下一块续接
        tail=block[start:]
        if tail:
            if self._oversized:
                # 该逻辑行已超上限：继续丢弃超出字节，仅在仍有容量时保留前缀
                room=self.max_line_bytes-len(self.buf)
                if room>0:self.buf.extend(tail[:room])
                if len(self.buf)>self.max_line_bytes:del self.buf[self.max_line_bytes:]
                return
            self.buf.extend(tail)
            if len(self.buf)>self.max_line_bytes:
                # 超限行：只保留上限内的前缀用于隔离留痕，超出部分丢弃，行边界仍正常等待。
                # _oversized 锁存到该逻辑行 _emit 才清除，避免后续块把标志冲掉。
                del self.buf[self.max_line_bytes:]
                self._oversized=True

    def flush(self):
        """文件结束后的残余（最后一行没有换行符）。"""
        self.pending_cr=False
        if not self.buf:
            return
        self.line_no+=1
        line=bytes(self.buf);self.buf.clear()
        oversized=self._oversized;self._oversized=False
        yield self.line_no,line,('单逻辑行超过%d字节上限'%self.max_line_bytes if oversized else None)


def parse_csv_line(line_bytes):
    """RFC4180 字段解析（单条逻辑行的字节，已去掉行尾换行）。

    返回 (字段字符串列表, 错误原因或None)。解析在字节层完成，解码只发生一次。
    """
    # 去掉行尾 CR/LF（LF splitter 已消费，这里防御残余 CR）
    if line_bytes and line_bytes[-1] in (CR,LF):
        line_bytes=line_bytes[:-1]
    if line_bytes and line_bytes[-1]==CR:
        line_bytes=line_bytes[:-1]
    fields=[]
    cur=bytearray()
    in_quotes=False
    i=0;n=len(line_bytes)
    while i<n:
        b=line_bytes[i]
        if in_quotes:
            if b==QUOTE:
                if i+1<n and line_bytes[i+1]==QUOTE:
                    cur.append(QUOTE);i+=2;continue
                in_quotes=False;i+=1;continue
            cur.append(b);i+=1;continue
        if b==QUOTE:
            in_quotes=True;i+=1;continue
        if b==0x2C:  # 逗号
            fields.append(bytes(cur));cur=bytearray();i+=1;continue
        cur.append(b);i+=1
    if in_quotes:
        return None,'引号未闭合，字段缺少结束引号'
    fields.append(bytes(cur))
    try:
        return [f.decode('utf-8') for f in fields],None
    except UnicodeDecodeError as e:
        return None,'UTF-8解码失败：'+str(e)[:120]


def looks_like_header(fields,expected_keys):
    """首行若恰好是表头（两列名与业务字段一致）则跳过。"""
    normalized=[f.strip().lower() for f in fields]
    return normalized==[k.lower() for k in expected_keys]


def iter_logical_lines(file_path,block_size=None,max_line_bytes=None):
    """逐块读取磁盘文件，产出完整逻辑行 (line_no, raw_text, line_error)。

    raw_text 保留原始行内容（解码失败时以 replacement 字符保留，供隔离区展示）。
    参数缺省时在调用点读取配置，保证运行期环境变量覆盖可生效。
    """
    from ..config import settings
    if block_size is None:block_size=settings.PARSE_BLOCK_BYTES
    if max_line_bytes is None:max_line_bytes=settings.MAX_LINE_BYTES
    splitter=LineSplitter(max_line_bytes)
    with open(file_path,'rb') as fh:
        while True:
            block=fh.read(block_size)
            if not block:break
            for line_no,raw,line_error in splitter.feed(block):
                if line_error:
                    yield line_no,raw.decode('utf-8','replace').rstrip('\r\n'),line_error
                    continue
                try:
                    yield line_no,raw.decode('utf-8').rstrip('\r\n'),None
                except UnicodeDecodeError as e:
                    yield line_no,raw.decode('utf-8','replace').rstrip('\r\n'),'UTF-8解码失败：'+str(e)[:120]
        for line_no,raw,line_error in splitter.flush():
            if line_error:
                yield line_no,raw.decode('utf-8','replace').rstrip('\r\n'),line_error
            else:
                try:
                    yield line_no,raw.decode('utf-8').rstrip('\r\n'),None
                except UnicodeDecodeError as e:
                    yield line_no,raw.decode('utf-8','replace').rstrip('\r\n'),'UTF-8解码失败：'+str(e)[:120]
