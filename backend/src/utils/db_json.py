"""跨方言 JSON 文本提取：MySQL 用 JSON_UNQUOTE/JSON_EXTRACT，SQLite 用 json_extract。

注意 values 是双方言保留字，列名必须按方言转义（MySQL 反引号、SQLite 双引号）。
"""
from sqlalchemy import text
from ..config.database import engine
def _quote(column):
    if engine.dialect.name=='mysql':
        return '`%s`'%column.replace('`','``')
    return '"%s"'%column.replace('"','""')
def json_text(column,key):
    """返回可用于 where/select 的 SQL 表达式，取出 JSON 列中的字符串值。"""
    col=_quote(column)
    if engine.dialect.name=='mysql':
        return text("JSON_UNQUOTE(JSON_EXTRACT(%s,'$.%s'))"%(col,key))
    return text("json_extract(%s,'$.%s')"%(col,key))
