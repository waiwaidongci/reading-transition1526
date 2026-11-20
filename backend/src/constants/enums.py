from enum import Enum
class Role(str,Enum):
    admin='admin'; analyst='analyst'; viewer='viewer'
class RunState(str,Enum):
    queued='queued'; running='running'; completed='completed'; failed='failed'
class ReviewState(str,Enum):
    draft='draft'; approved='approved'
class ImportState(str,Enum):
    receiving='receiving'      # 接收中，可继续追加分片
    received='received'        # 接收完成，等待/可重试校验
    validating='validating'    # 流式校验中
    validated='validated'      # 校验通过，等待发布
    publishing='publishing'    # 发布事务执行中
    published='published'      # 已整批发布，研究可见
    failed='failed'            # 整批失败（冲突/无合格样本/落盘异常），隔离区可查
