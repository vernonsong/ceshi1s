from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
from datalake.core.workflow.models import register_node, NodeOutputParameter

# 定义输入参数结构
class DBInfo(BaseModel):
    """数据库信息模型"""
    source_db: str = Field(..., description="源数据库名称")

# 定义输出参数结构
class DBTypeResult(BaseModel):
    """数据库类型查询结果"""
    source_db: str = Field(..., description="源数据库名称")
    db_type: str = Field(..., description="数据库类型")

@register_node(
    name="db_type_query",
    description="查询上游系统，返回数据库类型",
    type="task",
    inputs=[
        {"name": "source_db", "description": "源数据库名称", "data_type": "string", "required": True}
    ],
    outputs=[
        NodeOutputParameter(
            name="db_type",
            description="数据库类型",
            data_type="string"
        )
    ],
    category="metadata"
)
def db_type_query_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """数据库类型查询节点实现
    
    这个节点模拟查询上游系统，返回指定数据库的类型。
    它符合LangGraph节点的要求，接收状态对象并返回更新后的状态。
    
    Args:
        state: LangGraph状态字典，包含：
            - request_id: 请求ID
            - results: 之前节点的结果
            - source_data: 源数据（可能包含数据库信息）
            - workflow_config: 工作流配置
    
    Returns:
        更新后的状态字典，包含数据库类型信息
    """
    print(f"Executing DB Type Query Node for request: {state.get('request_id')}")
    
    # 从状态中获取输入参数
    # 优先从results中获取之前节点传递的数据库信息
    results = state.get("results", {})
    
    # 尝试从不同节点获取数据库信息
    db_info = None
    for node_name in ["page_submit", "table_check", "table_field_query"]:
        if node_name in results:
            node_result = results[node_name]
            if "source_db" in node_result:
                db_info = node_result
                break
    
    # 如果没有从results中获取到，尝试从source_data中获取
    if not db_info:
        source_data = state.get("source_data", {})
        db_info = source_data
    
    # 提取数据库信息
    source_db = db_info.get("source_db", "")
    
    # 模拟查询上游系统的逻辑
    # 在实际应用中，这里会调用真实的上游系统API或数据库连接来获取数据库类型
    print(f"Querying type for database {source_db} from upstream system...")
    
    # 模拟数据库类型数据
    # 根据不同的数据库名称返回不同的数据库类型，增加模拟的真实感
    mock_db_types = {
        "mysql_db": "MySQL",
        "postgresql_db": "PostgreSQL",
        "oracle_db": "Oracle",
        "sqlserver_db": "SQL Server",
        "hive_db": "Hive",
        "clickhouse_db": "ClickHouse",
        "mongodb_db": "MongoDB",
        "redis_db": "Redis",
        "source_db_1": "MySQL",
        "source_db_2": "PostgreSQL",
        "source_db_3": "Oracle"
    }
    
    # 获取当前数据库的模拟类型
    db_type = mock_db_types.get(source_db, "Unknown")
    
    # 创建结果对象
    result = DBTypeResult(
        source_db=source_db,
        db_type=db_type
    )
    
    # 返回更新后的状态
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **results,
            "db_type_query": {
                "status": "success",
                "source_db": source_db,
                "db_type": db_type
            }
        },
        "current_node": "db_type_query"
    }