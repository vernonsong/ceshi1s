from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from datalake.core.workflow.models import register_node, NodeOutputParameter

# 定义输入参数结构
class TableInfo(BaseModel):
    """表信息模型"""
    source_db: str = Field(..., description="源数据库名称")
    source_schema: str = Field(..., description="源数据库schema")
    source_table: str = Field(..., description="源表名称")

# 定义字段信息结构
class FieldInfo(BaseModel):
    """字段信息模型"""
    name: str = Field(..., description="字段名称")
    type: str = Field(..., description="字段类型")
    length: Optional[int] = Field(None, description="字段长度")
    precision: Optional[int] = Field(None, description="字段精度")
    nullable: bool = Field(..., description="是否允许为空")
    primary_key: bool = Field(default=False, description="是否主键")
    comment: str = Field(default="", description="字段注释")

# 定义输出参数结构
class TableFieldResult(BaseModel):
    """表字段查询结果"""
    source_db: str = Field(..., description="源数据库名称")
    source_schema: str = Field(..., description="源数据库schema")
    source_table: str = Field(..., description="源表名称")
    fields: List[FieldInfo] = Field(..., description="字段列表")
    total_fields: int = Field(..., description="字段总数")

@register_node(
    name="table_field_query",
    description="查询上游系统，返回表的字段信息",
    type="task",
    inputs=[
        {"name": "source_db", "description": "源数据库名称", "data_type": "string", "required": True},
        {"name": "source_schema", "description": "源数据库schema", "data_type": "string", "required": True},
        {"name": "source_table", "description": "源表名称", "data_type": "string", "required": True}
    ],
    outputs=[
        NodeOutputParameter(
            name="fields",
            description="字段列表",
            data_type="list"
        ),
        NodeOutputParameter(
            name="total_fields",
            description="字段总数",
            data_type="integer"
        )
    ],
    category="metadata"
)
def table_field_query_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """表字段查询节点实现
    
    这个节点模拟查询上游系统，返回指定表的字段信息。
    它符合LangGraph节点的要求，接收状态对象并返回更新后的状态。
    
    Args:
        state: LangGraph状态字典，包含：
            - request_id: 请求ID
            - results: 之前节点的结果
            - source_data: 源数据（可能包含表信息）
            - workflow_config: 工作流配置
    
    Returns:
        更新后的状态字典，包含表的字段信息
    """
    print(f"Executing Table Field Query Node for request: {state.get('request_id')}")
    
    # 从状态中获取输入参数
    # 优先从results中获取之前节点传递的表信息
    results = state.get("results", {})
    
    # 尝试从不同节点获取表信息
    table_info = None
    for node_name in ["page_submit", "table_check"]:
        if node_name in results:
            node_result = results[node_name]
            if all(key in node_result for key in ["source_db", "source_schema", "source_table"]):
                table_info = node_result
                break
    
    # 如果没有从results中获取到，尝试从source_data中获取
    if not table_info:
        source_data = state.get("source_data", {})
        table_info = source_data
    
    # 提取表信息
    source_db = table_info.get("source_db", "")
    source_schema = table_info.get("source_schema", "")
    source_table = table_info.get("source_table", "")
    
    # 模拟查询上游系统的逻辑
    # 在实际应用中，这里会调用真实的上游系统API或数据库连接来获取字段信息
    print(f"Querying fields for table {source_db}.{source_schema}.{source_table} from upstream system...")
    
    # 模拟字段数据
    # 根据不同的表名返回不同的字段列表，增加模拟的真实感
    mock_fields_data = {
        "source_table_1": [
            {"name": "id", "type": "int", "length": 11, "nullable": False, "primary_key": True, "comment": "主键ID"},
            {"name": "name", "type": "string", "length": 50, "nullable": False, "primary_key": False, "comment": "名称"},
            {"name": "value", "type": "float", "precision": 10, "nullable": True, "primary_key": False, "comment": "数值"},
            {"name": "create_time", "type": "timestamp", "nullable": False, "primary_key": False, "comment": "创建时间"},
            {"name": "update_time", "type": "timestamp", "nullable": True, "primary_key": False, "comment": "更新时间"}
        ],
        "source_table_2": [
            {"name": "user_id", "type": "varchar", "length": 36, "nullable": False, "primary_key": True, "comment": "用户ID"},
            {"name": "user_name", "type": "varchar", "length": 100, "nullable": False, "primary_key": False, "comment": "用户名"},
            {"name": "age", "type": "int", "length": 3, "nullable": True, "primary_key": False, "comment": "年龄"},
            {"name": "email", "type": "varchar", "length": 255, "nullable": True, "primary_key": False, "comment": "邮箱"},
            {"name": "phone", "type": "varchar", "length": 20, "nullable": True, "primary_key": False, "comment": "电话"},
            {"name": "address", "type": "text", "nullable": True, "primary_key": False, "comment": "地址"}
        ],
        "source_table_3": [
            {"name": "order_id", "type": "bigint", "length": 20, "nullable": False, "primary_key": True, "comment": "订单ID"},
            {"name": "user_id", "type": "varchar", "length": 36, "nullable": False, "primary_key": False, "comment": "用户ID"},
            {"name": "product_id", "type": "varchar", "length": 36, "nullable": False, "primary_key": False, "comment": "产品ID"},
            {"name": "quantity", "type": "int", "length": 10, "nullable": False, "primary_key": False, "comment": "数量"},
            {"name": "price", "type": "decimal", "precision": 15, "nullable": False, "primary_key": False, "comment": "价格"},
            {"name": "order_time", "type": "timestamp", "nullable": False, "primary_key": False, "comment": "订单时间"},
            {"name": "status", "type": "int", "length": 2, "nullable": False, "primary_key": False, "comment": "订单状态"}
        ]
    }
    
    # 获取当前表的模拟字段
    fields_data = mock_fields_data.get(source_table, [
        {"name": "id", "type": "int", "length": 11, "nullable": False, "primary_key": True, "comment": "主键ID"},
        {"name": "name", "type": "string", "length": 50, "nullable": False, "primary_key": False, "comment": "名称"},
        {"name": "value", "type": "string", "length": 255, "nullable": True, "primary_key": False, "comment": "值"}
    ])
    
    # 转换为FieldInfo对象列表
    fields = [FieldInfo(**field) for field in fields_data]
    total_fields = len(fields)
    
    # 创建结果对象
    result = TableFieldResult(
        source_db=source_db,
        source_schema=source_schema,
        source_table=source_table,
        fields=fields,
        total_fields=total_fields
    )
    
    # 返回更新后的状态
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **results,
            "table_field_query": {
                "status": "success",
                "source_db": source_db,
                "source_schema": source_schema,
                "source_table": source_table,
                "fields": [field.model_dump() for field in fields],
                "total_fields": total_fields
            }
        },
        "current_node": "table_field_query"
    }