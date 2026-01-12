from typing import Dict, Any
from pydantic import BaseModel, Field
from datalake.core.workflow.models import register_node, NodeOutputParameter

# 定义输入参数结构
class InputData(BaseModel):
    """节点输入数据模型"""
    user_input: str = Field(..., description="用户对话内容")
    username: str = Field(..., description="用户名")

# 定义输出参数结构
class PageSubmitResult(BaseModel):
    """页面提单结果"""
    source_db: str = Field(..., description="源数据库名称")
    source_schema: str = Field(..., description="源数据库schema")
    source_table: str = Field(..., description="源表名称")
    lake_db: str = Field(..., description="湖仓数据库名称")
    lake_schema: str = Field(..., description="湖仓数据库schema")
    lake_table: str = Field(..., description="湖仓表名称")
    submit_user: str = Field(..., description="提交用户")
    submit_time: str = Field(..., description="提交时间")

@register_node(
    name="page_submit",
    description="页面提单节点，从用户对话中提取入湖信息",
    type="task",
    inputs=[
        {"name": "user_input", "description": "用户对话内容", "data_type": "string", "required": True},
        {"name": "username", "description": "用户名", "data_type": "string", "required": True}
    ],
    outputs=[
        NodeOutputParameter(
            name="source_db",
            description="源数据库名称",
            data_type="string"
        ),
        NodeOutputParameter(
            name="source_schema",
            description="源数据库schema",
            data_type="string"
        ),
        NodeOutputParameter(
            name="source_table",
            description="源表名称",
            data_type="string"
        ),
        NodeOutputParameter(
            name="lake_db",
            description="湖仓数据库名称",
            data_type="string"
        ),
        NodeOutputParameter(
            name="lake_schema",
            description="湖仓数据库schema",
            data_type="string"
        ),
        NodeOutputParameter(
            name="lake_table",
            description="湖仓表名称",
            data_type="string"
        )
    ],
    category="ingestion"
)
def page_submit_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """页面提单节点实现
    
    这个节点从用户对话中提取入湖信息，包括源数据库、源表和目标湖仓信息。
    它符合LangGraph节点的要求，接收状态对象并返回更新后的状态。
    
    Args:
        state: LangGraph状态字典，包含：
            - request_id: 请求ID
            - results: 之前节点的结果
            - source_data: 包含user_input和username的源数据
            - workflow_config: 工作流配置
    
    Returns:
        更新后的状态字典，包含提取的入湖信息
    """
    print(f"Executing Page Submit Node for request: {state.get('request_id')}")
    
    # 从状态中获取输入参数
    source_data = state.get("source_data", {})
    user_input = source_data.get("user_input", "")
    username = source_data.get("username", "default_user")
    
    # 模拟从对话中解析出源库、源表等信息
    # 在实际应用中，这里会调用大模型或其他服务来解析对话内容
    source_db = "source_db_1"
    source_schema = "source_schema_1"
    source_table = "source_table_1"
    lake_db = "lake_db_1"
    lake_schema = "lake_schema_1"
    lake_table = "lake_table_1"
    
    # 创建结果对象
    submit_result = PageSubmitResult(
        source_db=source_db,
        source_schema=source_schema,
        source_table=source_table,
        lake_db=lake_db,
        lake_schema=lake_schema,
        lake_table=lake_table,
        submit_user=username,
        submit_time="2024-01-01T10:00:00"
    )
    
    # 返回更新后的状态
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "page_submit": {
                "status": "success",
                "source_db": source_db,
                "source_schema": source_schema,
                "source_table": source_table,
                "lake_db": lake_db,
                "lake_schema": lake_schema,
                "lake_table": lake_table,
                "submit_user": username,
                "submit_time": "2024-01-01T10:00:00"
            }
        },
        "current_node": "page_submit"
    }