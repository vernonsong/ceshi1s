from typing import Dict, Any
from datalake.core.workflow.models import NodeMetadata, NodeInputParameter, NodeOutputParameter, register_node

# 页面提单节点元数据
page_submit_metadata = NodeMetadata(
    name="page_submit",
    description="页面提单节点，从用户对话中提取入湖信息",
    type="task",
    inputs=[
        NodeInputParameter(
            name="user_input",
            description="用户对话内容",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="username",
            description="用户名",
            data_type="string",
            required=True
        )
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
        ),
        NodeOutputParameter(
            name="ticket_id",
            description="入湖单号",
            data_type="string"
        )
    ],
    category="ingestion"
)


# 页面提单节点
@register_node(page_submit_metadata)
def page_submit_node(state: dict) -> Dict[str, Any]:
    print(f"Executing Page Submit Node for request: {state.get('request_id')}")
    
    # 模拟页面提单逻辑，从对话中提取信息
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
    ticket_id = f"TICKET-{state.get('request_id')[:8].upper()}"
    
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
                "ticket_id": ticket_id,
                "submit_user": username,
                "submit_time": "2024-01-01T10:00:00"
            }
        },
        "current_node": "page_submit"
    }