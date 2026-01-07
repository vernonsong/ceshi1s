from typing import Dict, Any
from datalake.core.workflow.models import NodeMetadata, NodeInputParameter, NodeOutputParameter, register_node

# 集成任务生成节点元数据
integration_task_generate_metadata = NodeMetadata(
    name="integration_task_generate",
    description="集成任务生成节点，生成数据集成任务",
    type="task",
    inputs=[
        NodeInputParameter(
            name="table_schema",
            description="表结构信息",
            data_type="dict",
            required=True
        ),
        NodeInputParameter(
            name="source_db",
            description="源数据库名称",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="source_schema",
            description="源数据库schema",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="source_table",
            description="源表名称",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="lake_db",
            description="湖仓数据库名称",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="lake_schema",
            description="湖仓数据库schema",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="lake_table",
            description="湖仓表名称",
            data_type="string",
            required=True
        )
    ],
    outputs=[
        NodeOutputParameter(
            name="status",
            description="生成状态",
            data_type="string"
        ),
        NodeOutputParameter(
            name="task_name",
            description="任务名称",
            data_type="string"
        ),
        NodeOutputParameter(
            name="task_description",
            description="任务描述",
            data_type="string"
        ),
        NodeOutputParameter(
            name="task_id",
            description="任务ID",
            data_type="string"
        ),
        NodeOutputParameter(
            name="task_parameters",
            description="任务参数",
            data_type="dict"
        )
    ],
    category="integration"
)


# 集成任务生成节点
@register_node(integration_task_generate_metadata)
def integration_task_generate_node(state: dict) -> Dict[str, Any]:
    print(f"Executing Integration Task Generate Node for request: {state.get('request_id')}")
    
    # 从表检查结果中获取表信息
    table_check_result = state.get("results", {}).get("table_check", {})
    table_schema = table_check_result.get("table_schema", {})
    
    source_db = table_schema.get("source_db")
    source_schema = table_schema.get("source_schema")
    source_table = table_schema.get("source_table")
    lake_db = table_schema.get("lake_db")
    lake_schema = table_schema.get("lake_schema")
    lake_table = table_schema.get("lake_table")
    
    # 生成集成任务
    task_name = f"{source_db}_{source_schema}_{source_table}_to_{lake_db}_{lake_schema}_{lake_table}"
    task_description = f"从{source_db}.{source_schema}.{source_table}到{lake_db}.{lake_schema}.{lake_table}的数据集成任务"
    task_id = f"TASK-{state.get('request_id')[:8].upper()}"
    
    # 模拟任务生成参数
    import random
    task_parameters = {
        "source": {
            "db": source_db,
            "schema": source_schema,
            "table": source_table,
            "type": "mysql"
        },
        "target": {
            "db": lake_db,
            "schema": lake_schema,
            "table": lake_table,
            "type": "hive"
        },
        "schedule": "daily",
        "start_time": "2024-01-01 00:00:00",
        "retry_count": 3,
        "timeout": 3600,
        "id": task_id,
        "name": task_name
    }
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "integration_task_generate": {
                "status": "success",
                "task_name": task_name,
                "task_description": task_description,
                "task_id": task_id,
                "task_parameters": task_parameters
            }
        },
        "current_node": "integration_task_generate"
    }