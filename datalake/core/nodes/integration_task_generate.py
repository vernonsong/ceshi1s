from typing import Dict, Any, List
from datalake.core.workflow.models import NodeMetadata, NodeInputParameter, NodeOutputParameter, register_node

# 集成任务生成节点元数据
integration_task_generate_metadata = NodeMetadata(
    name="integration_task_generate",
    description="集成任务生成节点，生成数据集成任务",
    type="task",
    inputs=[
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
            name="target_db",
            description="目标数据库名称",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="target_schema",
            description="目标数据库schema",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="target_table",
            description="目标表名称",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="field_mapping",
            description="字段名映射列表（源和目标字段名）",
            data_type="list",
            required=True
        ),
        NodeInputParameter(
            name="username",
            description="用户名",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="integration_type",
            description="集成类型",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="parallelism",
            description="并行度",
            data_type="integer",
            required=True
        ),
        NodeInputParameter(
            name="audit_template_name",
            description="审计模板名",
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
            name="task_directory",
            description="任务目录",
            data_type="string"
        ),
        NodeOutputParameter(
            name="upstream_api_json",
            description="调用上游接口的JSON",
            data_type="dict"
        )
    ],
    category="integration"
)


# 集成任务生成节点
@register_node(integration_task_generate_metadata)
def integration_task_generate_node(state: dict) -> Dict[str, Any]:
    print(f"Executing Integration Task Generate Node for request: {state.get('request_id')}")
    
    # 获取输入参数
    source_data = state.get("source_data", {})
    results = state.get("results", {})
    
    # 从source_data或results中获取参数
    source_db = source_data.get("source_db") or results.get("table_check", {}).get("source_db", "")
    source_schema = source_data.get("source_schema") or results.get("table_check", {}).get("source_schema", "")
    source_table = source_data.get("source_table") or results.get("table_check", {}).get("source_table", "")
    target_db = source_data.get("target_db") or results.get("table_check", {}).get("target_db", "")
    target_schema = source_data.get("target_schema") or results.get("table_check", {}).get("target_schema", "")
    target_table = source_data.get("target_table") or results.get("table_check", {}).get("target_table", "")
    field_mapping = source_data.get("field_mapping", [])
    username = source_data.get("username", "default_user")
    integration_type = source_data.get("integration_type", "full")
    parallelism = source_data.get("parallelism", 1)
    audit_template_name = source_data.get("audit_template_name", "default_audit")
    
    print(f"Source: {source_db}.{source_schema}.{source_table}")
    print(f"Target: {target_db}.{target_schema}.{target_table}")
    print(f"Integration Type: {integration_type}, Parallelism: {parallelism}")
    print(f"Field Mapping: {field_mapping}")
    
    # 生成集成任务
    task_name = f"{source_db}_{source_schema}_{source_table}_to_{target_db}_{target_schema}_{target_table}_{integration_type}"
    task_description = f"从{source_db}.{source_schema}.{source_table}到{target_db}.{target_schema}.{target_table}的{integration_type}集成任务"
    task_directory = f"/integration_tasks/{source_db}/{source_schema}/{source_table}/{target_db}/{target_schema}/{target_table}"
    
    # 生成调用上游接口的JSON
    import random
    import time
    upstream_api_json = {
        "task_info": {
            "name": task_name,
            "description": task_description,
            "type": integration_type,
            "create_by": username,
            "create_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "update_time": time.strftime("%Y-%m-%d %H:%M:%S")
        },
        "source": {
            "db": source_db,
            "schema": source_schema,
            "table": source_table,
            "type": "mysql"
        },
        "target": {
            "db": target_db,
            "schema": target_schema,
            "table": target_table,
            "type": "hive"
        },
        "field_mapping": field_mapping,
        "advanced_settings": {
            "parallelism": parallelism,
            "retry_count": 3,
            "timeout": 3600,
            "audit_template": audit_template_name
        },
        "schedule": {
            "type": "cron",
            "expression": "0 0 * * *",
            "start_time": "2024-01-01 00:00:00"
        },
        "tags": [
            f"source:{source_db}",
            f"target:{target_db}",
            f"type:{integration_type}"
        ]
    }
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **results,
            "integration_task_generate": {
                "status": "success",
                "task_name": task_name,
                "task_description": task_description,
                "task_directory": task_directory,
                "upstream_api_json": upstream_api_json
            }
        },
        "current_node": "integration_task_generate"
    }