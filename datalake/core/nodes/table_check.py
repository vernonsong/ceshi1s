from typing import Dict, Any
from datalake.core.workflow.models import NodeMetadata, NodeInputParameter, NodeOutputParameter, register_node

# 表检查节点元数据
table_check_metadata = NodeMetadata(
    name="table_check",
    description="入湖表检查节点，检查源表和目标表的结构和数据",
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
            description="检查状态",
            data_type="string"
        ),
        NodeOutputParameter(
            name="check_result",
            description="检查结果详情",
            data_type="dict"
        ),
        NodeOutputParameter(
            name="error_message",
            description="错误信息",
            data_type="string"
        ),
        NodeOutputParameter(
            name="table_schema",
            description="表结构信息",
            data_type="dict"
        )
    ],
    category="validation"
)


# 入湖表检查节点
@register_node(table_check_metadata)
def table_check_node(state: dict) -> Dict[str, Any]:
    print(f"Executing Table Check Node for request: {state.get('request_id')}")
    
    # 获取检查规则配置
    workflow_config = state.get("workflow_config", {})
    # 检查workflow_config是否为Pydantic模型
    if hasattr(workflow_config, "node_configs"):
        # 如果是Pydantic模型，使用点符号访问
        node_config = workflow_config.node_configs.get("table_check", {})
    else:
        # 如果是字典，使用get()方法
        node_config = workflow_config.get("node_configs", {}).get("table_check", {})
    check_rules = node_config.get("table_check_rules", ["not_null", "data_type", "primary_key", "unique_constraint"])
    
    # 从页面提单结果中获取表信息
    page_submit_result = state.get("results", {}).get("page_submit", {})
    source_db = page_submit_result.get("source_db")
    source_schema = page_submit_result.get("source_schema")
    source_table = page_submit_result.get("source_table")
    lake_db = page_submit_result.get("lake_db")
    lake_schema = page_submit_result.get("lake_schema")
    lake_table = page_submit_result.get("lake_table")
    
    # 模拟表检查逻辑
    # 随机生成检查结果，50%概率通过
    import random
    is_passed = random.choice([True, False])
    
    check_results = {
        source_table: {
            "passed": is_passed,
            "checks": []
        }
    }
    
    for rule in check_rules:
        status = "passed" if is_passed else "failed"
        check_results[source_table]["checks"].append({
            "rule": rule,
            "status": status,
            "message": f"{rule} check {status} for table {source_table}"
        })
    
    # 定义表结构
    table_schema = {
        "source_db": source_db,
        "source_schema": source_schema,
        "source_table": source_table,
        "lake_db": lake_db,
        "lake_schema": lake_schema,
        "lake_table": lake_table,
        "columns": [
            {"name": "id", "type": "int", "primary_key": True},
            {"name": "name", "type": "string", "not_null": True},
            {"name": "value", "type": "float"},
            {"name": "create_time", "type": "timestamp"}
        ]
    }
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "table_check": {
                "status": "success" if is_passed else "failed",
                "check_result": check_results,
                "error_message": f"Table {source_table} failed check" if not is_passed else "",
                "table_schema": table_schema
            }
        },
        "current_node": "table_check"
    }