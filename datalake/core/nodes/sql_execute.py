from typing import Dict, Any
from datalake.core.workflow.models import NodeMetadata, NodeInputParameter, NodeOutputParameter, register_node

# SQL执行节点元数据
sql_execute_metadata = NodeMetadata(
    name="sql_execute",
    description="SQL执行节点，执行生成的SQL语句",
    type="task",
    inputs=[
        NodeInputParameter(
            name="sql",
            description="SQL语句",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="database_type",
            description="数据库类型",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="connection_info",
            description="数据库连接信息",
            data_type="dict",
            required=True
        )
    ],
    outputs=[
        NodeOutputParameter(
            name="status",
            description="执行状态",
            data_type="string"
        ),
        NodeOutputParameter(
            name="execution_result",
            description="执行结果",
            data_type="dict"
        ),
        NodeOutputParameter(
            name="affected_rows",
            description="影响的行数",
            data_type="integer"
        ),
        NodeOutputParameter(
            name="execution_time",
            description="执行时间(ms)",
            data_type="integer"
        )
    ],
    category="execution"
)


# SQL执行节点
@register_node(sql_execute_metadata)
def sql_execute_node(state: dict) -> Dict[str, Any]:
    print(f"Executing SQL Execute Node for request: {state.get('request_id')}")
    
    # 从SQL生成结果中获取SQL语句
    sql_generate_result = state.get("results", {}).get("sql_generate", {})
    generated_sql = sql_generate_result.get("generated_sql")
    
    # 模拟SQL执行
    # 随机生成执行结果，90%概率成功
    import random
    is_success = random.choice([True] * 9 + [False])
    
    if is_success:
        status = "success"
        affected_rows = random.randint(100, 10000)
        execution_time = random.randint(500, 5000)
    else:
        status = "failed"
        affected_rows = 0
        execution_time = random.randint(100, 1000)
    
    execution_result = {
        "sql": generated_sql,
        "status": status,
        "affected_rows": affected_rows,
        "execution_time": execution_time,
        "message": "SQL execution successful" if is_success else "SQL execution failed"
    }
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "sql_execute": {
                "status": status,
                "execution_result": execution_result,
                "affected_rows": affected_rows,
                "execution_time": execution_time
            }
        },
        "current_node": "sql_execute"
    }