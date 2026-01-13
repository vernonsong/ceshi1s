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
            name="database_name",
            description="数据库名称",
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
            required=False
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
            description="执行结果详情",
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
        ),
        NodeOutputParameter(
            name="output_data",
            description="查询结果数据（仅SELECT语句）",
            data_type="list"
        ),
        NodeOutputParameter(
            name="execution_log",
            description="执行日志",
            data_type="string"
        )
    ],
    category="execution"
)


# SQL执行节点
@register_node(sql_execute_metadata)
def sql_execute_node(state: dict) -> Dict[str, Any]:
    print(f"Executing SQL Execute Node for request: {state.get('request_id')}")
    
    # 获取输入参数
    source_data = state.get("source_data", {})
    results = state.get("results", {})
    
    # 尝试从不同来源获取SQL和数据库信息
    # 优先从source_data获取（直接输入）
    sql = source_data.get("sql")
    database_name = source_data.get("database_name")
    database_type = source_data.get("database_type")
    
    # 如果source_data中没有，尝试从sql_generate结果获取
    if not sql:
        sql_generate_result = results.get("sql_generate", {})
        sql = sql_generate_result.get("generated_sql")
    
    if not database_name:
        database_name = sql_generate_result.get("database_name", "default")
    
    if not database_type:
        database_type = sql_generate_result.get("database_type", "hive")
    
    print(f"Database: {database_name} ({database_type})")
    print(f"SQL to execute: {sql[:100]}..." if sql and len(sql) > 100 else f"SQL to execute: {sql}")
    
    # 模拟SQL执行
    # 随机生成执行结果，95%概率成功
    import random
    import time
    is_success = random.choice([True] * 19 + [False])
    
    # 记录执行开始时间
    start_time = time.time()
    
    # 模拟执行延迟
    execution_delay = random.randint(100, 5000) / 1000  # 转换为秒
    time.sleep(execution_delay)
    
    # 记录执行结束时间
    end_time = time.time()
    execution_time = int((end_time - start_time) * 1000)  # 转换为毫秒
    
    # 模拟执行结果
    if is_success:
        status = "success"
        affected_rows = random.randint(1, 10000) if not sql or "select" not in sql.lower() else 0
        message = "SQL execution successful"
        
        # 模拟查询结果（仅SELECT语句）
        output_data = []
        if sql and "select" in sql.lower():
            # 生成模拟查询结果
            num_rows = random.randint(1, 100)
            output_data = [
                {
                    "id": i,
                    "name": f"item_{i}",
                    "value": random.uniform(0, 1000),
                    "created_at": time.strftime("%Y-%m-%d %H:%M:%S")
                }
                for i in range(num_rows)
            ]
    else:
        status = "failed"
        affected_rows = 0
        message = f"SQL execution failed: {random.choice(['Syntax error', 'Permission denied', 'Table not found', 'Connection error'])}"
        output_data = []
    
    # 构建执行日志
    sql_display = f"{sql[:50]}..." if sql and len(sql) > 50 else (sql if sql else "None")
    execution_log = f"""
SQL执行日志:
- 请求ID: {state.get('request_id')}
- 数据库: {database_name} ({database_type})
- SQL语句: {sql_display}
- 执行状态: {status}
- 影响行数: {affected_rows}
- 执行时间: {execution_time}ms
- 消息: {message}
    """
    
    execution_result = {
        "sql": sql,
        "database_name": database_name,
        "database_type": database_type,
        "status": status,
        "affected_rows": affected_rows,
        "execution_time": execution_time,
        "message": message,
        "output_data": output_data
    }
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **results,
            "sql_execute": {
                "status": status,
                "execution_result": execution_result,
                "affected_rows": affected_rows,
                "execution_time": execution_time,
                "output_data": output_data,
                "execution_log": execution_log
            }
        },
        "current_node": "sql_execute"
    }