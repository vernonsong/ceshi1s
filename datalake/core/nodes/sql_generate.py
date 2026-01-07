from typing import Dict, Any
from datalake.core.workflow.models import NodeMetadata, NodeInputParameter, NodeOutputParameter, register_node

# SQL生成节点元数据
sql_generate_metadata = NodeMetadata(
    name="sql_generate",
    description="SQL生成节点，根据表结构生成入湖SQL语句",
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
            name="generated_sql",
            description="生成的SQL语句",
            data_type="string"
        ),
        NodeOutputParameter(
            name="sql_type",
            description="SQL类型",
            data_type="string"
        ),
        NodeOutputParameter(
            name="execution_plan",
            description="执行计划",
            data_type="string"
        )
    ],
    category="transformation"
)


# SQL生成节点
@register_node(sql_generate_metadata)
def sql_generate_node(state: dict) -> Dict[str, Any]:
    print(f"Executing SQL Generate Node for request: {state.get('request_id')}")
    
    # 从表检查结果中获取表结构
    table_check_result = state.get("results", {}).get("table_check", {})
    table_schema = table_check_result.get("table_schema", {})
    
    # 获取表信息
    source_db = table_schema.get("source_db", "source_db")
    source_schema = table_schema.get("source_schema", "source_schema")
    source_table = table_schema.get("source_table", "source_table")
    lake_db = table_schema.get("lake_db", "lake_db")
    lake_schema = table_schema.get("lake_schema", "lake_schema")
    lake_table = table_schema.get("lake_table", "lake_table")
    columns = table_schema.get("columns", [])
    
    # 生成SQL
    column_names = [col["name"] for col in columns]
    columns_str = ", ".join(column_names)
    
    generated_sql = f"""
INSERT INTO {lake_db}.{lake_schema}.{lake_table} ({columns_str})
SELECT {columns_str}
FROM {source_db}.{source_schema}.{source_table}
WHERE update_time > DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
"""
    
    # 生成执行计划
    execution_plan = f"""
执行计划:
1. 从源表 {source_db}.{source_schema}.{source_table} 读取数据
2. 过滤 update_time 在过去一天内的数据
3. 将数据插入到湖仓表 {lake_db}.{lake_schema}.{lake_table}
"""
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "sql_generate": {
                "status": "success",
                "generated_sql": generated_sql,
                "sql_type": "insert",
                "execution_plan": execution_plan
            }
        },
        "current_node": "sql_generate"
    }