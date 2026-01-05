from typing import Dict, Any, List
from models import NodeMetadata, NodeInputParameter, NodeOutputParameter


# 节点元数据定义
NODE_METADATA = {
    "page_submit": NodeMetadata(
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
    ),
    "table_check": NodeMetadata(
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
    ),
    "llm": NodeMetadata(
        name="llm",
        description="大模型分析节点，分析表检查结果并生成修复建议",
        type="task",
        inputs=[
            NodeInputParameter(
                name="status",
                description="表检查结果状态",
                data_type="string",
                required=True
            ),
            NodeInputParameter(
                name="check_result",
                description="表检查结果",
                data_type="dict",
                required=True
            ),
            NodeInputParameter(
                name="error_message",
                description="错误信息",
                data_type="string"
            ),
            NodeInputParameter(
                name="table_schema",
                description="表结构信息",
                data_type="dict"
            )
        ],
        outputs=[
            NodeOutputParameter(
                name="analysis_result",
                description="分析结果",
                data_type="string"
            ),
            NodeOutputParameter(
                name="suggestions",
                description="修复建议",
                data_type="list"
            ),
            NodeOutputParameter(
                name="is_blocked",
                description="是否阻断流程",
                data_type="boolean"
            )
        ],
        category="validation"
    ),
    "sql_generate": NodeMetadata(
        name="sql_generate",
        description="SQL生成节点，生成创建表和插入数据的SQL",
        type="task",
        inputs=[
            NodeInputParameter(
                name="table_schema",
                description="表结构信息",
                data_type="dict",
                required=True
            )
        ],
        outputs=[
            NodeOutputParameter(
                name="sql_query",
                description="生成的SQL语句",
                data_type="string"
            ),
            NodeOutputParameter(
                name="execution_plan",
                description="执行计划",
                data_type="dict"
            ),
            NodeOutputParameter(
                name="generated_sqls",
                description="生成的SQL集合",
                data_type="dict"
            )
        ],
        category="transformation"
    ),
    "sql_execute": NodeMetadata(
        name="sql_execute",
        description="SQL执行节点，执行生成的SQL语句",
        type="task",
        inputs=[
            NodeInputParameter(
                name="sql_query",
                description="要执行的SQL语句",
                data_type="string",
                required=True
            ),
            NodeInputParameter(
                name="execution_plan",
                description="执行计划",
                data_type="dict"
            )
        ],
        outputs=[
            NodeOutputParameter(
                name="execution_result",
                description="执行结果",
                data_type="dict"
            ),
            NodeOutputParameter(
                name="execution_status",
                description="执行状态",
                data_type="string"
            )
        ],
        category="transformation"
    ),
    "integration_task_generate": NodeMetadata(
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
                name="task_path",
                description="集成任务路径，由库、schema、表名拼接而成",
                data_type="string",
                required=True
            )
        ],
        outputs=[
            NodeOutputParameter(
                name="task_config",
                description="任务配置",
                data_type="dict"
            ),
            NodeOutputParameter(
                name="task_id",
                description="任务ID",
                data_type="string"
            )
        ],
        category="deployment"
    ),
    "integration_task_deploy": NodeMetadata(
        name="integration_task_deploy",
        description="集成任务部署节点，部署数据集成任务",
        type="task",
        inputs=[
            NodeInputParameter(
                name="task_config",
                description="任务配置",
                data_type="dict",
                required=True
            ),
            NodeInputParameter(
                name="task_id",
                description="任务ID",
                data_type="string",
                required=True
            )
        ],
        outputs=[
            NodeOutputParameter(
                name="deploy_status",
                description="部署状态",
                data_type="string"
            ),
            NodeOutputParameter(
                name="deploy_time",
                description="部署时间",
                data_type="string"
            ),
            NodeOutputParameter(
                name="deployment_result",
                description="部署结果",
                data_type="dict"
            )
        ],
        category="deployment"
    ),
    "artifact_generate": NodeMetadata(
        name="artifact_generate",
        description="制品生成节点，生成工作流制品",
        type="task",
        inputs=[
            NodeInputParameter(
                name="execution_results",
                description="执行结果",
                data_type="dict",
                required=True
            ),
            NodeInputParameter(
                name="workflow_config",
                description="工作流配置",
                data_type="dict",
                required=True
            )
        ],
        outputs=[
            NodeOutputParameter(
                name="artifact_path",
                description="制品路径",
                data_type="string"
            ),
            NodeOutputParameter(
                name="artifact_type",
                description="制品类型",
                data_type="string"
            ),
            NodeOutputParameter(
                name="artifact_metadata",
                description="制品元数据",
                data_type="dict"
            )
        ],
        category="deployment"
    )
}


# 1. 页面提单节点
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


# 2. 入湖表检查节点
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


# 3. SQL生成节点
def sql_generate_node(state: dict) -> Dict[str, Any]:
    print(f"Executing SQL Generate Node for request: {state.get('request_id')}")
    
    # 获取SQL模板配置
    workflow_config = state.get("workflow_config", {})
    # 检查workflow_config是否为Pydantic模型
    if hasattr(workflow_config, "node_configs"):
        # 如果是Pydantic模型，使用点符号访问
        node_config = workflow_config.node_configs.get("sql_generate", {})
    else:
        # 如果是字典，使用get()方法
        node_config = workflow_config.get("node_configs", {}).get("sql_generate", {})
    sql_template = node_config.get("sql_template", "CREATE TABLE {lake_table} ({columns});")
    
    # 从表检查结果中获取表结构
    table_check_result = state.get("results", {}).get("table_check", {})
    table_schema = table_check_result.get("table_schema", {})
    
    # 模拟SQL生成逻辑
    lake_table = table_schema.get("lake_table", "")
    columns = table_schema.get("columns", [])
    
    # 生成创建表SQL
    columns_sql = ", ".join([f"{col['name']} {col['type']}{' PRIMARY KEY' if col.get('primary_key') else ''}{' NOT NULL' if col.get('not_null') else ''}" for col in columns])
    create_sql = sql_template.format(lake_table=lake_table, columns=columns_sql)
    
    # 生成插入数据SQL（示例）
    insert_sql = f"INSERT INTO {lake_table} (id, name, value, create_time) VALUES (?, ?, ?, ?);"
    
    generated_sqls = {
        "create_table": create_sql,
        "insert_data": insert_sql
    }
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "sql_generate": {
                "status": "success",
                "sql_query": create_sql,
                "execution_plan": {
                    "steps": [
                        {"type": "create_table", "sql": create_sql},
                        {"type": "insert_data", "sql": insert_sql}
                    ],
                    "parallel": False
                },
                "generated_sqls": generated_sqls
            }
        },
        "current_node": "sql_generate"
    }


# 4. SQL执行节点
def sql_execute_node(state: dict) -> Dict[str, Any]:
    print(f"Executing SQL Execute Node for request: {state.get('request_id')}")
    
    # 从SQL生成结果中获取SQL和执行计划
    sql_generate_result = state.get("results", {}).get("sql_generate", {})
    sql_query = sql_generate_result.get("sql_query", "")
    execution_plan = sql_generate_result.get("execution_plan", {})
    
    # 模拟SQL执行逻辑
    execution_result = {
        "sql": sql_query,
        "status": "success",
        "affected_rows": 0,
        "execution_time": 1.2,
        "error_message": ""
    }
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "sql_execute": {
                "status": "success",
                "execution_result": execution_result,
                "execution_status": "completed"
            }
        },
        "current_node": "sql_execute"
    }


# 5. 集成任务生成节点
def integration_task_generate_node(state: dict) -> Dict[str, Any]:
    print(f"Executing Integration Task Generate Node for request: {state.get('request_id')}")
    
    # 获取并行度配置
    workflow_config = state.get("workflow_config", {})
    # 检查workflow_config是否为Pydantic模型
    if hasattr(workflow_config, "node_configs"):
        # 如果是Pydantic模型，使用点符号访问
        node_config = workflow_config.node_configs.get("integration_task_generate", {})
    else:
        # 如果是字典，使用get()方法
        node_config = workflow_config.get("node_configs", {}).get("integration_task_generate", {})
    parallelism = node_config.get("integration_parallelism", 1)
    
    # 从表检查结果中获取表结构
    table_check_result = state.get("results", {}).get("table_check", {})
    table_schema = table_check_result.get("table_schema", {})
    lake_table = table_schema.get("lake_table", "")
    
    # 获取task_path参数（这个参数是通过Python代码生成的）
    task_path = state.get("task_path")
    if not task_path:
        # 如果没有传入task_path，使用默认值
        source_db = table_schema.get("source_db", "")
        source_schema = table_schema.get("source_schema", "")
        source_table = table_schema.get("source_table", "")
        task_path = f"{source_db}/{source_schema}/{source_table}"
    
    # 模拟集成任务生成逻辑
    task_id = f"INT-{state.get('request_id')[:8].upper()}-001"
    
    task_config = {
        "task_id": task_id,
        "table_name": lake_table,
        "task_type": "integration",
        "parallelism": parallelism,
        "source_db": table_schema.get("source_db"),
        "source_schema": table_schema.get("source_schema"),
        "source_table": table_schema.get("source_table"),
        "lake_db": table_schema.get("lake_db"),
        "lake_schema": table_schema.get("lake_schema"),
        "lake_table": lake_table,
        "task_path": task_path,
        "schedule": "0 0 * * *",
        "retries": 3,
        "timeout": 3600
    }
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "integration_task_generate": {
                "status": "success",
                "task_config": task_config,
                "task_id": task_id
            }
        },
        "current_node": "integration_task_generate"
    }


# 6. 集成任务部署节点
def integration_task_deploy_node(state: dict) -> Dict[str, Any]:
    print(f"Executing Integration Task Deploy Node for request: {state.get('request_id')}")
    
    # 从集成任务生成结果中获取任务配置
    integration_task_result = state.get("results", {}).get("integration_task_generate", {})
    task_config = integration_task_result.get("task_config", {})
    task_id = integration_task_result.get("task_id", "")
    
    # 模拟集成任务部署逻辑
    deployment_result = {
        "task_id": task_id,
        "task_config": task_config,
        "deployment_status": "success",
        "deployed_time": "2024-01-01T10:15:00",
        "deployment_time": 5.6,
        "error_message": ""
    }
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "integration_task_deploy": {
                "status": "success",
                "deploy_status": "completed",
                "deploy_time": "2024-01-01T10:15:00",
                "deployment_result": deployment_result
            }
        },
        "current_node": "integration_task_deploy"
    }


# 7. 大模型节点
def llm_node(state: dict) -> Dict[str, Any]:
    print(f"Executing LLM Node for request: {state.get('request_id')}")
    
    # 获取大模型配置
    workflow_config = state.get("workflow_config", {})
    # 检查workflow_config是否为Pydantic模型
    if hasattr(workflow_config, "node_configs"):
        # 如果是Pydantic模型，使用点符号访问
        node_config = workflow_config.node_configs.get("llm", {})
    else:
        # 如果是字典，使用get()方法
        node_config = workflow_config.get("node_configs", {}).get("llm", {})
    llm_model = node_config.get("llm_model", "gpt-4")
    
    # 从表检查结果中获取检查结果、状态和错误信息
    table_check_result = state.get("results", {}).get("table_check", {})
    status = table_check_result.get("status", "")
    check_result = table_check_result.get("check_result", {})
    error_message = table_check_result.get("error_message", "")
    table_schema = table_check_result.get("table_schema", {})
    
    # 模拟大模型分析逻辑，分析表检查失败的原因并给出建议
    llm_analysis = {
        "model": llm_model,
        "analysis_time": "2024-01-01T10:50:00",
        "analysis_result": "根据表检查结果，发现以下问题：",
        "suggestions": [],
        "is_blocked": True  # 当表检查失败时，阻断流程直到用户重新执行
    }
    
    # 分析表检查结果
    for table_name, check_info in check_result.items():
        if not check_info["passed"]:
            llm_analysis["analysis_result"] += f"\n- 表 {table_name} 检查失败："
            
            for check in check_info["checks"]:
                if check["status"] == "failed":
                    llm_analysis["analysis_result"] += f"\n  * {check['rule']}: {check['message']}"
                    
                    # 生成修复建议
                    if check["rule"] == "not_null":
                        llm_analysis["suggestions"].append(f"为表 {table_name} 的 {check['rule']} 字段添加非空约束")
                    elif check["rule"] == "data_type":
                        llm_analysis["suggestions"].append(f"调整表 {table_name} 的 {check['rule']} 字段数据类型")
                    elif check["rule"] == "primary_key":
                        llm_analysis["suggestions"].append(f"为表 {table_name} 添加主键约束")
                    else:
                        llm_analysis["suggestions"].append(f"修复表 {table_name} 的 {check['rule']} 检查失败问题")
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "llm": {
                "status": "success",
                "analysis_result": llm_analysis["analysis_result"],
                "suggestions": llm_analysis["suggestions"],
                "is_blocked": llm_analysis["is_blocked"]
            }
        },
        "current_node": "llm",
        "status": "blocked" if llm_analysis["is_blocked"] else "completed"  # 当阻断时，将状态设置为 blocked
    }


# 8. 制品生成节点
def artifact_generate_node(state: dict) -> Dict[str, Any]:
    print(f"Executing Artifact Generate Node for request: {state.get('request_id')}")
    
    # 获取制品生成配置
    workflow_config = state.get("workflow_config", {})
    # 检查workflow_config是否为Pydantic模型
    if hasattr(workflow_config, "node_configs"):
        # 如果是Pydantic模型，使用点符号访问
        node_config = workflow_config.node_configs.get("artifact_generate", {})
    else:
        # 如果是字典，使用get()方法
        node_config = workflow_config.get("node_configs", {}).get("artifact_generate", {})
    artifact_type = node_config.get("artifact_type", "workflow")
    artifact_location = node_config.get("artifact_location", f"artifacts/{state.get('request_id')}")
    
    # 模拟制品生成逻辑
    execution_results = state.get("results", {})
    
    # 生成制品元数据
    # 检查workflow_config是否为Pydantic模型
    if hasattr(workflow_config, "name"):
        # 如果是Pydantic模型，使用点符号访问
        workflow_name = workflow_config.name
        nodes = workflow_config.nodes
        edges = workflow_config.edges
        node_configs = workflow_config.node_configs
    else:
        # 如果是字典，使用get()方法
        workflow_name = workflow_config.get("name", "default_workflow")
        nodes = workflow_config.get("nodes", [])
        edges = workflow_config.get("edges", [])
        node_configs = workflow_config.get("node_configs", {})
    
    artifact_metadata = {
        "request_id": state.get('request_id'),
        "workflow_name": workflow_name,
        "artifact_type": artifact_type,
        "generated_at": "2024-01-01T11:00:00",
        "execution_summary": {
            "nodes_executed": len(execution_results),
            "status": "completed",
            "start_time": "2024-01-01T10:00:00",
            "end_time": "2024-01-01T11:00:00"
        },
        "content": {
            "nodes": nodes,
            "edges": edges,
            "node_configs": node_configs
        }
    }
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "artifact_generate": {
                "status": "success",
                "artifact_path": artifact_location,
                "artifact_type": artifact_type,
                "artifact_metadata": artifact_metadata
            }
        },
        "current_node": "artifact_generate"
    }


# 9. 等待网关节点
def wait_gateway_node(state: dict) -> Dict[str, Any]:
    print(f"Executing Wait Gateway Node for request: {state.get('request_id')}")
    
    # 模拟等待逻辑
    wait_config = state.get("wait_config", {})
    wait_type = wait_config.get("type", "manual_approval")
    wait_condition = wait_config.get("condition", {})
    
    # 在实际应用中，这里会检查等待条件是否满足
    # 例如，检查外部事件、时间或人工审批
    wait_completed = True  # 模拟等待完成
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "wait_gateway": {
                "status": "completed" if wait_completed else "waiting",
                "wait_type": wait_type,
                "wait_condition": wait_condition,
                "wait_completed": wait_completed
            }
        },
        "current_node": "wait_gateway"
    }


# 节点映射
NODE_MAPPING = {
    "page_submit": page_submit_node,
    "table_check": table_check_node,
    "sql_generate": sql_generate_node,
    "sql_execute": sql_execute_node,
    "integration_task_generate": integration_task_generate_node,
    "integration_task_deploy": integration_task_deploy_node,
    "llm": llm_node,
    "artifact_generate": artifact_generate_node,
    "parallel_gateway": lambda state: state,
    "exclusive_gateway": lambda state: state,
    "wait_gateway": wait_gateway_node
}
