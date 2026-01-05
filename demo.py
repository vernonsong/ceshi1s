from workflow_manager import WorkflowManager
from models import WorkflowConfig, LakeIngestionRequest


# 初始化工作流管理器
workflow_manager = WorkflowManager()


# 定义示例工作流配置 - 入湖工作流（极简版，只包含基本的条件分支）
workflow_config = WorkflowConfig(
    name="lake_ingestion_workflow",
    description="入湖工作流：对话提单->入湖表检查->大模型分析->sql生成->集成任务生成->集成任务部署",
    nodes=[
        "page_submit",
        "table_check",
        "llm",
        "sql_generate"
    ],
    edges=[
        # 对话提单 -> 入湖表检查
        {"start": "page_submit", "end": "table_check"},
        
        # 入湖表检查 -> 大模型分析（条件：表检查失败）
        {"start": "table_check", "end": "llm", "condition": {"type": "table_check_failed"}},
        
        # 入湖表检查 -> SQL生成（条件：表检查通过）
        {"start": "table_check", "end": "sql_generate", "condition": {"type": "table_check_passed"}}
    ],
    node_configs={
        "table_check": {
            "table_check_rules": ["not_null", "data_type", "primary_key", "unique_constraint"]
        },
        "llm": {
            "llm_model": "gpt-4"
        }
    }
)


# 注册工作流
print("=== 注册工作流 ===")
workflow_name = workflow_manager.register_workflow(workflow_config)
print(f"工作流注册成功: {workflow_name}")
print()


# 列出所有工作流
print("=== 列出所有工作流 ===")
workflows = workflow_manager.list_workflows()
for wf in workflows:
    print(f"- {wf['name']}: {wf['description']} ({wf['nodes_count']} nodes, {wf['edges_count']} edges)")
print()


# 定义示例入湖请求 - 对话提单
lake_request = LakeIngestionRequest(
    workflow_name="lake_ingestion_workflow",
    source_data={
        "user_input": "我需要将用户表 user_info 从 source_db_1.source_schema_1 导入到 lake_db_1.lake_schema_1 中",
        "username": "demo_user"
    },
    custom_params={
        "extra_info": "This is a demo request for lake ingestion workflow"
    }
)


# 执行工作流
print("=== 执行工作流 ===")
result = workflow_manager.execute_workflow(lake_request)
print(f"工作流执行完成")
print(f"请求ID: {result['request_id']}")
print(f"状态: {result['status']}")
print(f"错误数: {len(result['errors'])}")
print()


# 输出关键结果
print("=== 工作流执行结果 ===")
results = result['results']

# 打印对话提单结果
if 'page_submit' in results:
    print("1. 对话提单结果:")
    page_submit_result = results['page_submit']
    print(f"   状态: {page_submit_result['status']}")
    print(f"   源库: {page_submit_result['source_db']}")
    print(f"   源Schema: {page_submit_result['source_schema']}")
    print(f"   源表: {page_submit_result['source_table']}")
    print(f"   湖库: {page_submit_result['lake_db']}")
    print(f"   湖Schema: {page_submit_result['lake_schema']}")
    print(f"   湖表: {page_submit_result['lake_table']}")
    print(f"   入湖单号: {page_submit_result['ticket_id']}")
    print()

# 打印表检查结果
if 'table_check' in results:
    print("2. 入湖表检查结果:")
    table_check_result = results['table_check']
    print(f"   状态: {table_check_result['status']}")
    for table_name, check_info in table_check_result['check_result'].items():
        print(f"   {table_name}: {'通过' if check_info['passed'] else '失败'}")
        for check in check_info['checks']:
            print(f"     - {check['rule']}: {check['status']} - {check['message']}")
    print()

# 打印大模型分析结果
if 'llm' in results:
    print("3. 大模型分析结果:")
    llm_result = results['llm']
    print(f"   状态: {llm_result['status']}")
    print(f"   分析结果: {llm_result['analysis_result']}")
    print("   修复建议:")
    for suggestion in llm_result['suggestions']:
        print(f"     - {suggestion}")
    print(f"   是否阻断: {'是' if llm_result['is_blocked'] else '否'}")
    print()

# 打印SQL生成结果
if 'sql_generate' in results:
    print("4. SQL生成结果:")
    sql_result = results['sql_generate']
    print(f"   状态: {sql_result['status']}")
    print(f"   SQL查询: {sql_result['sql_query']}")
    print()

print("=== 工作流执行完成 ===")
