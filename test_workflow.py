import requests
import json
import time

# API地址
BASE_URL = "http://127.0.0.1:5000"


def test_1_manual_orchestration():
    """测试1：用户手动编排"""
    print("\n=== 测试1：用户手动编排 ===")
    
    # 1. 查看现有工作流
    print("1. 查看现有工作流...")
    response = requests.get(f"{BASE_URL}/api/workflows")
    print(f"状态码: {response.status_code}")
    workflows = response.json()
    print(f"现有工作流数量: {len(workflows)}")
    for wf in workflows:
        print(f"  - {wf['name']}: {wf['description']}")
    
    # 2. 创建包含详细出入参配置的手动编排工作流
    print("\n2. 创建包含详细出入参配置的手动编排工作流...")
    manual_workflow_config = {
        "name": "manual_test_workflow",
        "description": "包含详细出入参配置的手动编排测试工作流",
        "nodes": [
            "page_submit",
            "table_check",
            "llm",
            "sql_generate",
            "integration_task_generate",
            "sql_execute",
            "integration_task_deploy",
            "artifact_generate"
        ],
        "edges": [
            {"start": "page_submit", "end": "table_check"},
            {"start": "table_check", "end": "llm", "condition": {"type": "table_check_failed"}},
            {"start": "table_check", "end": "sql_generate", "condition": {"type": "table_check_passed"}},
            {"start": "table_check", "end": "integration_task_generate", "condition": {"type": "table_check_passed"}},
            {"start": "llm", "end": "table_check"},
            {"start": "sql_generate", "end": "sql_execute"},
            {"start": "integration_task_generate", "end": "integration_task_deploy"},
            {"start": "sql_execute", "end": "artifact_generate"},
            {"start": "integration_task_deploy", "end": "artifact_generate"}
        ],
        "node_configs": {
            "table_check": {
                "table_check_rules": ["not_null", "data_type", "primary_key", "unique_constraint"]
            },
            "integration_task_generate": {
                "integration_parallelism": 2
            }
        },
        "orchestration_type": "manual",
        # 详细的节点入参配置
        "node_inputs_config": {
            # 大模型节点的输入参数配置
            "llm": {
                "status": {
                    "type": "previous_node_output",
                    "node": "table_check",
                    "output": "status"
                },
                "check_result": {
                    "type": "previous_node_output",
                    "node": "table_check",
                    "output": "check_result"
                },
                "error_message": {
                    "type": "previous_node_output",
                    "node": "table_check",
                    "output": "error_message"
                },
                "table_schema": {
                    "type": "previous_node_output",
                    "node": "table_check",
                    "output": "table_schema"
                }
            },
            # 集成任务生成节点的输入参数配置
            "integration_task_generate": {
                "table_schema": {
                    "type": "previous_node_output",
                    "node": "table_check",
                    "output": "table_schema"
                },
                "task_path": {
                    "type": "python_code",
                    "code": "\"\"\"生成任务路径，格式为：source_db/source_schema/source_table\"\"\"\n" +
                            "source_db = table_schema.get('source_db')\n" +
                            "source_schema = table_schema.get('source_schema')\n" +
                            "source_table = table_schema.get('source_table')\n" +
                            "return f'{source_db}/{source_schema}/{source_table}'"
                }
            },
            # SQL生成节点的输入参数配置
            "sql_generate": {
                "table_schema": {
                    "type": "previous_node_output",
                    "node": "table_check",
                    "output": "table_schema"
                }
            },
            # SQL执行节点的输入参数配置
            "sql_execute": {
                "sql_query": {
                    "type": "previous_node_output",
                    "node": "sql_generate",
                    "output": "sql_query"
                },
                "execution_plan": {
                    "type": "previous_node_output",
                    "node": "sql_generate",
                    "output": "execution_plan"
                }
            },
            # 集成任务部署节点的输入参数配置
            "integration_task_deploy": {
                "task_config": {
                    "type": "previous_node_output",
                    "node": "integration_task_generate",
                    "output": "task_config"
                },
                "task_id": {
                    "type": "previous_node_output",
                    "node": "integration_task_generate",
                    "output": "task_id"
                }
            },
            # 制品生成节点的输入参数配置
            "artifact_generate": {
                "execution_results": {
                    "type": "previous_nodes_outputs",
                    "nodes": ["sql_execute", "integration_task_deploy"]
                },
                "workflow_config": {
                    "type": "workflow_config"
                }
            }
        }
    }
    
    response = requests.post(f"{BASE_URL}/api/workflows", json=manual_workflow_config)
    print(f"状态码: {response.status_code}")
    result = response.json()
    print(f"结果: {result}")
    
    # 3. 验证工作流是否创建成功
    print("\n3. 验证工作流是否创建成功...")
    response = requests.get(f"{BASE_URL}/api/workflows")
    workflows = response.json()
    manual_workflow_exists = any(wf['name'] == 'manual_test_workflow' for wf in workflows)
    print(f"手动编排工作流创建成功: {manual_workflow_exists}")
    
    # 4. 获取创建的工作流详情
    print("\n4. 获取创建的工作流详情...")
    response = requests.get(f"{BASE_URL}/api/workflows/manual_test_workflow")
    print(f"状态码: {response.status_code}")
    workflow_detail = response.json()
    print(f"工作流名称: {workflow_detail['name']}")
    print(f"工作流描述: {workflow_detail['description']}")
    print(f"节点数量: {len(workflow_detail['nodes'])}")
    print(f"边数量: {len(workflow_detail['edges'])}")
    
    # 5. 验证是否包含节点输入参数配置
    if 'node_inputs_config' in workflow_detail:
        print("\n5. 验证节点输入参数配置...")
        node_inputs_config = workflow_detail['node_inputs_config']
        print(f"包含输入参数配置的节点数量: {len(node_inputs_config)}")
        
        # 验证集成任务生成节点的输入参数配置
        if 'integration_task_generate' in node_inputs_config:
            print("\n集成任务生成节点输入参数配置:")
            for param_name, param_config in node_inputs_config['integration_task_generate'].items():
                print(f"  - {param_name}: {param_config['type']}")
                if param_config['type'] == 'python_code':
                    print(f"    Python代码: {param_config['code'][:100]}...")
        
        # 验证大模型节点的输入参数配置
        if 'llm' in node_inputs_config:
            print("\n大模型节点输入参数配置:")
            for param_name, param_config in node_inputs_config['llm'].items():
                print(f"  - {param_name}: {param_config['type']}")
                if param_config['type'] == 'previous_node_output':
                    print(f"    来源节点: {param_config['node']}.{param_config['output']}")
    
    return manual_workflow_exists


def test_2_ai_orchestration():
    """测试2：AI编排"""
    print("\n=== 测试2：AI编排 ===")
    
    # 1. 发送AI编排请求
    print("1. 发送AI编排请求...")
    ai_orchestration_request = {
        "business_requirement": "将用户对话中提取的表信息导入到湖仓，需要进行表结构检查、SQL生成和集成任务部署，支持并行执行SQL生成和集成任务生成",
        "acceptance_criteria": [
            "工作流必须包含入湖对象检查节点",
            "工作流必须包含SQL生成节点",
            "工作流必须包含集成任务生成节点",
            "SQL生成和集成任务生成必须并行执行",
            "表检查失败时必须执行大模型分析",
            "工作流必须生成最终制品"
        ],
        "source_data_example": {
            "user_input": "请将数据库 source_db_1 中 source_schema_1 下的 source_table_1 表导入到湖仓 lake_db_1 中的 lake_schema_1 下，表名为 lake_table_1",
            "username": "test_user"
        }
    }
    
    response = requests.post(f"{BASE_URL}/api/orchestrate/ai", json=ai_orchestration_request)
    print(f"状态码: {response.status_code}")
    result = response.json()
    print(f"AI编排状态: {result['status']}")
    
    # 2. 验证AI生成的工作流
    print("\n2. 验证AI生成的工作流...")
    ai_workflow_name = result['final_workflow_config']['name']
    print(f"AI生成的工作流名称: {ai_workflow_name}")
    
    # 3. 检查工作流是否已注册
    response = requests.get(f"{BASE_URL}/api/workflows")
    workflows = response.json()
    ai_workflow_exists = any(wf['name'] == ai_workflow_name for wf in workflows)
    print(f"AI生成的工作流注册成功: {ai_workflow_exists}")
    
    return ai_workflow_exists, ai_workflow_name


def test_3_workflow_execution(workflow_name):
    """测试3：保存的流程图运行"""
    print(f"\n=== 测试3：保存的流程图运行 ({workflow_name}) ===")
    
    # 1. 执行工作流
    print("1. 执行工作流...")
    execution_request = {
        "source_data": {
            "user_input": "请将数据库 source_db_1 中 source_schema_1 下的 source_table_1 表导入到湖仓 lake_db_1 中的 lake_schema_1 下，表名为 lake_table_1",
            "username": "test_user"
        },
        "custom_params": {
            "table_check_rules": ["not_null", "data_type", "primary_key"]
        }
    }
    
    response = requests.post(f"{BASE_URL}/api/workflows/{workflow_name}/execute", json=execution_request)
    print(f"状态码: {response.status_code}")
    result = response.json()
    print(f"执行结果: {result['status']}")
    print(f"请求ID: {result['request_id']}")
    
    # 2. 验证执行结果
    print("\n2. 验证执行结果...")
    results = result.get('results', {})
    executed_nodes = list(results.keys())
    print(f"执行的节点数量: {len(executed_nodes)}")
    print(f"执行的节点: {executed_nodes}")
    
    # 检查关键节点是否执行
    key_nodes = ["page_submit", "table_check"]
    key_nodes_executed = all(node in executed_nodes for node in key_nodes)
    print(f"关键节点执行情况: {key_nodes_executed}")
    
    return key_nodes_executed


def test_4_get_nodes_metadata():
    """测试4：获取节点元数据"""
    print("\n=== 测试4：获取节点元数据 ===")
    
    # 获取节点元数据
    response = requests.get(f"{BASE_URL}/api/nodes/metadata")
    print(f"状态码: {response.status_code}")
    metadata = response.json()
    print(f"获取到的节点数量: {len(metadata)}")
    
    # 验证核心节点是否存在
    core_nodes = ["table_check", "sql_generate", "integration_task_generate", "artifact_generate"]
    core_nodes_exists = all(node in metadata for node in core_nodes)
    print(f"核心节点存在情况: {core_nodes_exists}")
    
    # 打印部分节点信息
    for node_name in list(metadata.keys())[:3]:
        node = metadata[node_name]
        print(f"\n节点 {node_name} 信息:")
        print(f"  类型: {node['type']}")
        print(f"  输入参数数量: {len(node['inputs'])}")
        print(f"  输出参数数量: {len(node['outputs'])}")
    
    return core_nodes_exists


if __name__ == "__main__":
    print("开始工作流系统测试...")
    
    # 初始化测试结果变量
    test_1_result = False
    test_2_result = False
    ai_workflow_name = ""
    test_3_default_result = False
    test_3_ai_result = False
    test_4_result = False
    
    try:
        # 测试1：手动编排
        test_1_result = test_1_manual_orchestration()
        
        # 测试2：AI编排
        test_2_result, ai_workflow_name = test_2_ai_orchestration()
        
        # 测试3：运行默认工作流
        test_3_default_result = test_3_workflow_execution("standard_lake_ingestion")
        
        # 测试4：运行AI生成的工作流
        if ai_workflow_name:
            test_3_ai_result = test_3_workflow_execution(ai_workflow_name)
        
        # 测试5：获取节点元数据
        test_4_result = test_4_get_nodes_metadata()
    except Exception as e:
        print(f"测试过程中发生错误: {e}")
    
    # 总结测试结果
    print("\n=== 测试总结 ===")
    print(f"1. 用户手动编排测试: {'通过' if test_1_result else '失败'}")
    print(f"2. AI编排测试: {'通过' if test_2_result else '失败'}")
    print(f"3. 默认工作流运行测试: {'通过' if test_3_default_result else '失败'}")
    print(f"4. AI生成工作流运行测试: {'通过' if test_3_ai_result else '失败'}")
    print(f"5. 节点元数据获取测试: {'通过' if test_4_result else '失败'}")
    
    all_tests_passed = all([
        test_1_result,
        test_2_result,
        test_3_default_result,
        test_3_ai_result,
        test_4_result
    ])
    
    print(f"\n所有测试结果: {'全部通过' if all_tests_passed else '部分失败'}")
