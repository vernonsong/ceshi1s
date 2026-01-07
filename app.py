from flask import Flask, request, jsonify
from flask_cors import CORS
from workflow_manager import WorkflowManager
from models import WorkflowConfig, LakeIngestionRequest, AIOchestrationRequest, AIOchestrationResult, AIOchestrationIteration
import uuid
from datetime import datetime
import random
import requests
import json
from typing import Dict, Any, List

# 读取.env文件
from dotenv import load_dotenv
import os
load_dotenv()

# 大模型配置
DASHSCOPE_API_KEY = os.getenv("ALIYUN_KEY")
DASHSCOPE_API_URL = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
MODEL_NAME = "qwen-plus-latest"

# 大模型调用函数
def call_llm(prompt: str) -> str:
    """调用通义千问大模型
    
    Args:
        prompt: 提示词
    
    Returns:
        大模型返回的结果
    """
    headers = {
        "Authorization": f"Bearer {DASHSCOPE_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # 检查API密钥是否存在
    if not DASHSCOPE_API_KEY:
        print("大模型API密钥未配置，使用模拟响应")
        # 返回模拟的大模型响应
        return "{\n  \"nodes\": [\"page_submit\", \"table_check\", \"llm\", \"sql_generate\", \"integration_task_generate\", \"sql_execute\", \"integration_task_deploy\", \"artifact_generate\"],\n  \"edges\": [{\n    \"start\": \"page_submit\",\n    \"end\": \"table_check\"\n  }, {\n    \"start\": \"table_check\",\n    \"end\": \"llm\",\n    \"condition\": {\n      \"type\": \"table_check_failed\"\n    }\n  }, {\n    \"start\": \"table_check\",\n    \"end\": \"sql_generate\",\n    \"condition\": {\n      \"type\": \"table_check_passed\"\n    }\n  }, {\n    \"start\": \"table_check\",\n    \"end\": \"integration_task_generate\",\n    \"condition\": {\n      \"type\": \"table_check_passed\"\n    }\n  }, {\n    \"start\": \"llm\",\n    \"end\": \"table_check\"\n  }, {\n    \"start\": \"sql_generate\",\n    \"end\": \"sql_execute\"\n  }, {\n    \"start\": \"integration_task_generate\",\n    \"end\": \"integration_task_deploy\"\n  }, {\n    \"start\": \"sql_execute\",\n    \"end\": \"artifact_generate\"\n  }, {\n    \"start\": \"integration_task_deploy\",\n    \"end\": \"artifact_generate\"\n  }],\n  \"node_configs\": {\n    \"table_check\": {\n      \"table_check_rules\": [\"not_null\", \"data_type\", \"primary_key\", \"unique_constraint\"],\n      \"parallelism\": 2\n    },\n    \"sql_generate\": {\n      \"generate_ddl\": true,\n      \"generate_dml\": true,\n      \"target_engine\": \"doris\",\n      \"parallelism\": 2\n    },\n    \"integration_task_generate\": {\n      \"parallelism\": 2,\n      \"source_connector\": \"jdbc\",\n      \"target_connector\": \"doris\"\n    },\n    \"llm\": {\n      \"model_name\": \"qwen-plus-latest\"\n    }\n  }\n}"
    
    data = {
        "model": MODEL_NAME,
        "input": {
            "messages": [
                {
                    "role": "system",
                    "content": "你是一个专业的工作流编排和验证专家，精通数据入湖流程和工作流设计。"
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        },
        "parameters": {
            "temperature": 0.7,
            "top_p": 0.95,
            "max_tokens": 2000
        }
    }
    
    try:
        response = requests.post(DASHSCOPE_API_URL, headers=headers, json=data, timeout=30)
        response.raise_for_status()
        result = response.json()
        llm_text = result["output"]["text"]
        print(f"\n=== 大模型调用成功，返回结果 ===")
        print(f"{llm_text}")
        print(f"=== 大模型响应结束 ===\n")
        return llm_text
    except Exception as e:
        print(f"大模型调用失败: {e}")
        if 'response' in locals():
            print(f"响应状态码: {response.status_code}")
            print(f"响应内容: {response.text}")
        else:
            print(f"没有收到响应")
        # 返回模拟的大模型响应
        print("使用模拟的大模型响应")
        return "{\n  \"nodes\": [\"page_submit\", \"table_check\", \"llm\", \"sql_generate\", \"integration_task_generate\", \"sql_execute\", \"integration_task_deploy\", \"artifact_generate\"],\n  \"edges\": [{\n    \"start\": \"page_submit\",\n    \"end\": \"table_check\"\n  }, {\n    \"start\": \"table_check\",\n    \"end\": \"llm\",\n    \"condition\": {\n      \"type\": \"table_check_failed\"\n    }\n  }, {\n    \"start\": \"table_check\",\n    \"end\": \"sql_generate\",\n    \"condition\": {\n      \"type\": \"table_check_passed\"\n    }\n  }, {\n    \"start\": \"table_check\",\n    \"end\": \"integration_task_generate\",\n    \"condition\": {\n      \"type\": \"table_check_passed\"\n    }\n  }, {\n    \"start\": \"llm\",\n    \"end\": \"table_check\"\n  }, {\n    \"start\": \"sql_generate\",\n    \"end\": \"sql_execute\"\n  }, {\n    \"start\": \"integration_task_generate\",\n    \"end\": \"integration_task_deploy\"\n  }, {\n    \"start\": \"sql_execute\",\n    \"end\": \"artifact_generate\"\n  }, {\n    \"start\": \"integration_task_deploy\",\n    \"end\": \"artifact_generate\"\n  }],\n  \"node_configs\": {\n    \"table_check\": {\n      \"table_check_rules\": [\"not_null\", \"data_type\", \"primary_key\", \"unique_constraint\"],\n      \"parallelism\": 2\n    },\n    \"sql_generate\": {\n      \"generate_ddl\": true,\n      \"generate_dml\": true,\n      \"target_engine\": \"doris\",\n      \"parallelism\": 2\n    },\n    \"integration_task_generate\": {\n      \"parallelism\": 2,\n      \"source_connector\": \"jdbc\",\n      \"target_connector\": \"doris\"\n    },\n    \"llm\": {\n      \"model_name\": \"qwen-plus-latest\"\n    }\n  }\n}"

app = Flask(__name__)
# 添加CORS支持
CORS(app)

# 初始化工作流管理器
workflow_manager = WorkflowManager()


@app.route('/api/workflows', methods=['GET'])
def get_workflows():
    """获取所有工作流"""
    workflows = workflow_manager.list_workflows()
    return jsonify(workflows)


@app.route('/api/workflows', methods=['POST'])
def create_workflow():
    """创建新工作流"""
    config_data = request.get_json()
    config = WorkflowConfig(**config_data)
    workflow_name = workflow_manager.register_workflow(config)
    return jsonify({"name": workflow_name, "message": "Workflow created successfully"})


@app.route('/api/workflows/<workflow_name>', methods=['GET'])
def get_workflow(workflow_name):
    """获取指定工作流"""
    config = workflow_manager.get_workflow_config(workflow_name)
    return jsonify(config.dict())


@app.route('/api/workflows/<workflow_name>/execute', methods=['POST'])
def execute_workflow(workflow_name):
    """执行工作流"""
    request_data = request.get_json()
    lake_request = LakeIngestionRequest(
        workflow_name=workflow_name,
        source_data=request_data.get('source_data', {}),
        custom_params=request_data.get('custom_params', {})
    )
    result = workflow_manager.execute_workflow(lake_request)
    return jsonify(result)


def mock_sql_execution(sql_query: str, database: str = "lake_db_1") -> Dict[str, Any]:
    """模拟SQL执行工具
    
    Args:
        sql_query: 要执行的SQL查询
        database: 目标数据库
    
    Returns:
        SQL执行结果
    """
    # 模拟SQL执行结果
    if "SELECT" in sql_query.upper():
        return {
            "status": "success",
            "data": [
                {"column1": "value1", "column2": "value2"},
                {"column1": "value3", "column2": "value4"}
            ],
            "rows_affected": 2,
            "execution_time": 0.5
        }
    elif "CREATE TABLE" in sql_query.upper():
        return {
            "status": "success",
            "rows_affected": 0,
            "message": "Table created successfully",
            "execution_time": 1.2
        }
    elif "INSERT" in sql_query.upper():
        return {
            "status": "success",
            "rows_affected": 100,
            "message": "Data inserted successfully",
            "execution_time": 2.5
        }
    else:
        return {
            "status": "failed",
            "error": f"Unsupported SQL operation: {sql_query[:50]}...",
            "execution_time": 0.1
        }


def mock_task_query(task_id: str) -> Dict[str, Any]:
    """模拟任务查询工具
    
    Args:
        task_id: 任务ID
    
    Returns:
        任务状态
    """
    # 模拟任务状态查询结果
    return {
        "task_id": task_id,
        "status": "running",
        "start_time": "2024-01-01T10:00:00",
        "estimated_completion": "2024-01-01T10:15:00",
        "progress": 65,
        "logs": [
            "Task started",
            "Processing data...",
            "65% completed"
        ]
    }


def validate_workflow(config: WorkflowConfig, acceptance_criteria: List[str] = None) -> Dict[str, Any]:
    """验证智能体：调用大模型进行工作流验证
    
    Args:
        config: 工作流配置
        acceptance_criteria: 验收标准列表
    
    Returns:
        验证结果，包含验证计划、执行结果和最终结论
    """
    # 默认验收标准
    if not acceptance_criteria:
        acceptance_criteria = [
            "工作流必须包含入湖对象检查节点",
            "工作流必须包含SQL生成节点",
            "工作流必须包含集成任务生成节点",
            "SQL生成和集成任务生成必须并行执行",
            "表检查失败时必须执行大模型分析",
            "工作流必须生成最终制品"
        ]
    
    print(f"验证智能体：调用大模型验证工作流配置...")
    print(f"工作流名称: {config.name}")
    print(f"验收标准: {acceptance_criteria}")
    
    # 1. 生成验证计划
    print("\n验证智能体：生成验证计划...")
    validation_plan = {
        "steps": [],
        "tools": [],
        "acceptance_criteria": acceptance_criteria
    }
    
    # 根据验收标准生成验证步骤
    for i, criterion in enumerate(acceptance_criteria):
        step = {
            "id": i + 1,
            "description": criterion,
            "status": "pending",
            "tools": [],
            "result": None
        }
        
        # 根据验收标准确定需要调用的工具
        if "入湖对象检查" in criterion:
            step["tools"].append({"name": "workflow_analyzer", "action": "check_node_exists", "params": {"node_name": "table_check"}})
        elif "SQL生成" in criterion:
            step["tools"].append({"name": "workflow_analyzer", "action": "check_node_exists", "params": {"node_name": "sql_generate"}})
        elif "集成任务生成" in criterion:
            step["tools"].append({"name": "workflow_analyzer", "action": "check_node_exists", "params": {"node_name": "integration_task_generate"}})
        elif "并行执行" in criterion:
            step["tools"].append({"name": "workflow_analyzer", "action": "check_parallel_execution", "params": {"nodes": ["sql_generate", "integration_task_generate"]}})
        elif "大模型分析" in criterion:
            step["tools"].append({"name": "workflow_analyzer", "action": "check_conditional_edge", "params": {"start_node": "table_check", "end_node": "llm", "condition": "table_check_failed"}})
        elif "生成最终制品" in criterion:
            step["tools"].append({"name": "workflow_analyzer", "action": "check_node_exists", "params": {"node_name": "artifact_generate"}})
        
        validation_plan["steps"].append(step)
    
    # 2. 执行验证计划
    print("\n验证智能体：执行验证计划...")
    validation_results = []
    all_passed = True
    
    for step in validation_plan["steps"]:
        step_results = []
        step_passed = True
        
        # 模拟工具执行
        for tool in step["tools"]:
            tool_result = {
                "tool": tool["name"],
                "action": tool["action"],
                "status": "success",
                "result": None,
                "execution_time": 0.5
            }
            
            # 根据工具类型模拟执行结果
            if tool["name"] == "workflow_analyzer":
                if tool["action"] == "check_node_exists":
                    node_name = tool["params"]["node_name"]
                    exists = node_name in config.nodes
                    tool_result["result"] = {"exists": exists, "node_name": node_name}
                    if not exists:
                        tool_result["status"] = "failed"
                        step_passed = False
                        all_passed = False
                elif tool["action"] == "check_parallel_execution":
                    # 检查是否有从同一节点到两个目标节点的条件边
                    parallel_edges = [
                        edge for edge in config.edges 
                        if edge["start"] == "table_check" 
                        and edge["end"] in ["sql_generate", "integration_task_generate"]
                    ]
                    is_parallel = len(parallel_edges) == 2
                    tool_result["result"] = {"is_parallel": is_parallel, "nodes": tool["params"]["nodes"]}
                    if not is_parallel:
                        tool_result["status"] = "failed"
                        step_passed = False
                        all_passed = False
                elif tool["action"] == "check_conditional_edge":
                    # 检查条件边是否存在
                    conditional_edge = next(
                        (edge for edge in config.edges 
                         if edge["start"] == tool["params"]["start_node"] 
                         and edge["end"] == tool["params"]["end_node"]),
                        None
                    )
                    edge_exists = conditional_edge is not None
                    tool_result["result"] = {"edge_exists": edge_exists, "start_node": tool["params"]["start_node"], "end_node": tool["params"]["end_node"]}
                    if not edge_exists:
                        tool_result["status"] = "failed"
                        step_passed = False
                        all_passed = False
            
            step_results.append(tool_result)
        
        # 更新步骤状态
        step["status"] = "passed" if step_passed else "failed"
        step["results"] = step_results
        validation_results.append(step)
    
    # 3. 检查并行度配置
    integration_config = config.node_configs.get("integration_task_generate", {})
    parallelism = integration_config.get("integration_parallelism", 1)
    
    if parallelism > 4:
        # 并行度过高问题
        validation_results.append({
            "id": len(validation_results) + 1,
            "description": "系统并行度检查",
            "status": "failed",
            "tools": [{"name": "system_checker", "action": "check_parallelism_limit"}],
            "results": [
                {
                    "tool": "system_checker",
                    "action": "check_parallelism_limit",
                    "status": "failed",
                    "result": {"current_parallelism": parallelism, "max_parallelism": 4},
                    "execution_time": 0.3
                }
            ]
        })
        all_passed = False
    
    # 4. 模拟执行流程图
    execution_result = {
        "status": "success" if all_passed else "failed",
        "nodes_executed": ["page_submit", "table_check", "sql_generate", "integration_task_generate", "sql_execute", "integration_task_deploy", "artifact_generate"] if all_passed else ["page_submit", "table_check"],
        "errors": [] if all_passed else [
            {"message": f"DAG执行失败：并行度 {parallelism} 超过系统限制", "node": "integration_task_generate"}
        ],
        "execution_plan": {
            "steps": [
                "page_submit",
                "table_check",
                "sql_generate",
                "integration_task_generate",
                "sql_execute",
                "integration_task_deploy",
                "artifact_generate"
            ],
            "parallel_steps": [
                {"nodes": ["sql_generate", "integration_task_generate"], "start_node": "table_check"}
            ]
        }
    }
    
    # 5. 调用大模型分析验证结果
    print("\n验证智能体：调用大模型分析验证结果...")
    
    # 构建大模型提示词
    validation_issues = []
    for result in validation_results:
        if result["status"] == "failed":
            for tool_result in result["results"]:
                if tool_result["status"] == "failed":
                    validation_issues.append({
                        "description": result["description"],
                        "details": tool_result["result"],
                        "step_id": result["id"]
                    })
    
    # 构建JSON格式的示例输出，避免在f-string中嵌套太多大括号
    json_example = '''
    {
        "validation_status": "valid" 或 "invalid",
        "feedback": "验证反馈和分析",
        "improvement_suggestions": ["建议1", "建议2", ...],
        "fix_actions": {
            "nodes_to_add": ["节点1", "节点2"],
            "edges_to_add": [{"start": "节点1", "end": "节点2"}],
            "node_configs_to_update": {
                "节点1": {"配置项": "值"}
            }
        }
    }
    '''
    
    prompt = f"""
    请分析以下工作流配置的验证结果，并给出改进建议。
    
    工作流配置：
    - 名称：{config.name}
    - 节点：{config.nodes}
    - 边：{config.edges}
    - 节点配置：{config.node_configs}
    
    验收标准：
    {"\n".join([f"- {c}" for c in acceptance_criteria])}
    
    验证结果：
    - 验证状态：{"通过" if all_passed else "失败"}
    - 执行状态：{execution_result["status"]}
    - 执行的节点：{execution_result["nodes_executed"]}
    - 错误信息：{execution_result["errors"]}
    - 验证问题：{validation_issues}
    
    请按照以下JSON格式输出分析结果：
    {json_example}
    
    注意事项：
    1. 严格按照JSON格式输出，不要输出其他内容
    2. 分析要详细，覆盖所有验证问题
    3. 改进建议要具体，可操作
    4. fix_actions要具体，说明需要添加的节点、边和需要更新的节点配置
    """
    
    # 调用大模型
    llm_response = call_llm(prompt)
    print(f"大模型验证分析: {llm_response}")
    
    # 6. 解析大模型响应
    llm_analysis = {
        "validation_status": "valid" if all_passed else "invalid",
        "feedback": "工作流配置验证通过" if all_passed else "工作流配置存在问题",
        "improvement_suggestions": [],
        "fix_actions": {
            "nodes_to_add": [],
            "edges_to_add": [],
            "node_configs_to_update": {}
        }
    }
    
    try:
        if llm_response.strip():
            parsed_analysis = json.loads(llm_response)
            llm_analysis.update(parsed_analysis)
            print("\n验证智能体：成功解析大模型验证分析")
    except json.JSONDecodeError as e:
        print(f"\n验证智能体：无法解析大模型验证分析，使用默认分析结果: {e}")
    except Exception as e:
        print(f"\n验证智能体：处理大模型验证分析时出错，使用默认分析结果: {e}")
    
    # 7. 模拟调用SQL执行工具和任务查询工具
    sql_result = mock_sql_execution("SELECT * FROM lake_table_1 LIMIT 5")
    task_result = mock_task_query("INT-TEST-001")
    
    # 8. 生成最终验证结果
    validation_result = {
        "status": llm_analysis["validation_status"],
        "issues": validation_issues,
        "feedback": llm_analysis["feedback"],
        "validation_plan": validation_plan,
        "validation_results": validation_results,
        "tool_execution_results": {
            "sql_execution": sql_result,
            "task_query": task_result
        },
        "llm_analysis": llm_analysis
    }
    
    # 如果有问题，添加详细的问题描述
    if llm_analysis["validation_status"] == "invalid":
        for issue in validation_issues:
            validation_result["issues"].append({
                "type": "validation_failed",
                "message": issue["description"],
                "details": issue["details"],
                "step_id": issue["step_id"]
            })
    
    print(f"\n验证智能体：验证完成，结果: {validation_result['status']}")
    return {
        "validation_result": validation_result,
        "execution_result": execution_result
    }


def adjust_workflow(config: WorkflowConfig, validation_result: Dict[str, Any]) -> WorkflowConfig:
    """编排智能体：根据大模型分析结果调整工作流配置
    
    Args:
        config: 当前工作流配置
        validation_result: 验证结果，包含大模型分析
    
    Returns:
        调整后的工作流配置
    """
    print(f"编排智能体：根据大模型分析结果调整工作流配置...")
    
    # 复制配置
    from copy import deepcopy
    adjusted_config = deepcopy(config)
    
    # 检查验证结果
    if validation_result["status"] == "invalid":
        # 检查是否包含大模型分析结果
        if "llm_analysis" in validation_result:
            llm_analysis = validation_result["llm_analysis"]
            fix_actions = llm_analysis.get("fix_actions", {})
            
            print(f"根据大模型分析调整工作流，fix_actions: {fix_actions}")
            
            # 1. 添加新节点
            nodes_to_add = fix_actions.get("nodes_to_add", [])
            for node in nodes_to_add:
                if node not in adjusted_config.nodes:
                    adjusted_config.nodes.append(node)
            
            # 2. 添加新边
            edges_to_add = fix_actions.get("edges_to_add", [])
            for edge in edges_to_add:
                adjusted_config.edges.append(edge)
            
            # 3. 更新节点配置
            node_configs_to_update = fix_actions.get("node_configs_to_update", {})
            for node_name, config_updates in node_configs_to_update.items():
                if node_name not in adjusted_config.node_configs:
                    adjusted_config.node_configs[node_name] = {}
                adjusted_config.node_configs[node_name].update(config_updates)
        else:
            # 如果没有大模型分析结果，使用默认调整逻辑
            print("没有大模型分析结果，使用默认调整逻辑...")
            for issue in validation_result["issues"]:
                if issue.get("message") and "并行度" in issue["message"]:
                    # 降低并行度
                    if "integration_task_generate" in adjusted_config.node_configs:
                        adjusted_config.node_configs["integration_task_generate"]["integration_parallelism"] = 2
                    else:
                        adjusted_config.node_configs["integration_task_generate"] = {
                            "integration_parallelism": 2
                        }
                elif issue.get("message") and "集成任务生成" in issue["message"]:
                    # 检查条件边配置
                    # 确保table_check到integration_task_generate的条件边正确配置
                    integration_edge_exists = any(
                        edge["start"] == "table_check" and edge["end"] == "integration_task_generate" 
                        for edge in adjusted_config.edges
                    )
                    if not integration_edge_exists:
                        # 添加缺少的边
                        adjusted_config.edges.append({
                            "start": "table_check", 
                            "end": "integration_task_generate", 
                            "condition": {"type": "table_check_passed"}
                        })
    
    # 无论是否有大模型分析，都确保并行度不超过4
    if "integration_task_generate" in adjusted_config.node_configs:
        current_parallelism = adjusted_config.node_configs["integration_task_generate"].get("integration_parallelism", 1)
        if current_parallelism > 4:
            adjusted_config.node_configs["integration_task_generate"]["integration_parallelism"] = 2
            print(f"调整并行度：从 {current_parallelism} 降低到 2")
    
    return adjusted_config


def orchestration_agent(business_requirement: str, acceptance_criteria: List[str]) -> WorkflowConfig:
    """编排智能体：调用大模型生成工作流配置
    
    Args:
        business_requirement: 业务需求描述
        acceptance_criteria: 验收标准列表
    
    Returns:
        生成的工作流配置
    """
    print(f"编排智能体：调用大模型分析业务需求...")
    print(f"业务需求: {business_requirement}")
    print(f"验收标准: {acceptance_criteria}")
    
    # 1. 调用大模型生成工作流配置
    print("\n编排智能体：调用大模型生成工作流配置...")
    
    # 构建JSON格式的示例输出，避免在f-string中嵌套太多大括号
    json_example = '''
    {
        "nodes": ["节点1", "节点2", ...],
        "edges": [{"start": "节点1", "end": "节点2", "condition": {"type": "条件类型"}}],
        "node_configs": {
            "节点1": {
                "配置项1": "值1",
                "配置项2": "值2"
            }
        }
    }
    '''
    
    # 构建提示词
    prompt = f"""
    请根据以下业务需求和验收标准，生成一个完整的入湖工作流配置。
    
    业务需求：
    {business_requirement}
    
    验收标准：
    {"\n".join([f"- {c}" for c in acceptance_criteria])}
    
    请按照以下JSON格式输出工作流配置：
    {json_example}
    
    可用节点列表：
    - page_submit：页面提单节点，从用户对话中提取入湖信息
    - table_check：入湖对象检查节点，检查源表和目标表的结构和数据
    - llm：大模型分析节点，分析表检查结果并生成修复建议
    - sql_generate：SQL生成节点，生成创建表和插入数据的SQL
    - sql_execute：SQL执行节点，执行生成的SQL语句
    - integration_task_generate：集成任务生成节点，生成数据集成任务
    - integration_task_deploy：集成任务部署节点，部署数据集成任务
    - artifact_generate：制品生成节点，生成工作流制品
    
    注意事项：
    1. 确保所有验收标准都被满足
    2. 节点配置要合理，特别是并行度不能超过4
    3. 边配置要支持分支、并行等流程控制
    4. 只输出JSON格式，不要输出其他内容
    """
    
    # 调用大模型
    llm_response = call_llm(prompt)
    print(f"\n=== 大模型完整响应开始 ===")
    print(f"{llm_response}")
    print(f"=== 大模型完整响应结束 ===\n")
    
    # 2. 解析大模型响应
    workflow_config_data = {
        "nodes": ["page_submit", "table_check", "llm", "sql_generate", "integration_task_generate", "sql_execute", "integration_task_deploy", "artifact_generate"],
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
            },
            "llm": {
                "llm_model": "qwen-plus-latest"
            }
        },
        "llm_response": llm_response  # 保存大模型响应
    }
    
    # 尝试解析大模型响应
    try:
        if llm_response.strip():
            parsed_response = json.loads(llm_response)
            # 合并大模型生成的配置
            if "nodes" in parsed_response:
                workflow_config_data["nodes"] = parsed_response["nodes"]
            if "edges" in parsed_response:
                workflow_config_data["edges"] = parsed_response["edges"]
            if "node_configs" in parsed_response:
                workflow_config_data["node_configs"] = parsed_response["node_configs"]
            print("\n成功解析大模型响应，使用生成的工作流配置")
    except json.JSONDecodeError as e:
        print(f"\n无法解析大模型响应，使用默认配置: {e}")
    except Exception as e:
        print(f"\n处理大模型响应时出错，使用默认配置: {e}")
    
    print(f"最终节点配置: {workflow_config_data['nodes']}")
    print(f"最终边配置: {workflow_config_data['edges']}")
    print(f"最终节点配置: {workflow_config_data['node_configs']}")
    
    # 3. 生成完整的工作流配置
    workflow_name = f"ai_generated_workflow_{str(uuid.uuid4())[:8]}"
    workflow_config = WorkflowConfig(
        name=workflow_name,
        description=f"大模型生成的入湖工作流 - {business_requirement[:50]}...",
        nodes=workflow_config_data["nodes"],
        edges=workflow_config_data["edges"],
        node_configs=workflow_config_data["node_configs"],
        orchestration_type="ai",
        acceptance_criteria=acceptance_criteria,
        generated_by="orchestration_agent_with_llm",
        llm_response=workflow_config_data.get("llm_response")  # 添加大模型响应
    )
    
    print(f"\n编排智能体：生成工作流配置完成！")
    return workflow_config


@app.route('/api/orchestrate/ai', methods=['POST'])
def ai_orchestrate():
    """AI编排请求端点：编排智能体和验证智能体的实际交互"""
    request_data = request.get_json()
    ai_request = AIOchestrationRequest(**request_data)
    
    # 生成请求ID
    request_id = str(uuid.uuid4())
    
    # AI编排过程：编排智能体和验证智能体的交互
    iterations = []
    max_iterations = 5
    iteration_count = 1
    validation_passed = False
    
    current_config = None
    
    while iteration_count <= max_iterations and not validation_passed:
        print(f"\n=== AI编排迭代 {iteration_count} ===")
        
        # 1. 编排智能体：生成/调整工作流配置
        if iteration_count == 1:
            # 第一次迭代：生成初始工作流配置
            print("\n1. 编排智能体：生成初始工作流配置...")
            current_config = orchestration_agent(
                ai_request.business_requirement,
                ai_request.acceptance_criteria
            )
        else:
            # 后续迭代：根据验证结果调整工作流配置
            print("\n1. 编排智能体：根据验证结果调整工作流配置...")
            current_config = adjust_workflow(current_config, previous_validation_result)
        
        # 2. 验证智能体：根据验收标准验证工作流配置
        print("\n2. 验证智能体：开始验证工作流配置...")
        validation_output = validate_workflow(current_config, ai_request.acceptance_criteria)
        validation_result = validation_output["validation_result"]
        execution_result = validation_output["execution_result"]
        
        # 保存验证结果，用于下一次迭代调整
        previous_validation_result = validation_result
        
        # 3. 创建迭代记录
        iteration = AIOchestrationIteration(
            iteration_number=iteration_count,
            workflow_config=current_config,
            execution_result=execution_result,
            validation_result=validation_result,
            feedback=validation_result["feedback"]
        )
        iterations.append(iteration)
        
        # 4. 检查验证结果
        print(f"\n3. 验证结果：{validation_result['status']}")
        print(f"   反馈：{validation_result['feedback']}")
        
        if validation_result["status"] == "valid":
            validation_passed = True
            print(f"\n=== 迭代 {iteration_count}：验证通过！ ===")
            break
        else:
            # 打印详细问题
            print("\n   详细问题：")
            for issue in validation_result["issues"]:
                print(f"   - {issue['message']} (节点: {issue.get('node', '全局')})")
            
            print(f"\n=== 迭代 {iteration_count}：验证失败，准备调整... ===")
            iteration_count += 1
    
    # 如果超过最大迭代次数仍未通过，使用最后一次调整的配置
    if not validation_passed:
        print(f"\n=== 超过最大迭代次数 {max_iterations}，使用最后一次调整的配置 ===")
    
    # 注册生成的工作流
    workflow_manager.register_workflow(current_config)
    
    # 返回结果，包含详细的验证计划和执行结果
    ai_result = AIOchestrationResult(
        request_id=request_id,
        status="completed",
        business_requirement=ai_request.business_requirement,
        acceptance_criteria=ai_request.acceptance_criteria,
        iterations=iterations,
        final_workflow_config=current_config
    )
    
    return jsonify(ai_result.model_dump())


@app.route('/api/orchestrate/ai/<request_id>', methods=['GET'])
def get_ai_orchestration_result(request_id):
    """获取AI编排结果"""
    # 模拟获取AI编排结果
    # 在实际应用中，这里会从数据库或缓存中获取结果
    return jsonify({
        "request_id": request_id,
        "status": "completed",
        "message": "AI编排已完成"
    })


@app.route('/api/nodes/metadata', methods=['GET'])
def get_nodes_metadata():
    """获取所有节点的元数据"""
    try:
        from nodes import NODE_METADATA
        metadata_dict = {name: metadata.model_dump() for name, metadata in NODE_METADATA.items()}
        return jsonify(metadata_dict)
    except Exception as e:
        print(f"获取节点元数据失败: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/', methods=['GET'])
def index():
    """前端页面入口"""
    return app.send_static_file('index.html')


if __name__ == '__main__':
    # 注册默认工作流（手动编排示例）
    default_config = WorkflowConfig(
        name="standard_lake_ingestion",
        description="标准入湖工作流",
        nodes=[
            "page_submit",
            "table_check",
            "llm",
            "sql_generate",
            "integration_task_generate",
            "sql_execute",
            "integration_task_deploy",
            "artifact_generate"
        ],
        edges=[
            {"start": "page_submit", "end": "table_check"},
            {"start": "table_check", "end": "llm", "condition": {"type": "table_check_failed"}},
            {"start": "table_check", "end": "sql_generate", "condition": {"type": "table_check_passed"}},
            {"start": "table_check", "end": "integration_task_generate", "condition": {"type": "table_check_passed"}},
            {"start": "llm", "end": "table_check"},  # 修复后重新检查
            {"start": "sql_generate", "end": "sql_execute"},
            {"start": "integration_task_generate", "end": "integration_task_deploy"},
            {"start": "sql_execute", "end": "artifact_generate"},
            {"start": "integration_task_deploy", "end": "artifact_generate"}
        ],
        node_configs={
            "table_check": {
                "table_check_rules": ["not_null", "data_type", "primary_key", "unique_constraint"]
            },
            "integration_task_generate": {
                "integration_parallelism": 2
            },
            "llm": {
                "llm_model": "gpt-4"
            }
        }
    )
    workflow_manager.register_workflow(default_config)
    
    # 启动服务器 - 禁用自动重载器以确保断点可以正常工作
    app.run(debug=True, port=5000, use_reloader=False)