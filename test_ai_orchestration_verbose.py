import requests
import json
import time

# API地址
BASE_URL = "http://127.0.0.1:5000"


def test_ai_orchestration_verbose():
    """详细测试AI编排，查看大模型输出"""
    print("=== 详细测试AI编排 ===")
    
    # 1. 发送AI编排请求
    print("\n1. 发送AI编排请求...")
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
    
    # 2. 打印AI生成的工作流配置
    print("\n2. AI生成的工作流配置:")
    final_config = result['final_workflow_config']
    print(f"工作流名称: {final_config['name']}")
    print(f"工作流描述: {final_config['description']}")
    print(f"节点: {final_config['nodes']}")
    print(f"边: {final_config['edges']}")
    print(f"节点配置: {final_config['node_configs']}")
    
    # 3. 打印大模型完整响应
    if 'llm_response' in final_config:
        print("\n=== 大模型完整响应开始 ===")
        print(f"{final_config['llm_response']}")
        print("=== 大模型完整响应结束 ===")
    
    # 3. 打印迭代历史
    print("\n3. AI编排迭代历史:")
    iterations = result['iterations']
    for i, iteration in enumerate(iterations):
        print(f"\n迭代 {i+1}:")
        print(f"  反馈: {iteration['feedback']}")
        print(f"  验证结果状态: {iteration['validation_result']['status']}")
        if 'llm_analysis' in iteration['validation_result']:
            print(f"  大模型分析: {iteration['validation_result']['llm_analysis']['feedback']}")
            if 'improvement_suggestions' in iteration['validation_result']['llm_analysis']:
                print(f"  改进建议: {iteration['validation_result']['llm_analysis']['improvement_suggestions']}")
    
    # 4. 验证AI生成的工作流
    print("\n4. 验证AI生成的工作流...")
    ai_workflow_name = final_config['name']
    
    # 检查工作流是否已注册
    response = requests.get(f"{BASE_URL}/api/workflows")
    workflows = response.json()
    ai_workflow_exists = any(wf['name'] == ai_workflow_name for wf in workflows)
    print(f"AI生成的工作流注册成功: {ai_workflow_exists}")
    
    return ai_workflow_exists, ai_workflow_name


if __name__ == "__main__":
    test_ai_orchestration_verbose()
