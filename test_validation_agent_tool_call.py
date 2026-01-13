#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试validation_agent的工具调用功能
"""

from datalake.core.agents.validation_agent import get_validation_agent
import json

def test_validation_agent_tool_call():
    """
    测试validation_agent的工具调用功能
    """
    print("=== 测试validation_agent的工具调用功能 ===\n")
    
    # 获取验证智能体实例
    agent = get_validation_agent()
    
    # 模拟工作流JSON
    workflow_json = {
        "nodes": [
            {
                "id": "table_check",
                "type": "table_check",
                "name": "检查源表"
            }
        ],
        "edges": [],
        "start_node": "table_check",
        "end_nodes": ["table_check"]
    }
    
    # 验证要求 - 更明确的验证任务描述
    validation_requirements = '''你是一个专业的工作流验证工程师，只需要验证工作流是否满足以下要求：
    1. 工作流必须包含table_check节点
    2. 必须使用get_table_ddl工具验证default.test_table1表是否存在
    请严格按照要求进行验证，不要进行任何与验证无关的操作或分析。
    验证完成后，请按照以下格式输出结论：
    ```
    结论：
    验证结果：成功/失败
    详细信息：
    1. 节点验证：包含/缺少table_check节点
    2. 表验证：default.test_table1存在/不存在（使用get_table_ddl工具验证）
    ```'''
    
    try:
        # 执行验证
        result = agent.validate_workflow(workflow_json, validation_requirements)
        
        print("验证结果：")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
        return result
    except Exception as e:
        print(f"验证过程中发生错误：{e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    test_validation_agent_tool_call()