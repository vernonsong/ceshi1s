#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工作流编排智能体测试脚本
"""

import json
from datalake.agents import get_workflow_agent
from datalake.core.workflow.workflow_orchestrator import create_workflow_from_json, execute_workflow_with_params

def test_workflow_agent():
    """测试工作流编排智能体"""
    print("测试工作流编排智能体开始...")
    
    # 获取工作流编排智能体实例
    agent = get_workflow_agent()
    
    # 定义用户需求
    user_requirement = "请创建一个工作流，用于从MySQL数据库的test_schema.test_table表中获取表结构，生成Hive湖表的建表DDL，并执行该DDL创建表。"
    print(f"用户需求：\n{user_requirement}")
    
    try:
        # 根据用户需求生成流程图JSON
        print("\n生成流程图JSON...")
        workflow_json = agent.generate_workflow_json(user_requirement)
        
        # 打印生成的流程图JSON
        print("\n生成的流程图JSON：")
        print(json.dumps(workflow_json, ensure_ascii=False, indent=2))
        
        # 验证流程图JSON的有效性
        print("\n验证流程图JSON的有效性...")
        is_valid = agent.validate_workflow_json(workflow_json)
        if is_valid:
            print("流程图JSON验证通过！")
        else:
            print("流程图JSON验证失败！")
            return False
        
        # 根据生成的流程图JSON创建工作流
        print("\n根据流程图JSON创建工作流...")
        workflow = create_workflow_from_json(json.dumps(workflow_json))
        
        # 定义输入参数
        input_params = {
            "request_id": "test_request_001",
            "workflow_config": {},
            "source_data": {
                "source_db": "mysql",
                "source_schema": "test_schema",
                "source_table": "test_table",
                "lake_db_type": "hive",
                "lake_schema": "test_schema",
                "lake_table": "target_table"
            }
        }
        
        # 执行工作流
        print("\n执行工作流...")
        result = execute_workflow_with_params(workflow, input_params)
        
        # 打印结果
        print("\n工作流执行结果：")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
        print("\n测试成功！")
        return True
        
    except Exception as e:
        print(f"\n测试失败！错误信息：{e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_workflow_agent()