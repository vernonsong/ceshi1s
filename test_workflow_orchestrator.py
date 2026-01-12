#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工作流编排器测试脚本
"""

import json
from datalake.core.workflow import create_workflow_from_json, get_workflow_json_example, execute_workflow_with_params

def test_workflow_orchestrator():
    """测试工作流编排器"""
    print("测试工作流编排器开始...")
    
    # 获取流程图JSON示例
    workflow_json = get_workflow_json_example()
    print(f"流程图JSON示例:\n{workflow_json}")
    
    try:
        # 从JSON创建工作流
        workflow = create_workflow_from_json(workflow_json)
        print("\n工作流创建成功！")
        
        # 定义输入参数
        input_params = {
            "request_id": "test_request_001",
            "workflow_config": {},
            "source_data": {
                "source_db": "mysql",
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
    test_workflow_orchestrator()