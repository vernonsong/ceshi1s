#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试验证智能体
"""

from datalake.core.agents.validation_agent import get_validation_agent
import json

# 示例工作流JSON
test_workflow = {
    "nodes": [
        {
            "id": "table_check",
            "type": "table_check",
            "name": "检查源表",
            "inputs": [
                {
                    "name": "database_name",
                    "source_type": "raw_input",
                    "input_key": "source_database"
                },
                {
                    "name": "table_name",
                    "source_type": "raw_input",
                    "input_key": "source_table"
                }
            ]
        },
        {
            "id": "integration_task_generate",
            "type": "integration_task_generate",
            "name": "生成集成任务",
            "inputs": [
                {
                    "name": "source_db",
                    "source_type": "raw_input",
                    "input_key": "source_database"
                },
                {
                    "name": "source_table",
                    "source_type": "raw_input",
                    "input_key": "source_table"
                },
                {
                    "name": "target_db",
                    "source_type": "raw_input",
                    "input_key": "target_database"
                },
                {
                    "name": "target_table",
                    "source_type": "raw_input",
                    "input_key": "target_table"
                }
            ]
        },
        {
            "id": "integration_task_deploy",
            "type": "integration_task_deploy",
            "name": "部署集成任务",
            "inputs": [
                {
                    "name": "upstream_api_json",
                    "source_type": "node_output",
                    "node_id": "integration_task_generate",
                    "output_field": "upstream_api_json"
                }
            ]
        }
    ],
    "edges": [
        {
            "source": "table_check",
            "target": "integration_task_generate"
        },
        {
            "source": "integration_task_generate",
            "target": "integration_task_deploy"
        }
    ],
    "start_node": "table_check",
    "end_nodes": ["integration_task_deploy"]
}

# 验证要求
test_validation_requirements = "验证工作流是否满足以下要求：1. 工作流包含了表检查节点；2. 工作流包含了集成任务生成和部署节点；3. 源表default.test_table1存在；4. 集成任务task_123处于running状态"

def test_validation_agent():
    """
    测试验证智能体
    """
    print("=== 测试验证智能体 ===")
    
    # 获取验证智能体
    validation_agent = get_validation_agent()
    
    # 验证工作流
    result = validation_agent.validate_workflow(test_workflow, test_validation_requirements)
    
    print("\n=== 验证结果 ===")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    
    return result

if __name__ == "__main__":
    test_validation_agent()