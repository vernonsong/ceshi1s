#!/usr/bin/env python3
"""
测试integration_task_generate和integration_task_deploy节点
"""

import os
import sys
import json

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datalake.core.nodes.integration_task_generate import integration_task_generate_node
from datalake.core.nodes.integration_task_deploy import integration_task_deploy_node

def test_integration_task_generate():
    """测试集成任务生成节点"""
    print("\n=== 测试集成任务生成节点 ===")
    
    # 准备输入数据
    state = {
        "request_id": "test-123456",
        "source_data": {
            "source_db": "mysql_db",
            "source_schema": "test_schema",
            "source_table": "test_table",
            "target_db": "hive_db",
            "target_schema": "target_schema",
            "target_table": "target_table",
            "field_mapping": [
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
                {"source": "age", "target": "age"},
                {"source": "create_time", "target": "create_time"}
            ],
            "username": "test_user",
            "integration_type": "incremental",
            "parallelism": 2,
            "audit_template_name": "custom_audit"
        },
        "results": {}
    }
    
    # 调用节点函数
    result = integration_task_generate_node(state)
    
    # 打印结果
    print("\n节点执行结果:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    # 获取生成的上游API JSON
    upstream_api_json = result.get("results", {}).get("integration_task_generate", {}).get("upstream_api_json")
    if upstream_api_json:
        print("\n生成的上游API JSON:")
        print(json.dumps(upstream_api_json, indent=2, ensure_ascii=False))
    
    return upstream_api_json

def test_integration_task_deploy(upstream_api_json):
    """测试集成任务部署节点"""
    print("\n=== 测试集成任务部署节点 ===")
    
    # 准备输入数据
    state = {
        "request_id": "test-123456",
        "source_data": {
            "upstream_api_json": upstream_api_json
        },
        "results": {
            "integration_task_generate": {
                "upstream_api_json": upstream_api_json
            }
        }
    }
    
    # 调用节点函数
    result = integration_task_deploy_node(state)
    
    # 打印结果
    print("\n节点执行结果:")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    # 获取jobid
    jobid = result.get("results", {}).get("integration_task_deploy", {}).get("jobid")
    status = result.get("results", {}).get("integration_task_deploy", {}).get("status")
    
    print(f"\n部署状态: {status}")
    print(f"Job ID: {jobid}")
    
    return jobid

if __name__ == "__main__":
    print("开始测试集成任务节点...")
    
    # 测试第一个节点
    upstream_api_json = test_integration_task_generate()
    
    if upstream_api_json:
        # 测试第二个节点
        jobid = test_integration_task_deploy(upstream_api_json)
        
        if jobid:
            print("\n✅ 测试成功！")
            print(f"生成的Job ID: {jobid}")
        else:
            print("\n❌ 测试失败：未生成Job ID")
    else:
        print("\n❌ 测试失败：未生成上游API JSON")