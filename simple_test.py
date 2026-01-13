#!/usr/bin/env python3
"""
简单测试集成任务节点功能
"""

import json
import random
import time

def test_integration_task_generate():
    """测试集成任务生成逻辑"""
    print("\n=== 测试集成任务生成逻辑 ===")
    
    # 模拟输入参数
    source_db = "mysql_db"
    source_schema = "test_schema"
    source_table = "test_table"
    target_db = "hive_db"
    target_schema = "target_schema"
    target_table = "target_table"
    field_mapping = [
        {"source": "id", "target": "id"},
        {"source": "name", "target": "name"},
        {"source": "age", "target": "age"},
        {"source": "create_time", "target": "create_time"}
    ]
    username = "test_user"
    integration_type = "incremental"
    parallelism = 2
    audit_template_name = "custom_audit"
    
    # 生成任务信息
    task_name = f"{source_db}_{source_schema}_{source_table}_to_{target_db}_{target_schema}_{target_table}_{integration_type}"
    task_description = f"从{source_db}.{source_schema}.{source_table}到{target_db}.{target_schema}.{target_table}的{integration_type}集成任务"
    task_directory = f"/integration_tasks/{source_db}/{source_schema}/{source_table}/{target_db}/{target_schema}/{target_table}"
    
    # 生成上游API JSON
    upstream_api_json = {
        "task_info": {
            "name": task_name,
            "description": task_description,
            "type": integration_type,
            "create_by": username,
            "create_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "update_time": time.strftime("%Y-%m-%d %H:%M:%S")
        },
        "source": {
            "db": source_db,
            "schema": source_schema,
            "table": source_table,
            "type": "mysql"
        },
        "target": {
            "db": target_db,
            "schema": target_schema,
            "table": target_table,
            "type": "hive"
        },
        "field_mapping": field_mapping,
        "advanced_settings": {
            "parallelism": parallelism,
            "retry_count": 3,
            "timeout": 3600,
            "audit_template": audit_template_name
        },
        "schedule": {
            "type": "cron",
            "expression": "0 0 * * *",
            "start_time": "2024-01-01 00:00:00"
        },
        "tags": [
            f"source:{source_db}",
            f"target:{target_db}",
            f"type:{integration_type}"
        ]
    }
    
    print(f"任务名称: {task_name}")
    print(f"任务描述: {task_description}")
    print(f"任务目录: {task_directory}")
    print("\n上游API JSON:")
    print(json.dumps(upstream_api_json, indent=2, ensure_ascii=False))
    
    return upstream_api_json

def test_integration_task_deploy(upstream_api_json):
    """测试集成任务部署逻辑"""
    print("\n=== 测试集成任务部署逻辑 ===")
    
    print(f"部署任务: {upstream_api_json.get('task_info', {}).get('name')}")
    
    # 模拟部署结果
    is_success = random.choice([True] * 19 + [False])
    
    if is_success:
        status = "success"
        jobid = f"JOB-{random.randint(10000000, 99999999)}"
        deploy_message = f"Task deployed successfully, Job ID: {jobid}"
    else:
        status = "failed"
        jobid = ""
        deploy_message = f"Task deployment failed: {random.choice(['API connection error', 'Invalid JSON format', 'Permission denied', 'Server error'])}"
    
    print(f"状态: {status}")
    print(f"Job ID: {jobid}")
    print(f"消息: {deploy_message}")
    
    return status, jobid, deploy_message

if __name__ == "__main__":
    print("开始测试集成任务节点功能...")
    
    # 测试生成节点
    upstream_api_json = test_integration_task_generate()
    
    # 测试部署节点
    status, jobid, deploy_message = test_integration_task_deploy(upstream_api_json)
    
    print("\n=== 测试完成 ===")
    print(f"最终结果: {status}")
    if status == "success":
        print(f"生成的Job ID: {jobid}")