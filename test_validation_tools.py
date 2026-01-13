#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试验证工具
"""

from datalake.services.validation_tools import delete_table, delete_integration_task, query_integration_task, get_table_ddl
import json

def test_tools():
    """
    测试所有验证工具
    """
    print("=== 测试验证工具 ===\n")
    
    # 1. 测试获取表DDL
    print("1. 测试获取表DDL：")
    result = get_table_ddl("default", "test_table1")
    print(f"结果：{json.dumps(result, ensure_ascii=False, indent=2)}")
    
    # 2. 测试查询集成任务
    print("\n2. 测试查询集成任务：")
    result = query_integration_task()
    print(f"结果：{json.dumps(result, ensure_ascii=False, indent=2)}")
    
    # 3. 测试根据任务ID查询集成任务
    print("\n3. 测试根据任务ID查询集成任务：")
    result = query_integration_task(task_id="task_123")
    print(f"结果：{json.dumps(result, ensure_ascii=False, indent=2)}")
    
    # 4. 测试根据状态查询集成任务
    print("\n4. 测试根据状态查询集成任务：")
    result = query_integration_task(status="running")
    print(f"结果：{json.dumps(result, ensure_ascii=False, indent=2)}")
    
    # 5. 测试删除表
    print("\n5. 测试删除表：")
    result = delete_table("default", "test_table2")
    print(f"结果：{json.dumps(result, ensure_ascii=False, indent=2)}")
    
    # 6. 测试删除不存在的表
    print("\n6. 测试删除不存在的表：")
    result = delete_table("default", "non_existent_table")
    print(f"结果：{json.dumps(result, ensure_ascii=False, indent=2)}")
    
    # 7. 测试删除集成任务
    print("\n7. 测试删除集成任务：")
    result = delete_integration_task("task_456")
    print(f"结果：{json.dumps(result, ensure_ascii=False, indent=2)}")
    
    # 8. 测试删除不存在的集成任务
    print("\n8. 测试删除不存在的集成任务：")
    result = delete_integration_task("non_existent_task")
    print(f"结果：{json.dumps(result, ensure_ascii=False, indent=2)}")
    
    # 9. 验证删除后的状态
    print("\n9. 验证删除后的状态：")
    print("查询所有表：")
    tables = get_table_ddl("default", "test_table2")
    print(f"test_table2是否存在：{tables['success']}")
    
    print("查询所有集成任务：")
    tasks = query_integration_task()
    print(f"剩余任务数量：{tasks['count']}")
    for task in tasks['tasks']:
        print(f"  - {task['task_id']}: {task['status']}")

if __name__ == "__main__":
    test_tools()