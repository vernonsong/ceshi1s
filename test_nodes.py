#!/usr/bin/env python3
"""
测试节点注册机制的脚本
"""

from datalake.workflow.models import node_registry
from datalake.nodes import *

print("=== 测试节点注册机制 ===")

# 打印所有注册的节点
print("\n1. 所有注册的节点:")
for node_name in node_registry.keys():
    print(f"   - {node_name}")

# 打印节点元数据
print("\n2. 节点元数据:")
for node_name, entry in node_registry.items():
    metadata = entry['metadata']
    node_func = entry['function']
    print(f"\n   节点名称: {node_name}")
    print(f"   描述: {metadata.description}")
    print(f"   输入参数: {[param.name for param in metadata.inputs]}")
    print(f"   输出参数: {[param.name for param in metadata.outputs]}")

# 测试执行一个节点
print("\n3. 测试执行页面提单节点:")
test_state = {
    "request_id": "test-123",
    "source_data": {
        "user_input": "测试用户输入",
        "username": "test_user"
    }
}

# 获取节点函数
page_submit_func = node_registry["page_submit"]["function"]
result = page_submit_func(test_state)
print(f"   执行结果: {result['results']['page_submit']['status']}")
print(f"   源数据库: {result['results']['page_submit']['source_db']}")
print(f"   源表: {result['results']['page_submit']['source_table']}")

print("\n=== 测试完成 ===")