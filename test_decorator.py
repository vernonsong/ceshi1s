#!/usr/bin/env python3
# 测试新的register_node装饰器功能
from datalake.core.workflow.models import node_registry, register_node, NodeInputParameter, NodeOutputParameter
from datalake.core.nodes import NODE_MAPPING

# 测试1：使用参数直接注册元数据
@register_node(
    name="test_node1",
    description="测试节点1，使用参数直接注册",
    inputs=[
        NodeInputParameter(name="param1", description="参数1", data_type="string", required=True)
    ],
    outputs=[
        NodeOutputParameter(name="result1", description="结果1", data_type="string")
    ]
)
def test_node1(state):
    return state

# 测试2：使用默认名称（函数名）注册
@register_node(
    description="测试节点2，使用默认名称",
    inputs=[
        NodeInputParameter(name="param2", description="参数2", data_type="integer", required=True)
    ],
    outputs=[
        NodeOutputParameter(name="result2", description="结果2", data_type="integer")
    ]
)
def test_node2(state):
    return state

# 测试3：从函数文档字符串提取描述
@register_node(
    inputs=[
        NodeInputParameter(name="param3", description="参数3", data_type="string", required=True)
    ],
    outputs=[
        NodeOutputParameter(name="result3", description="结果3", data_type="string")
    ]
)
def test_node3(state):
    """
    测试节点3，从函数文档字符串提取描述
    这是一个多行文档字符串的示例。
    """
    return state

# 主测试函数
if __name__ == "__main__":
    print("=== 测试节点注册装饰器 ===")
    
    # 打印所有注册的节点
    print("\n1. 所有注册的节点:")
    for node_name, node_info in node_registry.items():
        print(f"   - {node_name}")
    
    # 测试节点1的元数据
    print("\n2. 测试节点1元数据:")
    node1 = node_registry.get("test_node1")
    if node1:
        metadata1 = node1["metadata"]
        print(f"   名称: {metadata1.name}")
        print(f"   描述: {metadata1.description}")
        print(f"   输入参数: {[p.name for p in metadata1.inputs]}")
        print(f"   输出参数: {[p.name for p in metadata1.outputs]}")
    
    # 测试节点2的元数据（使用默认名称）
    print("\n3. 测试节点2元数据:")
    node2 = node_registry.get("test_node2")
    if node2:
        metadata2 = node2["metadata"]
        print(f"   名称: {metadata2.name}")
        print(f"   描述: {metadata2.description}")
    
    # 测试节点3的元数据（从文档字符串提取描述）
    print("\n4. 测试节点3元数据:")
    node3 = node_registry.get("test_node3")
    if node3:
        metadata3 = node3["metadata"]
        print(f"   名称: {metadata3.name}")
        print(f"   描述: {metadata3.description}")
    
    # 测试原有节点是否仍然正常注册
    print("\n5. 原有节点注册情况:")
    for node_name in ["llm", "example"]:
        if node_name in node_registry:
            print(f"   ✓ {node_name} 已注册")
        else:
            print(f"   ✗ {node_name} 未注册")
    
    # 测试NODE_MAPPING是否包含新节点
    print("\n6. NODE_MAPPING包含的节点:")
    for node_name in NODE_MAPPING.keys():
        print(f"   - {node_name}")
    
    print("\n=== 测试完成 ===")