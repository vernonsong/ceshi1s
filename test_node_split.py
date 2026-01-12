# 测试节点拆分后的功能
import sys
import os

# 将项目根目录添加到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from datalake.workflow.models import node_registry
from datalake.nodes import *


def test_node_registration():
    """测试节点是否正确注册"""
    print("测试节点注册情况:")
    print("=" * 50)
    
    # 打印所有注册的节点
    for node_name, node_info in node_registry.items():
        metadata = node_info["metadata"]
        function = node_info["function"]
        
        print(f"节点名称: {node_name}")
        print(f"节点类型: {metadata.type}")
        print(f"节点描述: {metadata.description}")
        print(f"节点分类: {metadata.category}")
        print(f"输入参数: {[input_param.name for input_param in metadata.inputs]}")
        print(f"输出参数: {[output_param.name for output_param in metadata.outputs]}")
        print(f"节点函数: {function.__name__}")
        print("-" * 50)
    
    print(f"\n总注册节点数: {len(node_registry)}")


def test_node_import():
    """测试节点是否可以正确导入"""
    print("\n测试节点导入情况:")
    print("=" * 50)
    
    # 测试导入的节点是否可以访问
    test_nodes = [
        page_submit_node,
        table_check_node,
        llm_node,
        sql_generate_node,
        sql_execute_node,
        integration_task_generate_node,
        integration_task_deploy_node,
        artifact_generate_node,
        wait_gateway_node,
        parallel_gateway,
        exclusive_gateway
    ]
    
    for node in test_nodes:
        print(f"节点函数: {node.__name__} 导入成功")
    
    print(f"\n总导入节点数: {len(test_nodes)}")


if __name__ == "__main__":
    test_node_registration()
    test_node_import()
    print("\n所有测试完成!")