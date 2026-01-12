#!/usr/bin/env python3
# 测试数据处理节点功能
from datalake.core.workflow.models import node_registry
from datalake.core.nodes import NODE_MAPPING, InputData, ProcessingConfig, DataSourceConfig
from datalake.core.nodes.data_processing import data_processing_node

# 测试1：检查节点是否正确注册
print("=== 测试数据处理节点注册 ===")
if "data_processing" in node_registry:
    print("✓ 数据处理节点已成功注册")
    
    # 查看节点元数据
    node_info = node_registry["data_processing"]
    metadata = node_info["metadata"]
    print(f"节点名称: {metadata.name}")
    print(f"节点描述: {metadata.description}")
    print(f"节点输入: {[inp.name for inp in metadata.inputs]}")
    print(f"节点输出: {[out.name for out in metadata.outputs]}")
    
    # 检查是否包含JSON Schema
    if hasattr(metadata, "input_schema"):
        print("✓ 输入参数包含JSON Schema")
    if hasattr(metadata, "output_schema"):
        print("✓ 输出参数包含JSON Schema")
else:
    print("✗ 数据处理节点未注册")

# 测试2：检查节点映射
print("\n=== 测试节点映射 ===")
if "data_processing" in NODE_MAPPING:
    print("✓ 数据处理节点在NODE_MAPPING中")
    print(f"映射函数: {NODE_MAPPING['data_processing'].__name__}")
else:
    print("✗ 数据处理节点不在NODE_MAPPING中")

# 测试3：测试Pydantic模型功能
print("\n=== 测试Pydantic模型功能 ===")
try:
    # 创建测试配置
    source_config = DataSourceConfig(
        type="database",
        connection_string="postgresql://test:pass@localhost:5432/testdb",
        query="SELECT * FROM test_data WHERE status = 'active'",
        batch_size=500
    )
    
    processing_config = ProcessingConfig(
        algorithm="feature_scaling",
        parameters={"method": "z-score", "mean": 0, "std": 1},
        parallelism=4
    )
    
    # 创建输入数据
    input_data = InputData(
        source_config=source_config,
        processing_config=processing_config,
        target_columns=["value1", "value2", "value3", "value4"]
    )
    
    print("✓ 成功创建输入数据模型")
    print(f"输入数据JSON: {input_data.model_dump_json(indent=2)}")
    
except Exception as e:
    print(f"✗ 创建模型时出错: {e}")

# 测试4：测试节点执行
print("\n=== 测试节点执行 ===")
try:
    # 创建测试状态
    test_state = {
        "request_id": "test-123",
        "results": {
            "input_data": input_data.model_dump(),
            "timeout": 120
        }
    }
    
    # 执行节点
    result = data_processing_node(test_state)
    
    print("✓ 节点执行成功")
    print(f"处理状态: {result['results']['data_processing']['status']}")
    print(f"处理结果: {result['results']['data_processing']['processing_result']}")
    
except Exception as e:
    print(f"✗ 节点执行出错: {e}")

print("\n=== 测试完成 ===")