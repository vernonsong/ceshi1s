# 数据处理节点示例
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from datalake.core.workflow.models import register_node, NodeOutputParameter

# 定义复杂的输入参数结构（嵌套类）
class ProcessingConfig(BaseModel):
    """数据处理配置"""
    algorithm: str = Field(..., description="处理算法名称", examples=["batch_normalization", "feature_scaling"])
    parameters: Dict[str, Any] = Field(default_factory=dict, description="算法参数", examples=[{"epsilon": 0.001, "momentum": 0.9}])
    parallelism: int = Field(default=1, description="并行处理数量", ge=1)

class DataSourceConfig(BaseModel):
    """数据源配置"""
    type: str = Field(..., description="数据源类型", examples=["database", "api", "file"])
    connection_string: str = Field(..., description="连接字符串", examples=["postgresql://user:pass@localhost:5432/db"])
    query: Optional[str] = Field(None, description="查询语句（针对数据库类型）", examples=["SELECT * FROM users WHERE active = true"])
    batch_size: int = Field(default=1000, description="批处理大小", ge=1)

class InputData(BaseModel):
    """节点输入数据模型"""
    source_config: DataSourceConfig = Field(..., description="数据源配置")
    processing_config: ProcessingConfig = Field(..., description="处理配置")
    target_columns: List[str] = Field(..., description="目标处理列", examples=[["age", "income", "score"]])

# 定义输出参数结构
class ProcessingResult(BaseModel):
    """处理结果"""
    processed_rows: int = Field(..., description="处理的行数")
    processing_time: float = Field(..., description="处理时间（秒）")
    metrics: Dict[str, float] = Field(..., description="处理指标", examples=[{"accuracy": 0.95, "precision": 0.92}])
    output_data: Dict[str, List[Any]] = Field(..., description="处理后的数据", examples=[{"age": [25, 30, 35], "income_scaled": [0.2, 0.5, 0.8]}])

@register_node(
    name="data_processing",
    description="复杂数据处理节点，支持多种数据源和处理算法",
    inputs=[
        {"name": "input_data", "description": "数据处理输入配置", "data_type": "InputData", "required": True},
        {"name": "timeout", "description": "处理超时时间（秒）", "data_type": "integer", "required": False, "default_value": 300}
    ],
    outputs=[
        NodeOutputParameter(
            name="processing_result",
            description="数据处理结果",
            data_type="ProcessingResult"
        ),
        NodeOutputParameter(
            name="status",
            description="处理状态",
            data_type="string",
            examples=["success", "failed", "timeout"]
        )
    ],
    category="data",
    # 提供JSON Schema示例，帮助智能体理解复杂结构
    input_schema=InputData.model_json_schema(),
    output_schema=ProcessingResult.model_json_schema()
)
def data_processing_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """数据处理节点实现
    
    这个节点演示了如何处理复杂的嵌套输入参数，并返回结构化的输出结果。
    它符合LangGraph节点的要求，接收状态对象并返回更新后的状态。
    
    Args:
        state: LangGraph状态字典，包含：
            - request_id: 请求ID
            - results: 之前节点的结果
            - input_data: 数据处理输入配置（来自之前节点或初始输入）
            - timeout: 处理超时时间（可选，默认300秒）
    
    Returns:
        更新后的状态字典，包含：
            - processing_result: 处理结果对象
            - status: 处理状态
    """
    print(f"Executing Data Processing Node for request: {state.get('request_id')}")
    
    # 从状态中获取输入参数
    results = state.get("results", {})
    
    # 查找包含input_data的节点结果或直接使用输入
    input_data = results.get("input_data")
    if not input_data:
        # 如果没有找到，使用默认配置（仅用于演示）
        input_data = InputData(
            source_config=DataSourceConfig(
                type="database",
                connection_string="postgresql://user:pass@localhost:5432/db",
                query="SELECT * FROM sample_data",
                batch_size=100
            ),
            processing_config=ProcessingConfig(
                algorithm="feature_scaling",
                parameters={"method": "min-max"},
                parallelism=2
            ),
            target_columns=["value1", "value2", "value3"]
        )
    
    # 获取超时时间
    timeout = results.get("timeout", 300)
    
    # 模拟数据处理（实际应用中会调用真实的处理逻辑或外部接口）
    print(f"Processing data with config: {input_data}")
    print(f"Timeout: {timeout} seconds")
    
    # 模拟处理结果
    processing_result = ProcessingResult(
        processed_rows=1234,
        processing_time=45.6,
        metrics={"mean_value": 123.45, "std_deviation": 23.45, "min_value": 0.0, "max_value": 255.0},
        output_data={
            "value1_processed": [1.2, 3.4, 5.6, 7.8],
            "value2_processed": [2.3, 4.5, 6.7, 8.9],
            "value3_processed": [3.4, 5.6, 7.8, 9.0]
        }
    )
    
    # 返回更新后的状态
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "data_processing": {
                "status": "success",
                "processing_result": processing_result.model_dump(),
                "processed_at": "2024-01-01T12:00:00Z"
            }
        },
        "current_node": "data_processing"
    }