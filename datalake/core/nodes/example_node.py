from typing import Dict, Any
from datalake.core.workflow.models import NodeInputParameter, NodeOutputParameter, register_node

# 示例节点 - 使用注解方式注册元数据
@register_node(
    name="example",
    description="示例节点，演示如何使用注解方式注册元数据",
    inputs=[
        NodeInputParameter(
            name="input_text",
            description="输入文本",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="multiplier",
            description="乘数",
            data_type="integer",
            required=False,
            default_value=1
        )
    ],
    outputs=[
        NodeOutputParameter(
            name="output_text",
            description="输出文本",
            data_type="string"
        ),
        NodeOutputParameter(
            name="processed_data",
            description="处理后的数据",
            data_type="dict"
        )
    ],
    category="utility"
)
def example_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    示例节点实现函数
    
    这个函数演示了如何使用新的装饰器方式注册节点元数据。
    装饰器可以直接接受元数据参数，无需先创建NodeMetadata对象。
    
    Args:
        state: 工作流状态字典
        
    Returns:
        处理后的结果字典
    """
    print(f"Executing Example Node for request: {state.get('request_id')}")
    
    # 从状态中获取输入参数
    results = state.get("results", {})
    
    # 获取输入参数
    input_text = results.get("input_text", "Default Text")
    multiplier = results.get("multiplier", 1)
    
    # 处理数据
    output_text = input_text * multiplier
    processed_data = {
        "original": input_text,
        "multiplier": multiplier,
        "result": output_text
    }
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "example": {
                "status": "success",
                "output_text": output_text,
                "processed_data": processed_data
            }
        },
        "current_node": "example"
    }