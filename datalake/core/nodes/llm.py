from typing import Dict, Any
from datalake.core.workflow.models import NodeInputParameter, NodeOutputParameter, register_node

# LLM节点 - 使用新的装饰器语法直接注册元数据
@register_node(
    name="llm",
    description="大语言模型节点，调用大语言模型进行文本生成",
    type="task",
    inputs=[
        NodeInputParameter(
            name="prompt",
            description="提示词",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="model_name",
            description="模型名称",
            data_type="string",
            required=False,  # 可选参数，使用默认模型
            default="gpt-4"
        )
    ],
    outputs=[
        NodeOutputParameter(
            name="response",
            description="模型响应内容",
            data_type="string"
        ),
        NodeOutputParameter(
            name="tokens_used",
            description="使用的tokens数量",
            data_type="integer"
        ),
        NodeOutputParameter(
            name="model_info",
            description="模型信息",
            data_type="dict"
        )
    ],
    category="ai"
)
def llm_node(state: dict) -> Dict[str, Any]:
    print(f"Executing LLM Node for request: {state.get('request_id')}")
    
    # 从状态中获取输入参数
    results = state.get("results", {})
    
    # 查找包含prompt的节点结果
    prompt = None
    for node_name, node_result in results.items():
        if isinstance(node_result, dict) and "prompt" in node_result:
            prompt = node_result["prompt"]
            break
    
    # 如果没有找到prompt，使用默认提示词
    if not prompt:
        prompt = "Hello, how are you?"
    
    # 获取模型配置
    workflow_config = state.get("workflow_config", {})
    if hasattr(workflow_config, "node_configs"):
        node_config = workflow_config.node_configs.get("llm", {})
    else:
        node_config = workflow_config.get("node_configs", {}).get("llm", {})
    
    model_name = node_config.get("model_name", "gpt-4")
    
    # 模拟LLM调用
    response = f"This is a mock response from {model_name} for prompt: '{prompt}'"
    tokens_used = len(prompt) + len(response)
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "llm": {
                "status": "success",
                "response": response,
                "tokens_used": tokens_used,
                "model_info": {
                    "name": model_name,
                    "type": "llm"
                }
            }
        },
        "current_node": "llm"
    }