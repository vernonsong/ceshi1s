# 欲买桂花同载酒，
# 终不似、少年游。
# Copyright (c) VernonSong. All rights reserved.
# ======================================================================================================================
import datetime
from typing import List, Optional, Any, Dict, Callable, Union, Tuple

from pydantic import BaseModel



# 节点参数模型
class NodeParameter(BaseModel):
    name: str
    description: str
    data_type: str
    required: bool = False
    default_value: Optional[Any] = None


# 节点输入参数模型
class NodeInputParameter(NodeParameter):
    pass


# 节点输出参数模型
class NodeOutputParameter(NodeParameter):
    pass


class NodeMetadata(BaseModel):
    name: str
    description: str
    inputs: List[NodeInputParameter]
    outputs: List[NodeOutputParameter]
    version: str = "1.0.0"
    
    class Config:
        extra = "allow"


# 节点注册表
node_registry: Dict[str, Dict[str, Union[NodeMetadata, Callable]]] = {}


# 工作流配置模型
class WorkflowConfig(BaseModel):
    name: str
    description: str
    nodes: List[str]
    edges: List[Dict[str, Any]]
    node_configs: Dict[str, Any]
    orchestration_type: str = "manual"  # manual, ai
    acceptance_criteria: Optional[List[str]] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    created_at: datetime.datetime = datetime.datetime.now()
    updated_at: datetime.datetime = datetime.datetime.now()


# 工作流状态模型
class WorkflowState(BaseModel):
    request_id: str
    workflow_config: WorkflowConfig
    source_data: Dict[str, Any]
    current_node: str
    results: Dict[str, Any] = {}
    errors: List[str] = []
    status: str = "running"
    custom_params: Optional[Dict[str, Any]] = {}


# 入湖请求模型
class LakeIngestionRequest(BaseModel):
    workflow_name: str
    source_data: Dict[str, Any]
    custom_params: Optional[Dict[str, Any]] = {}


def register_node(func: Optional[Callable] = None, *, name: str = None, description: str = "", inputs: List[NodeInputParameter] = None, outputs: List[NodeOutputParameter] = None, version: str = "1.0.0", **kwargs):
    """
    节点注册装饰器，用于注册节点并存储元数据
    
    支持两种使用方式：
    1. @register_node(metadata=NodeMetadata(...)) - 保持原有方式
    2. @register_node(name="", description="", inputs=[], outputs=[]) - 直接通过参数指定元数据
    3. @register_node - 从函数注解中提取元数据（推荐）
    
    Args:
        func: 节点处理函数（用于无参数装饰器）
        name: 节点名称
        description: 节点描述
        inputs: 输入参数列表
        outputs: 输出参数列表
        version: 节点版本
        **kwargs: 其他元数据参数
        
    Returns:
        装饰器函数或装饰后的函数
    """
    # 处理有参数的情况
    def decorator(fn: Callable):
        # 从函数注解中提取元数据
        node_name = name or fn.__name__
        node_description = description or fn.__doc__ or ""
        
        # 如果没有提供inputs和outputs，尝试从函数签名中提取
        node_inputs = inputs
        node_outputs = outputs
        
        # 创建元数据对象
        metadata = NodeMetadata(
            name=node_name,
            description=node_description.strip() if node_description else "",
            inputs=node_inputs or [],
            outputs=node_outputs or [],
            version=version,
            **kwargs
        )
        
        # 注册节点元数据和函数
        node_registry[metadata.name] = {
            "metadata": metadata,
            "function": fn
        }
        return fn
    
    # 处理无参数装饰器的情况
    if func is None:
        return decorator
    # 处理原有方式兼容
    elif isinstance(func, NodeMetadata):
        metadata = func
        return lambda fn: register_node(fn, name=metadata.name, description=metadata.description, 
                                        inputs=metadata.inputs, outputs=metadata.outputs, 
                                        version=metadata.version, **metadata.dict(exclude={"name", "description", "inputs", "outputs", "version"}))
    # 处理直接装饰函数的情况
    else:
        return decorator(func)