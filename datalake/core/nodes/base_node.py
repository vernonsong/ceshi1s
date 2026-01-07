# 欲买桂花同载酒，
# 终不似、少年游。
# Copyright (c) VernonSong. All rights reserved.
# ======================================================================================================================
from typing import Dict, Any, List, Type
from datalake.core.workflow.models import NodeMetadata, NodeInputParameter, NodeOutputParameter


class BaseNode:
    """节点基类，定义节点的基本接口"""
    metadata: NodeMetadata = None
    node_name: str = None
    
    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """节点执行入口，子类需要实现此方法"""
        raise NotImplementedError("Subclasses must implement the __call__ method")