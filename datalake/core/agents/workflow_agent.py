#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工作流编排智能体
根据用户的自然语言需求生成流程图JSON
"""

from typing import Dict, Any, List
import json
from langchain_openai import ChatOpenAI
from langchain.messages import HumanMessage
from datalake.core.workflow.models import NodeMetadata
from datalake.core.nodes import NODE_MAPPING

class WorkflowAgent:
    """工作流编排智能体"""
    
    def __init__(self, model_name: str = "qwen-plus", temperature: float = 0.1):
        """
        初始化工作流编排智能体
        
        Args:
            model_name: 使用的大模型名称
            temperature: 大模型的温度参数，控制生成结果的随机性
        """
        self.llm = ChatOpenAI(
            model=model_name,
            temperature=temperature,
            # 配置API密钥和基础URL，这些应该从环境变量或配置文件中获取
            api_key="",
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
        )
        
        # 获取支持的节点类型信息
        self.supported_nodes = self._get_supported_nodes_info()
    
    def _get_supported_nodes_info(self) -> Dict[str, Any]:
        """
        获取支持的节点类型信息
        
        Returns:
            节点类型信息字典，包含节点类型、名称、描述、输入输出参数等
        """
        nodes_info = {}
        
        from datalake.core.workflow.models import node_registry
        
        for node_type in NODE_MAPPING.keys():
            # 从节点注册表中获取节点元数据
            if node_type in node_registry:
                registry_entry = node_registry[node_type]
                metadata = registry_entry.get("metadata")
                if isinstance(metadata, NodeMetadata):
                    # 获取节点类型的额外属性
                    type_attrs = metadata.__dict__ if hasattr(metadata, "__dict__") else {}
                    
                    nodes_info[node_type] = {
                        "name": metadata.name,
                        "description": metadata.description,
                        "type": type_attrs.get("type", "custom"),
                        "inputs": [
                            {
                                "name": input_param.name,
                                "description": input_param.description,
                                "data_type": getattr(input_param, "data_type", "string"),
                                "required": getattr(input_param, "required", False)
                            }
                            for input_param in metadata.inputs
                        ],
                        "outputs": [
                            {
                                "name": output_param.name,
                                "description": output_param.description,
                                "data_type": getattr(output_param, "data_type", "string")
                            }
                            for output_param in metadata.outputs
                        ],
                        "category": type_attrs.get("category", "default")
                    }
        
        return nodes_info
    
    def generate_workflow_json(self, user_requirement: str) -> Dict[str, Any]:
        """
        根据用户需求生成流程图JSON
        
        Args:
            user_requirement: 用户的自然语言需求
            
        Returns:
            流程图JSON字典
        """
        # 构建提示词模板
        prompt_template = self._build_prompt_template(user_requirement)
        
        # 调用大模型生成流程图JSON
        messages = [HumanMessage(content=prompt_template)]
        response = self.llm.invoke(messages)
        
        # 解析大模型的响应
        response_content = response.content
        
        # 移除可能的代码块标记
        if response_content.startswith("```json"):
            response_content = response_content[7:]
        if response_content.endswith("```"):
            response_content = response_content[:-3]
        
        # 解析JSON
        workflow_json = json.loads(response_content.strip())
        
        return workflow_json
    
    def _build_prompt_template(self, user_requirement: str) -> str:
        """
        构建提示词模板
        
        Args:
            user_requirement: 用户的自然语言需求
            
        Returns:
            完整的提示词
        """
        supported_nodes_json = json.dumps(self.supported_nodes, ensure_ascii=False, indent=2)
        
        prompt = f"""
        你是一位专业的工作流编排工程师，请根据用户的需求和提供的支持节点信息，生成一个符合要求的流程图JSON。
        
        用户需求：
        {user_requirement}
        
        支持的节点类型信息：
        {supported_nodes_json}
        
        流程图JSON的格式要求：
        {{
            "nodes": [
                {{
                    "id": "节点ID",
                    "type": "节点类型",
                    "name": "节点名称",
                    "inputs": [
                        {{
                            "name": "参数名",
                            "source_type": "raw_input|node_output|complex",
                            "input_key": "原始输入的键",
                            "node_id": "前置节点ID",
                            "output_field": "前置节点输出字段",
                            "transform_script": "Python转换脚本"
                        }}
                    ]
                }}
            ],
            "edges": [
                {{
                    "source": "源节点ID",
                    "target": "目标节点ID",
                    "condition": "条件表达式"
                }}
            ],
            "start_node": "起始节点ID",
            "end_nodes": ["结束节点ID"]
        }}
        
        注意事项：
        1. 请严格按照用户需求生成流程图，确保节点之间的依赖关系正确
        2. 请根据节点类型的输入输出参数要求，正确配置每个节点的输入参数
        3. 对于需要从原始输入获取的参数，使用source_type="raw_input"，并指定input_key
        4. 对于需要从前置节点获取的参数，使用source_type="node_output"，并指定node_id和output_field
        5. 对于需要复杂转换的参数，使用source_type="complex"，并编写Python转换脚本
        6. 请确保流程图JSON格式正确，没有语法错误
        7. 请确保所有节点ID唯一，并且边的source和target指向存在的节点ID
        8. 请为每个节点提供清晰的名称和描述
        9. 请指定正确的起始节点和结束节点
        10. 对于有条件的边，请提供条件表达式
        
        请直接返回流程图JSON，不要包含其他无关内容。
        """
        
        return prompt
    
    def validate_workflow_json(self, workflow_json: Dict[str, Any]) -> bool:
        """
        验证流程图JSON的有效性
        
        Args:
            workflow_json: 流程图JSON字典
            
        Returns:
            验证结果，True表示有效，False表示无效
        """
        # 检查必要的字段
        if "nodes" not in workflow_json or not isinstance(workflow_json["nodes"], list):
            return False
            
        if "edges" not in workflow_json or not isinstance(workflow_json["edges"], list):
            return False
            
        if "start_node" not in workflow_json:
            return False
            
        if "end_nodes" not in workflow_json or not isinstance(workflow_json["end_nodes"], list):
            return False
        
        # 检查节点ID的唯一性
        node_ids = []
        for node in workflow_json["nodes"]:
            if "id" not in node:
                return False
            if node["id"] in node_ids:
                return False
            node_ids.append(node["id"])
        
        # 检查边的source和target是否指向存在的节点ID
        for edge in workflow_json["edges"]:
            if "source" not in edge or "target" not in edge:
                return False
            if edge["source"] not in node_ids:
                return False
            if edge["target"] and edge["target"] not in node_ids:
                return False
        
        # 检查起始节点是否存在
        if workflow_json["start_node"] not in node_ids:
            return False
        
        # 检查结束节点是否存在
        for end_node in workflow_json["end_nodes"]:
            if end_node not in node_ids:
                return False
        
        return True
    
    def optimize_workflow_json(self, workflow_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        优化流程图JSON，提高执行效率
        
        Args:
            workflow_json: 流程图JSON字典
            
        Returns:
            优化后的流程图JSON字典
        """
        # 这里可以添加一些优化逻辑，例如：
        # 1. 合并可以并行执行的节点
        # 2. 移除冗余的节点
        # 3. 优化节点之间的依赖关系
        # 4. 优化输入输出参数的映射
        
        # 目前暂时返回原始的流程图JSON
        return workflow_json

# 创建全局的工作流编排智能体实例
workflow_agent = WorkflowAgent()

def get_workflow_agent() -> WorkflowAgent:
    """
    获取工作流编排智能体实例
    
    Returns:
        工作流编排智能体实例
    """
    return workflow_agent