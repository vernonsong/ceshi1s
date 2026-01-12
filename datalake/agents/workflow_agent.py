#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工作流编排智能体
根据用户的自然语言需求生成流程图JSON
"""

from typing import Dict, Any, List
import json
import os
from langchain_community.chat_models import ChatOpenAI
from langchain_core.messages import HumanMessage
from dotenv import load_dotenv

# 导入节点注册表
from datalake.core.workflow.models import node_registry

# 加载环境变量
load_dotenv()

class WorkflowAgent:
    """工作流编排智能体"""
    
    def __init__(self, model_name: str = "qwen-plus", temperature: float = 0.1):
        """
        初始化工作流编排智能体
        
        Args:
            model_name: 使用的大模型名称
            temperature: 大模型的温度参数，控制生成结果的随机性
        """
        # 从环境变量获取API密钥
        api_key = os.getenv("ALIYUN_KEY")
        
        # 使用阿里云的大模型配置
        self.llm = ChatOpenAI(
            model_name=model_name,
            temperature=temperature,
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
            api_key=api_key
        )
        
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
        # 获取所有注册节点的元数据信息
        available_nodes = []
        for node_name, node_info in node_registry.items():
            metadata = node_info["metadata"]
            available_nodes.append({
                "name": metadata.name,
                "description": metadata.description,
                "inputs": [
                    {
                        "name": param.name,
                        "description": param.description,
                        "data_type": param.data_type,
                        "required": param.required,
                        "default_value": param.default_value
                    }
                    for param in metadata.inputs
                ],
                "outputs": [
                    {
                        "name": param.name,
                        "description": param.description,
                        "data_type": param.data_type
                    }
                    for param in metadata.outputs
                ],
                "version": metadata.version
            })
        
        prompt = f"""
你是一位专业的工作流编排工程师，请根据用户的需求生成一个符合要求的流程图JSON。

# 可用节点信息
以下是系统中可用的所有节点及其元数据信息：
{json.dumps(available_nodes, ensure_ascii=False, indent=2)}

# 参数样式说明
在工作流JSON中，节点参数支持三种不同的source_type样式：
1. raw_input：直接从原始输入中获取参数值，使用input_key指定原始输入的键
   示例：{{"name": "source_db", "source_type": "raw_input", "input_key": "source_db"}}

2. node_output：从其他节点的输出中获取参数值，使用node_id和output_field指定
   示例：{{"name": "db_type", "source_type": "node_output", "node_id": "db_type_query", "output_field": "db_type"}}

3. complex：使用Python脚本转换输入，使用transform_script指定转换脚本
   示例：{{"name": "lake_db_type", "source_type": "complex", "transform_script": "result = state.get('source_data', {{}}).get('lake_db_type', 'hive')"}}

用户需求：
{user_requirement}

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
2. 请确保流程图JSON格式正确，没有语法错误
3. 请确保所有节点ID唯一，并且边的source和target指向存在的节点ID
4. 请为每个节点提供清晰的名称
5. 请指定正确的起始节点和结束节点
6. 对于有条件的边，请提供条件表达式
7. 必须使用系统中存在的节点类型
8. 参数设置必须符合节点元数据的要求

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

# 创建全局的工作流编排智能体实例
workflow_agent = WorkflowAgent()

def get_workflow_agent() -> WorkflowAgent:
    """
    获取工作流编排智能体实例
    
    Returns:
        工作流编排智能体实例
    """
    return workflow_agent