#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证智能体，基于LangChain Agent，用于验证工作流是否符合要求
"""

from typing import Dict, Any, List, Optional
from datalake.services.validation_tools import tool_registry
import json
from langchain_openai import ChatOpenAI

class ValidationAgent:
    """
    验证智能体，基于LangChain Agent
    """
    
    def __init__(self, model_name: str = "qwen-plus", temperature: float = 0.1):
        """
        初始化验证智能体
        
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
        
        # 最大迭代次数
        self.max_iterations = 10
    
    def validate_workflow(self, workflow_json: Dict[str, Any], validation_requirements: str) -> Dict[str, Any]:
        """
        验证工作流
        
        Args:
            workflow_json: 工作流JSON数据
            validation_requirements: 验证要求
            
        Returns:
            验证结果
        """
        # 构建初始提示
        initial_prompt = self._build_initial_prompt(workflow_json, validation_requirements)
        
        # 初始化对话历史
        conversation_history: List[Any] = [initial_prompt]
        
        # 限制最大迭代次数，避免无限循环
        max_iterations = 10
        current_iteration = 0
        
        while current_iteration < max_iterations:
            current_iteration += 1
            
            # 构建当前提示
            current_prompt = self._build_react_prompt(conversation_history)
            print("当前提示词:", current_prompt)
            # 调用大模型
            response = self.llm.invoke(current_prompt)
            print("大模型响应:", response.content)
            # 添加大模型响应到对话历史
            conversation_history.append(response.content)
            
            # 尝试解析工具调用
            tool_call = self._parse_tool_call(response.content)
            
            if not tool_call:
                # 没有工具调用，解析最终结果
                final_result = self._parse_final_result(response.content)
                return final_result
            
            # 执行工具调用
            tool_result = self._call_tool(tool_call)
            
            # 添加工具执行结果到对话历史
            conversation_history.append(tool_result)
        
        # 如果超过最大迭代次数，返回超时结果
        return {
            "status": "error",
            "message": "验证超时",
            "details": f"验证过程超过了最大迭代次数({max_iterations})，可能存在无限循环"
        }
    
    def _build_initial_prompt(self, workflow_json: Dict[str, Any], validation_requirements: str) -> str:
        """
        构建初始提示
        
        Args:
            workflow_json: 工作流JSON数据
            validation_requirements: 验证要求
            
        Returns:
            初始提示文本
        """
        initial_prompt = ""
        
        # 添加系统提示
        initial_prompt += f"你是一位专业的工作流验证工程师，负责验证工作流是否符合要求。\n\n"
        
        # 添加工作流信息
        initial_prompt += f"以下是当前工作流的详细信息：\n"
        initial_prompt += json.dumps(workflow_json, indent=2, ensure_ascii=False) + "\n\n"
        
        # 添加验证要求
        initial_prompt += f"验证要求：\n{validation_requirements}\n\n"
        
        # 添加工具信息
        initial_prompt += "可用工具：\n"
        initial_prompt += self._get_tools_description()
        
        # 添加输出格式要求
        initial_prompt += "\n\n请严格按照要求进行验证，不要进行任何与验证无关的操作或分析。\n"
        initial_prompt += "验证完成后，请按照以下格式输出结论：\n\n"
        initial_prompt += "```json\n"
        initial_prompt += "{\n"
        initial_prompt += "  \"status\": \"success\",\n"
        initial_prompt += "  \"message\": \"工作流验证通过\",\n"
        initial_prompt += "  \"details\": {\n"
        initial_prompt += "    \"check_points\": [\n"
        initial_prompt += "      {\n"
        initial_prompt += "        \"check_item\": \"节点完整性检查\",\n"
        initial_prompt += "        \"result\": \"通过\",\n"
        initial_prompt += "        \"reason\": \"工作流包含所有必要节点\"\n"
        initial_prompt += "      }\n"
        initial_prompt += "    ]\n"
        initial_prompt += "  }\n"
        initial_prompt += "}\n"
        initial_prompt += "```\n"
        
        return initial_prompt
        
    def _build_react_prompt(self, conversation_history: List[str]) -> str:
        """
        构建React提示
        
        Args:
            conversation_history: 对话历史
            
        Returns:
            React提示文本
        """
        react_prompt = ""
        
        # 添加对话历史
        for message in conversation_history:
            react_prompt += message + "\n\n"
        
        # 添加工具调用格式提示
        react_prompt += "工具调用格式：\n"
        react_prompt += "```json\n"
        react_prompt += "{\n"
        react_prompt += "  \"tool_call\": {\n"
        react_prompt += "    \"thought\": \"调用工具的思考过程\",\n"
        react_prompt += "    \"name\": \"工具名称\",\n"
        react_prompt += "    \"params\": {\n"
        react_prompt += "      \"参数名\": \"参数值\"\n"
        react_prompt += "    }\n"
        react_prompt += "  }\n"
        react_prompt += "}\n"
        react_prompt += "```\n"
        
        return react_prompt
        

        
    def _get_tools_description(self) -> str:
        """
        获取工具描述
        
        Returns:
            工具描述文本
        """
        tools_description = ""
        
        for tool_name, tool_info in tool_registry.items():
            tools_description += f"- {tool_name}：{tool_info['description']}\n"
            tools_description += "  参数：\n"
            for param in tool_info["parameters"]:
                req = "必填" if param["required"] else "可选"
                tools_description += f"    - {param['name']}：{param['description']}（类型：{param['type']}，{req}）\n"
        
        return tools_description
        
    def _parse_tool_call(self, response: str) -> Optional[Dict[str, Any]]:
        """
        解析工具调用
        
        Args:
            response: 大模型响应
            
        Returns:
            工具调用信息，如果没有则返回None
        """
        # 查找工具调用的JSON部分
        try:
            # 查找工具调用的开始和结束位置
            start_pos = response.find("```json") + 7
            end_pos = response.find("```", start_pos)
            
            if start_pos == -1 or end_pos == -1:
                return None
            
            # 解析JSON
            tool_call_json = response[start_pos:end_pos].strip()
            tool_call_data = json.loads(tool_call_json)
            
            # 检查是否是工具调用格式
            if "tool_call" in tool_call_data:
                return tool_call_data["tool_call"]
            
            return None
        except Exception as e:
            print(f"解析工具调用失败：{e}")
            return None
        
    def _call_tool(self, tool_call: Dict[str, Any]) -> str:
        """
        调用工具
        
        Args:
            tool_call: 工具调用信息
            
        Returns:
            工具调用结果
        """
        try:
            tool_name = tool_call["name"]
            params = tool_call["params"]
            
            # 检查工具是否存在
            if tool_name not in tool_registry:
                return f"错误：工具 {tool_name} 不存在"
            
            # 检查参数是否完整
            tool_info = tool_registry[tool_name]
            for param in tool_info["parameters"]:
                if param["required"] and param["name"] not in params:
                    return f"错误：工具 {tool_name} 缺少必填参数 {param['name']}"
            
            # 调用工具
            tool_func = tool_info["function"]
            result = tool_func(**params)
            
            # 返回结果
            return f"工具执行结果：\n{json.dumps(result, indent=2, ensure_ascii=False)}"
        except Exception as e:
            return f"工具调用失败：{str(e)}"
        
    def _parse_final_result(self, response: str) -> Dict[str, Any]:
        """
        解析最终结果
        
        Args:
            response: 大模型响应
            
        Returns:
            验证结果
        """
        try:
            # 查找JSON结果部分
            start_pos = response.find("```json") + 7
            end_pos = response.find("```", start_pos)
            
            if start_pos == -1 or end_pos == -1:
                # 如果没有找到JSON格式的结果，返回原始响应
                return {
                    "status": "error",
                    "message": "验证完成但未找到JSON格式的结果",
                    "details": response
                }
            
            # 解析JSON
            result_json = response[start_pos:end_pos].strip()
            result = json.loads(result_json)
            
            return result
        except Exception as e:
            print(f"解析最终结果失败：{e}")
            return {
                "status": "error",
                "message": "解析验证结果失败",
                "details": str(e)
            }

# 创建全局的验证智能体实例
validation_agent = ValidationAgent()

def get_validation_agent() -> ValidationAgent:
    """
    获取验证智能体实例
    
    Returns:
        验证智能体实例
    """
    return validation_agent