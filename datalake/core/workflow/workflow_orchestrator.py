import json
import re
import ast
from typing import Dict, Any, List, Optional
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from datalake.core.nodes.db_type_query import db_type_query_node
from datalake.core.nodes.table_field_query import table_field_query_node
from datalake.core.nodes.sql_generate import sql_generate_node
from datalake.core.nodes.sql_execute import sql_execute_node
from datalake.core.nodes.page_submit import page_submit_node
from datalake.core.nodes.table_check import table_check_node
from datalake.core.nodes.data_processing import data_processing_node
from datalake.core.nodes.example_node import example_node

# 定义工作流状态类型
class WorkflowState(Dict[str, Any]):
    """工作流状态类"""
    pass

def create_workflow_from_json(workflow_json: str, checkpoint_saver: Optional[MemorySaver] = None) -> StateGraph:
    """
    根据流程图JSON创建langgraph工作流
    
    Args:
        workflow_json: 流程图JSON字符串
        checkpoint_saver: 可选的检查点保存器，用于持久化工作流状态
        
    Returns:
        构建好的StateGraph对象
    """
    # 解析流程图JSON
    workflow_data = json.loads(workflow_json)
    
    # 提取节点和边
    nodes = workflow_data.get("nodes", [])
    edges = workflow_data.get("edges", [])
    
    # 创建状态图
    graph = StateGraph(WorkflowState)
    
    # 节点注册字典，用于存储节点类型和对应处理函数的映射
    node_registry = {
        "db_type_query": db_type_query_node,
        "table_field_query": table_field_query_node,
        "sql_generate": sql_generate_node,
        "sql_execute": sql_execute_node,
        "page_submit": page_submit_node,
        "table_check": table_check_node,
        "data_processing": data_processing_node,
        "example": example_node
    }
    
    # 添加节点到图中
    for node in nodes:
        node_id = node.get("id")
        node_type = node.get("type")
        
        if node_type in node_registry:
            node_func = node_registry[node_type]
            graph.add_node(node_id, node_func)
        else:
            raise ValueError(f"不支持的节点类型: {node_type}")
    
    # 解析并添加边
    for edge in edges:
        source = edge.get("source")
        target = edge.get("target")
        
        if not source or not target:
            continue
            
        # 检查是否为条件边
        if "condition" in edge:
            # 处理条件边
            condition = edge.get("condition")
            
            def conditional_router(state, condition=condition):
                """条件路由函数"""
                # 执行条件表达式
                try:
                    # 将状态变量注入局部作用域
                    local_vars = {**state}
                    return "next" if eval(condition, {}, local_vars) else "end"
                except Exception as e:
                    print(f"条件执行失败: {e}")
                    return "next"
            
            # 添加条件路由
            graph.add_conditional_edges(source, conditional_router, {
                "next": target,
                "end": END
            })
        else:
            # 添加普通边
            graph.add_edge(source, target)
    
    # 配置起始节点
    start_node = workflow_data.get("start_node")
    if start_node:
        graph.set_entry_point(start_node)
    else:
        # 如果没有指定起始节点，使用第一个节点
        if nodes:
            graph.set_entry_point(nodes[0].get("id"))
    
    # 配置结束节点
    end_nodes = workflow_data.get("end_nodes", [])
    for end_node in end_nodes:
        graph.add_edge(end_node, END)
    
    # 如果没有指定结束节点，将所有没有出边的节点连接到END
    if not end_nodes:
        # 收集所有有出边的节点
        nodes_with_outgoing = set()
        for edge in edges:
            source = edge.get("source")
            if source:
                nodes_with_outgoing.add(source)
        
        # 将没有出边的节点连接到END
        for node in nodes:
            node_id = node.get("id")
            if node_id not in nodes_with_outgoing:
                graph.add_edge(node_id, END)
    
    # 编译图
    compiled_graph = graph.compile(checkpointer=checkpoint_saver)
    
    return compiled_graph

def execute_workflow_with_params(compiled_graph, input_params: Dict[str, Any]) -> Dict[str, Any]:
    """
    执行工作流并返回结果
    
    Args:
        compiled_graph: 编译好的langgraph工作流
        input_params: 输入参数
        
    Returns:
        工作流执行结果
    """
    # 准备初始状态
    initial_state = {
        "request_id": input_params.get("request_id", "test_request"),
        "workflow_config": input_params.get("workflow_config", {}),
        "source_data": input_params.get("source_data", {}),
        "results": {}
    }
    
    # 执行工作流
    result = compiled_graph.invoke(initial_state)
    
    return result

def process_complex_params(params: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    """
    处理复杂参数来源，执行Python脚本转换输入
    
    Args:
        params: 参数配置，包含source和transform_script
        state: 当前工作流状态
        
    Returns:
        转换后的参数
    """
    processed_params = {}
    
    for param_name, param_config in params.items():
        source_type = param_config.get("source_type")
        
        if source_type == "complex":
            # 处理复杂来源
            transform_script = param_config.get("transform_script")
            
            if transform_script:
                try:
                    # 准备执行环境
                    local_vars = {"state": state, "result": None}
                    
                    # 执行转换脚本
                    exec(transform_script, globals(), local_vars)
                    
                    # 获取转换结果
                    processed_params[param_name] = local_vars.get("result")
                except Exception as e:
                    print(f"执行转换脚本失败: {e}")
                    processed_params[param_name] = None
            else:
                processed_params[param_name] = None
        elif source_type == "node_output":
            # 处理前置节点输出
            node_id = param_config.get("node_id")
            output_field = param_config.get("output_field")
            
            if node_id in state.get("results", {}):
                node_result = state["results"][node_id]
                processed_params[param_name] = node_result.get(output_field)
            else:
                processed_params[param_name] = None
        else:
            # 处理原始输入
            input_key = param_config.get("input_key")
            processed_params[param_name] = state.get("source_data", {}).get(input_key)
    
    return processed_params

def get_workflow_json_example() -> str:
    """
    获取流程图JSON示例
    
    Returns:
        流程图JSON字符串示例
    """
    example = {
        "nodes": [
            {
                "id": "db_type_query",
                "type": "db_type_query",
                "name": "数据库类型查询",
                "inputs": [
                    {
                        "name": "source_db",
                        "source_type": "raw_input",
                        "input_key": "source_db"
                    }
                ]
            },
            {
                "id": "table_field_query",
                "type": "table_field_query",
                "name": "表字段查询",
                "inputs": [
                    {
                        "name": "db_type",
                        "source_type": "node_output",
                        "node_id": "db_type_query",
                        "output_field": "db_type"
                    },
                    {
                        "name": "source_db",
                        "source_type": "raw_input",
                        "input_key": "source_db"
                    },
                    {
                        "name": "source_table",
                        "source_type": "raw_input",
                        "input_key": "source_table"
                    }
                ]
            },
            {
                "id": "sql_generate",
                "type": "sql_generate",
                "name": "SQL生成",
                "inputs": [
                    {
                        "name": "source_db_type",
                        "source_type": "node_output",
                        "node_id": "db_type_query",
                        "output_field": "db_type"
                    },
                    {
                        "name": "source_fields",
                        "source_type": "node_output",
                        "node_id": "table_field_query",
                        "output_field": "fields"
                    },
                    {
                        "name": "lake_db_type",
                        "source_type": "complex",
                        "transform_script": "result = state.get('source_data', {}).get('lake_db_type', 'hive')"
                    },
                    {
                        "name": "lake_schema",
                        "source_type": "raw_input",
                        "input_key": "lake_schema"
                    },
                    {
                        "name": "lake_table",
                        "source_type": "raw_input",
                        "input_key": "lake_table"
                    }
                ]
            },
            {
                "id": "sql_execute",
                "type": "sql_execute",
                "name": "SQL执行",
                "inputs": [
                    {
                        "name": "sql",
                        "source_type": "node_output",
                        "node_id": "sql_generate",
                        "output_field": "generated_sql"
                    },
                    {
                        "name": "db_type",
                        "source_type": "node_output",
                        "node_id": "db_type_query",
                        "output_field": "db_type"
                    }
                ]
            }
        ],
        "edges": [
            {
                "source": "db_type_query",
                "target": "table_field_query"
            },
            {
                "source": "table_field_query",
                "target": "sql_generate"
            },
            {
                "source": "sql_generate",
                "target": "sql_execute"
            },
            {
                "source": "sql_execute",
                "target": "",
                "condition": "state.get('results', {}).get('sql_execute', {}).get('status') == 'success'"
            }
        ],
        "start_node": "db_type_query",
        "end_nodes": ["sql_execute"]
    }
    
    return json.dumps(example, indent=2, ensure_ascii=False)