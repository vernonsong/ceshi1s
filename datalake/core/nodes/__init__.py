# 欲买桂花同载酒，
# 终不似、少年游。
# Copyright (c) VernonSong. All rights reserved.
# ======================================================================================================================

# 导入所有节点
from .page_submit import page_submit_node
from .table_check import table_check_node, table_check_metadata
from .llm import llm_node
from .sql_generate import sql_generate_node, sql_generate_metadata
from .sql_execute import sql_execute_node, sql_execute_metadata
from .integration_task_generate import integration_task_generate_node, integration_task_generate_metadata
from .integration_task_deploy import integration_task_deploy_node, integration_task_deploy_metadata
from .artifact_generate import artifact_generate_node, artifact_generate_metadata
from .example_node import example_node
from .data_processing import data_processing_node, InputData, ProcessingConfig, DataSourceConfig, ProcessingResult
from .db_type_query import db_type_query_node
from .table_field_query import table_field_query_node

# 创建节点映射
try:
    NODE_MAPPING = {
        "page_submit": page_submit_node,
        "table_check": table_check_node,
        "llm": llm_node,
        "sql_generate": sql_generate_node,
        "sql_execute": sql_execute_node,
        "integration_task_generate": integration_task_generate_node,
        "integration_task_deploy": integration_task_deploy_node,
        "artifact_generate": artifact_generate_node,
        "example": example_node,
        "db_type_query": db_type_query_node,
        "table_field_query": table_field_query_node,
        "data_processing": data_processing_node,
        "db_type_query": db_type_query_node
    }
except Exception as e:
    print(f"Error creating NODE_MAPPING: {e}")
    NODE_MAPPING = {}

# 导出所有节点
__all__ = [
    # 页面提单节点
    "page_submit_node",
    
    # 表检查节点
    "table_check_node",
    "table_check_metadata",
    
    # LLM节点
    "llm_node",
    
    # SQL生成节点
    "sql_generate_node",
    "sql_generate_metadata",
    
    # SQL执行节点
    "sql_execute_node",
    "sql_execute_metadata",
    
    # 集成任务生成节点
    "integration_task_generate_node",
    "integration_task_generate_metadata",
    
    # 集成任务部署节点
    "integration_task_deploy_node",
    "integration_task_deploy_metadata",
    
    # 制品生成节点
    "artifact_generate_node",
    "artifact_generate_metadata",
    
    # 示例节点
    "example_node",
    
    # 数据处理节点
    "data_processing_node",
    "InputData",
    "ProcessingConfig",
    "DataSourceConfig",
    "ProcessingResult",
    
    # 数据库类型查询节点
    "db_type_query_node",
    
    # 节点映射
    "NODE_MAPPING"
]