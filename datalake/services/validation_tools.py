#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证智能体使用的工具集合
"""

from typing import Dict, Any, List, Optional
import random
import time

# 模拟数据存储
mock_tables = {
    "default.test_table1": {
        "ddl": "CREATE TABLE default.test_table1 (id INT, name STRING, age INT) COMMENT 'Test table'",
        "database": "default",
        "table_name": "test_table1"
    },
    "default.test_table2": {
        "ddl": "CREATE TABLE default.test_table2 (user_id INT, email STRING, phone STRING) COMMENT 'Test table 2'",
        "database": "default",
        "table_name": "test_table2"
    }
}

mock_integration_tasks = {
    "task_123": {
        "task_id": "task_123",
        "name": "Test Integration Task",
        "status": "running",
        "source_table": "source_db.source_table",
        "target_table": "target_db.target_table",
        "created_at": "2023-01-01 10:00:00",
        "updated_at": "2023-01-01 10:05:00"
    },
    "task_456": {
        "task_id": "task_456",
        "name": "Another Integration Task",
        "status": "completed",
        "source_table": "source_db.another_table",
        "target_table": "target_db.another_table",
        "created_at": "2023-01-02 14:30:00",
        "updated_at": "2023-01-02 15:00:00"
    }
}

def delete_table(database_name: str, table_name: str) -> Dict[str, Any]:
    """
    删除表的工具
    
    Args:
        database_name: 数据库名称
        table_name: 表名称
    
    Returns:
        删除结果
    """
    # 模拟延迟
    time.sleep(random.uniform(0.1, 0.5))
    
    table_key = f"{database_name}.{table_name}"
    
    if table_key in mock_tables:
        del mock_tables[table_key]
        return {
            "success": True,
            "message": f"Table {database_name}.{table_name} deleted successfully",
            "database": database_name,
            "table_name": table_name
        }
    else:
        return {
            "success": False,
            "message": f"Table {database_name}.{table_name} not found",
            "database": database_name,
            "table_name": table_name
        }

def delete_integration_task(task_id: str) -> Dict[str, Any]:
    """
    删除集成任务的工具
    
    Args:
        task_id: 集成任务ID
    
    Returns:
        删除结果
    """
    # 模拟延迟
    time.sleep(random.uniform(0.1, 0.5))
    
    if task_id in mock_integration_tasks:
        del mock_integration_tasks[task_id]
        return {
            "success": True,
            "message": f"Integration task {task_id} deleted successfully",
            "task_id": task_id
        }
    else:
        return {
            "success": False,
            "message": f"Integration task {task_id} not found",
            "task_id": task_id
        }

def query_integration_task(task_id: Optional[str] = None, status: Optional[str] = None) -> Dict[str, Any]:
    """
    查询集成任务的工具
    
    Args:
        task_id: 集成任务ID（可选）
        status: 任务状态（可选）
    
    Returns:
        查询结果
    """
    # 模拟延迟
    time.sleep(random.uniform(0.1, 0.5))
    
    if task_id:
        if task_id in mock_integration_tasks:
            return {
                "success": True,
                "tasks": [mock_integration_tasks[task_id]],
                "count": 1
            }
        else:
            return {
                "success": True,
                "tasks": [],
                "count": 0
            }
    elif status:
        filtered_tasks = [task for task in mock_integration_tasks.values() if task["status"] == status]
        return {
            "success": True,
            "tasks": filtered_tasks,
            "count": len(filtered_tasks)
        }
    else:
        return {
            "success": True,
            "tasks": list(mock_integration_tasks.values()),
            "count": len(mock_integration_tasks)
        }

def get_table_ddl(database_name: str, table_name: str) -> Dict[str, Any]:
    """
    获取表DDL的工具
    
    Args:
        database_name: 数据库名称
        table_name: 表名称
    
    Returns:
        表DDL信息
    """
    # 模拟延迟
    time.sleep(random.uniform(0.1, 0.5))
    
    table_key = f"{database_name}.{table_name}"
    
    if table_key in mock_tables:
        return {
            "success": True,
            "ddl": mock_tables[table_key]["ddl"],
            "database": database_name,
            "table_name": table_name
        }
    else:
        return {
            "success": False,
            "message": f"Table {database_name}.{table_name} not found",
            "database": database_name,
            "table_name": table_name
        }

# 工具注册表
tool_registry = {
    "delete_table": {
        "function": delete_table,
        "description": "删除指定数据库中的表",
        "parameters": [
            {
                "name": "database_name",
                "type": "string",
                "description": "数据库名称",
                "required": True
            },
            {
                "name": "table_name",
                "type": "string",
                "description": "表名称",
                "required": True
            }
        ]
    },
    "delete_integration_task": {
        "function": delete_integration_task,
        "description": "删除指定的集成任务",
        "parameters": [
            {
                "name": "task_id",
                "type": "string",
                "description": "集成任务ID",
                "required": True
            }
        ]
    },
    "query_integration_task": {
        "function": query_integration_task,
        "description": "查询集成任务，可按任务ID或状态过滤",
        "parameters": [
            {
                "name": "task_id",
                "type": "string",
                "description": "集成任务ID（可选）",
                "required": False
            },
            {
                "name": "status",
                "type": "string",
                "description": "任务状态（可选）",
                "required": False
            }
        ]
    },
    "get_table_ddl": {
        "function": get_table_ddl,
        "description": "获取指定表的DDL语句",
        "parameters": [
            {
                "name": "database_name",
                "type": "string",
                "description": "数据库名称",
                "required": True
            },
            {
                "name": "table_name",
                "type": "string",
                "description": "表名称",
                "required": True
            }
        ]
    }
}