# 欲买桂花同载酒，
# 终不似、少年游。
# Copyright (c) VernonSong. All rights reserved.
# ======================================================================================================================
from fastapi import APIRouter, HTTPException
from datalake.core.workflow.workflow_manager import WorkflowManager
from datalake.core.workflow.models import WorkflowConfig, LakeIngestionRequest
from typing import List, Dict, Any

router = APIRouter()

# 工作流管理器实例
workflow_manager = WorkflowManager()


@router.post("/workflows/register", response_model=Dict[str, Any])
async def register_workflow(config: WorkflowConfig):
    """
    注册工作流
    
    Args:
        config: 工作流配置
        
    Returns:
        注册结果
    """
    try:
        workflow_name = workflow_manager.register_workflow(config)
        return {"message": "Workflow registered successfully", "workflow_name": workflow_name}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/workflows/execute", response_model=Dict[str, Any])
async def execute_workflow(request: LakeIngestionRequest):
    """
    执行工作流
    
    Args:
        request: 入湖请求
        
    Returns:
        执行结果
    """
    try:
        result = workflow_manager.execute_workflow(request)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows", response_model=List[Dict[str, Any]])
async def list_workflows():
    """
    列出所有工作流
    
    Returns:
        工作流列表
    """
    return workflow_manager.list_workflows()


@router.get("/workflows/{workflow_name}", response_model=WorkflowConfig)
async def get_workflow(workflow_name: str):
    """
    获取工作流配置
    
    Args:
        workflow_name: 工作流名称
        
    Returns:
        工作流配置
    """
    try:
        config = workflow_manager.get_workflow_config(workflow_name)
        return config
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.put("/workflows/update", response_model=Dict[str, Any])
async def update_workflow(config: WorkflowConfig):
    """
    更新工作流
    
    Args:
        config: 工作流配置
        
    Returns:
        更新结果
    """
    try:
        workflow_name = workflow_manager.update_workflow(config)
        return {"message": "Workflow updated successfully", "workflow_name": workflow_name}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/workflows/{workflow_name}", response_model=Dict[str, bool])
async def delete_workflow(workflow_name: str):
    """
    删除工作流
    
    Args:
        workflow_name: 工作流名称
        
    Returns:
        删除结果
    """
    try:
        result = workflow_manager.delete_workflow(workflow_name)
        if result:
            return {"message": "Workflow deleted successfully", "success": True}
        else:
            raise HTTPException(status_code=404, detail="Workflow not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))