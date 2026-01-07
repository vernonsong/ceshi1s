from typing import Dict, Any
from datalake.core.workflow.models import NodeMetadata, NodeInputParameter, NodeOutputParameter, register_node

# 集成任务部署节点元数据
integration_task_deploy_metadata = NodeMetadata(
    name="integration_task_deploy",
    description="集成任务部署节点，部署数据集成任务",
    type="task",
    inputs=[
        NodeInputParameter(
            name="task_id",
            description="任务ID",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="task_name",
            description="任务名称",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="task_parameters",
            description="任务参数",
            data_type="dict",
            required=True
        )
    ],
    outputs=[
        NodeOutputParameter(
            name="status",
            description="部署状态",
            data_type="string"
        ),
        NodeOutputParameter(
            name="deployment_result",
            description="部署结果",
            data_type="dict"
        ),
        NodeOutputParameter(
            name="task_schedule",
            description="任务调度信息",
            data_type="dict"
        ),
        NodeOutputParameter(
            name="error_message",
            description="错误信息",
            data_type="string"
        )
    ],
    category="integration"
)


# 集成任务部署节点
@register_node(integration_task_deploy_metadata)
def integration_task_deploy_node(state: dict) -> Dict[str, Any]:
    print(f"Executing Integration Task Deploy Node for request: {state.get('request_id')}")
    
    # 从集成任务生成结果中获取任务信息
    integration_task_generate_result = state.get("results", {}).get("integration_task_generate", {})
    task_id = integration_task_generate_result.get("task_id")
    task_name = integration_task_generate_result.get("task_name")
    task_parameters = integration_task_generate_result.get("task_parameters", {})
    
    # 模拟任务部署
    # 随机生成部署结果，95%概率成功
    import random
    is_success = random.choice([True] * 19 + [False])
    
    if is_success:
        status = "success"
        deployment_result = {
            "task_id": task_id,
            "task_name": task_name,
            "status": "deployed",
            "message": "Task deployed successfully"
        }
        task_schedule = {
            "task_id": task_id,
            "schedule_type": "cron",
            "schedule_expression": "0 0 * * *",
            "next_run_time": "2024-01-02 00:00:00",
            "is_active": True
        }
        error_message = ""
    else:
        status = "failed"
        deployment_result = {
            "task_id": task_id,
            "task_name": task_name,
            "status": "failed",
            "message": "Task deployment failed"
        }
        task_schedule = {}
        error_message = "Connection timeout while deploying task"
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "integration_task_deploy": {
                "status": status,
                "deployment_result": deployment_result,
                "task_schedule": task_schedule,
                "error_message": error_message
            }
        },
        "current_node": "integration_task_deploy"
    }