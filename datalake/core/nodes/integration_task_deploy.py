from typing import Dict, Any
from datalake.core.workflow.models import NodeMetadata, NodeInputParameter, NodeOutputParameter, register_node

# 集成任务部署节点元数据
integration_task_deploy_metadata = NodeMetadata(
    name="integration_task_deploy",
    description="集成任务部署节点，部署数据集成任务",
    type="task",
    inputs=[
        NodeInputParameter(
            name="upstream_api_json",
            description="调用上游接口的JSON数据",
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
            name="jobid",
            description="部署任务的Job ID",
            data_type="string"
        ),
        NodeOutputParameter(
            name="deploy_message",
            description="部署消息",
            data_type="string"
        )
    ],
    category="integration"
)


# 集成任务部署节点
@register_node(integration_task_deploy_metadata)
def integration_task_deploy_node(state: dict) -> Dict[str, Any]:
    print(f"Executing Integration Task Deploy Node for request: {state.get('request_id')}")
    
    # 获取输入参数
    source_data = state.get("source_data", {})
    results = state.get("results", {})
    
    # 从integration_task_generate结果中获取上游接口JSON
    integration_task_generate_result = results.get("integration_task_generate", {})
    upstream_api_json = integration_task_generate_result.get("upstream_api_json", {})
    
    # 如果results中没有，尝试从source_data获取
    if not upstream_api_json:
        upstream_api_json = source_data.get("upstream_api_json", {})
    
    print(f"Deploying task with JSON: {upstream_api_json}")
    print(f"Task Name: {upstream_api_json.get('task_info', {}).get('name')}")
    
    # 模拟调用上游接口部署任务
    import random
    import time
    
    # 模拟API调用延迟
    time.sleep(0.5)
    
    # 随机生成部署结果，95%概率成功
    is_success = random.choice([True] * 19 + [False])
    
    if is_success:
        status = "success"
        # 生成随机的jobid
        jobid = f"JOB-{random.randint(10000000, 99999999)}"
        deploy_message = f"Task deployed successfully, Job ID: {jobid}"
    else:
        status = "failed"
        jobid = ""
        deploy_message = f"Task deployment failed: {random.choice(['API connection error', 'Invalid JSON format', 'Permission denied', 'Server error'])}"
    
    print(f"Deployment Result: {status}, JobID: {jobid}")
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **results,
            "integration_task_deploy": {
                "status": status,
                "jobid": jobid,
                "deploy_message": deploy_message
            }
        },
        "current_node": "integration_task_deploy"
    }