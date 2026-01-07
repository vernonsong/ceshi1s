from typing import Dict, Any
from datalake.core.workflow.models import NodeMetadata, NodeInputParameter, NodeOutputParameter, register_node

# 制品生成节点元数据
artifact_generate_metadata = NodeMetadata(
    name="artifact_generate",
    description="制品生成节点，生成数据集成任务的制品",
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
        ),
        NodeInputParameter(
            name="execution_result",
            description="执行结果",
            data_type="dict",
            required=True
        )
    ],
    outputs=[
        NodeOutputParameter(
            name="status",
            description="生成状态",
            data_type="string"
        ),
        NodeOutputParameter(
            name="artifacts",
            description="生成的制品列表",
            data_type="list"
        ),
        NodeOutputParameter(
            name="artifact_info",
            description="制品信息",
            data_type="dict"
        ),
        NodeOutputParameter(
            name="storage_location",
            description="存储位置",
            data_type="string"
        )
    ],
    category="artifact"
)


# 制品生成节点
@register_node(artifact_generate_metadata)
def artifact_generate_node(state: dict) -> Dict[str, Any]:
    print(f"Executing Artifact Generate Node for request: {state.get('request_id')}")
    
    # 从集成任务生成结果中获取任务信息
    integration_task_generate_result = state.get("results", {}).get("integration_task_generate", {})
    task_id = integration_task_generate_result.get("task_id")
    task_name = integration_task_generate_result.get("task_name")
    task_parameters = integration_task_generate_result.get("task_parameters", {})
    
    # 从SQL执行结果中获取执行结果
    sql_execute_result = state.get("results", {}).get("sql_execute", {})
    execution_result = sql_execute_result.get("execution_result", {})
    
    # 模拟制品生成
    # 随机生成制品列表
    import random
    artifacts = [
        {
            "name": f"{task_name}_ddl.sql",
            "type": "sql",
            "size": random.randint(1000, 10000),
            "created_time": "2024-01-01 10:00:00"
        },
        {
            "name": f"{task_name}_execution.log",
            "type": "log",
            "size": random.randint(5000, 50000),
            "created_time": "2024-01-01 10:01:00"
        },
        {
            "name": f"{task_name}_metadata.json",
            "type": "json",
            "size": random.randint(1000, 5000),
            "created_time": "2024-01-01 10:02:00"
        }
    ]
    
    storage_location = f"s3://datalake-artifacts/{task_id}/"
    
    artifact_info = {
        "task_id": task_id,
        "task_name": task_name,
        "artifact_count": len(artifacts),
        "total_size": sum(artifact["size"] for artifact in artifacts),
        "storage_location": storage_location,
        "created_time": "2024-01-01 10:00:00"
    }
    
    return {
        "request_id": state.get('request_id'),
        "workflow_config": state.get('workflow_config'),
        "source_data": state.get('source_data'),
        "results": {
            **state.get('results', {}),
            "artifact_generate": {
                "status": "success",
                "artifacts": artifacts,
                "artifact_info": artifact_info,
                "storage_location": storage_location
            }
        },
        "current_node": "artifact_generate"
    }