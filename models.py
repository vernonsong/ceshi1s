from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from datetime import datetime


# 节点参数模型
class NodeParameter(BaseModel):
    name: str
    description: str
    data_type: str
    required: bool = False
    default_value: Optional[Any] = None


# 节点输入参数模型
class NodeInputParameter(NodeParameter):
    pass


# 节点输出参数模型
class NodeOutputParameter(NodeParameter):
    pass


# 节点元数据模型
class NodeMetadata(BaseModel):
    name: str
    description: str
    type: str  # task, gateway
    inputs: List[NodeInputParameter]
    outputs: List[NodeOutputParameter]
    category: str  # ingestion, validation, transformation, deployment
    version: str = "1.0.0"


# 工作流配置模型
class WorkflowConfig(BaseModel):
    name: str
    description: str
    nodes: List[str]
    edges: List[Dict[str, Any]]
    node_configs: Dict[str, Any]
    orchestration_type: str = "manual"  # manual, ai
    acceptance_criteria: Optional[List[str]] = None
    generated_by: Optional[str] = None
    llm_response: Optional[str] = None  # 大模型响应
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()


# 入湖请求模型
class LakeIngestionRequest(BaseModel):
    workflow_name: str
    source_data: Dict[str, Any]
    custom_params: Optional[Dict[str, Any]] = None


# 工作流状态模型
class WorkflowState(BaseModel):
    request_id: str
    workflow_config: WorkflowConfig
    source_data: Dict[str, Any]
    current_node: str
    results: Dict[str, Any] = {}
    errors: List[Dict[str, Any]] = []
    status: str = "pending"  # pending, running, completed, failed, blocked, waiting
    custom_params: Dict[str, Any] = {}
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    execution_logs: List[Dict[str, Any]] = []


# AI编排请求模型
class AIOchestrationRequest(BaseModel):
    business_requirement: str
    acceptance_criteria: List[str]
    source_data_example: Optional[Dict[str, Any]] = None
    custom_params: Optional[Dict[str, Any]] = None


# AI编排迭代结果模型
class AIOchestrationIteration(BaseModel):
    iteration_number: int
    workflow_config: WorkflowConfig
    execution_result: Dict[str, Any]
    validation_result: Dict[str, Any]
    feedback: str
    created_at: datetime = datetime.now()


# AI编排结果模型
class AIOchestrationResult(BaseModel):
    request_id: str
    status: str  # running, completed, failed
    business_requirement: str
    acceptance_criteria: List[str]
    iterations: List[AIOchestrationIteration] = []
    final_workflow_config: Optional[WorkflowConfig] = None
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()


# 节点配置模型
class NodeConfig(BaseModel):
    # 入湖表检查配置
    table_check_rules: Optional[List[str]] = None
    # 集成任务配置
    integration_parallelism: Optional[int] = 1
    # SQL生成配置
    sql_template: Optional[str] = None
    # 调度任务配置
    schedule_cron: Optional[str] = "0 0 * * *"
    # 初始化任务配置
    init_batch_size: Optional[int] = 1000
    # 大模型配置
    llm_model: Optional[str] = "gpt-4"
    # 制品生成配置
    artifact_type: Optional[str] = "workflow"
    artifact_location: Optional[str] = None


# 执行结果模型
class ExecutionResult(BaseModel):
    request_id: str
    workflow_name: str
    status: str  # completed, failed, blocked
    results: Dict[str, Any]
    errors: List[Dict[str, Any]]
    execution_time: float
    started_at: datetime
    completed_at: Optional[datetime] = None


# 并行执行状态模型
class ParallelExecutionState(BaseModel):
    node_name: str
    status: str  # running, completed, failed
    results: Dict[str, Any]
    errors: List[Dict[str, Any]]
    started_at: datetime
    completed_at: Optional[datetime] = None


# 等待状态模型
class WaitState(BaseModel):
    node_name: str
    wait_type: str  # manual_approval, time_delay, external_event
    wait_condition: Dict[str, Any]
    wait_until: Optional[datetime] = None
    status: str = "waiting"  # waiting, completed, cancelled
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
