from typing import Dict, Any, List
from uuid import uuid4
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from models import WorkflowState, WorkflowConfig, LakeIngestionRequest
from nodes import NODE_MAPPING


class WorkflowManager:
    def __init__(self):
        self.workflows: Dict[str, StateGraph] = {}
        self.workflow_configs: Dict[str, WorkflowConfig] = {}
        self.memory = MemorySaver()
    
    def register_workflow(self, config: WorkflowConfig) -> str:
        """注册新的工作流"""
        print(f"Registering workflow: {config.name}")
        
        # 创建状态图
        workflow = StateGraph(dict)
        
        # 添加节点
        for node_name in config.nodes:
            if node_name in NODE_MAPPING:
                # 普通任务节点
                workflow.add_node(node_name, NODE_MAPPING[node_name])
            elif 'gateway' in node_name:
                # 网关节点 - 简单传递状态
                workflow.add_node(node_name, lambda state: state)
            else:
                raise ValueError(f"Unknown node type: {node_name}")
        
        # 处理条件边
        # 首先收集所有条件边和非条件边
        conditional_edges = {}
        non_conditional_edges = []
        
        for edge in config.edges:
            if edge.get('condition'):
                start = edge['start']
                if start not in conditional_edges:
                    conditional_edges[start] = []
                conditional_edges[start].append(edge)
            else:
                non_conditional_edges.append(edge)
        
        # 处理非条件边
        for edge in non_conditional_edges:
            workflow.add_edge(edge['start'], edge['end'])
        
        # 处理条件边
        for start_node, edges in conditional_edges.items():
            if start_node == 'table_check':
                # 对于table_check节点，使用统一的条件函数处理所有分支
                def table_check_conditional(state):
                    table_check_result = state.get('results', {}).get('table_check', {})
                    status = table_check_result.get('status')
                    
                    # 过滤掉指向自身的边，避免无限循环
                    valid_edges = [e for e in edges if e['end'] != 'table_check']
                    
                    # 如果没有有效的边，返回None
                    if not valid_edges:
                        return None
                    
                    # 根据状态返回不同的分支
                    if status == 'failed':
                        # 找到失败分支的目标节点，使用默认值避免StopIteration
                        failed_edge = next((e for e in valid_edges if e['condition']['type'] == 'table_check_failed'), None)
                        if failed_edge:
                            return failed_edge['end']
                        # 如果没有找到失败分支，返回第一个可用的节点
                        return valid_edges[0]['end']
                    elif status == 'success':
                        # 找到成功分支的目标节点，使用默认值避免StopIteration
                        # 注意：LangGraph不直接支持并行执行，所以我们只返回第一个成功分支
                        passed_edge = next((e for e in valid_edges if e['condition']['type'] == 'table_check_passed'), None)
                        if passed_edge:
                            return passed_edge['end']
                        # 如果没有找到成功分支，返回第一个可用的节点
                        return valid_edges[0]['end']
                    else:
                        # 如果状态未知，返回第一个可用的节点
                        return valid_edges[0]['end']
                
                # 创建分支映射
                branches = {}
                for edge in edges:
                    branches[edge['end']] = edge['end']
                
                # 添加条件边
                workflow.add_conditional_edges(
                    start_node,
                    table_check_conditional,
                    branches
                )
            else:
                # 其他节点的条件边
                for edge in edges:
                    # 使用lambda函数和默认参数来捕获变量值，避免闭包问题
                    condition = edge['condition']
                    end_node = edge['end']
                    
                    condition_func = lambda state, cond=condition, end=end_node: {
                        True: end if (
                            (cond.get('type') == 'equals' and state.get(cond.get('field')) == cond.get('value')) or
                            (cond.get('type') == 'not_equals' and state.get(cond.get('field')) != cond.get('value')) or
                            cond.get('type') not in ['equals', 'not_equals']
                        ) else None
                    }
                    
                    # 使用统一的条件函数返回分支名称
                    def get_branch(state, cf=condition_func):
                        result = cf(state)
                        return result.get(True)
                    
                    # 创建分支映射
                    branches = {end_node: end_node}
                    
                    workflow.add_conditional_edges(
                        start_node,
                        get_branch,
                        branches
                    )
        

        
        # 设置入口点
        workflow.set_entry_point(config.nodes[0])
        
        # 设置结束点
        # 找到所有没有出边的节点作为结束点
        end_nodes = []
        for node in config.nodes:
            has_out_edge = any(edge['start'] == node for edge in config.edges)
            if not has_out_edge:
                end_nodes.append(node)
        
        for end_node in end_nodes:
            workflow.set_finish_point(end_node)
        
        # 编译工作流
        compiled_workflow = workflow.compile(checkpointer=self.memory)
        
        # 存储工作流
        self.workflows[config.name] = compiled_workflow
        self.workflow_configs[config.name] = config
        
        return config.name
    
    def execute_workflow(self, request: LakeIngestionRequest) -> Dict[str, Any]:
        """执行工作流"""
        print(f"Executing workflow: {request.workflow_name}")
        
        if request.workflow_name not in self.workflows:
            raise ValueError(f"Workflow not found: {request.workflow_name}")
        
        workflow = self.workflows[request.workflow_name]
        workflow_config = self.workflow_configs[request.workflow_name]
        
        # 生成请求ID
        request_id = str(uuid4())
        
        # 合并自定义参数
        custom_params = request.custom_params or {}
        
        # 初始化状态
        initial_state = {
            "request_id": request_id,
            "workflow_config": workflow_config,
            "source_data": request.source_data,
            "current_node": workflow_config.nodes[0],
            "results": {},
            "errors": [],
            "status": "running",
            "custom_params": custom_params
        }
        
        # 执行工作流，添加错误处理
        try:
            # 直接使用invoke方法，stream方法可能有问题
            result = workflow.invoke(
                initial_state,
                config={"configurable": {"thread_id": request_id}}
            )
            
            # 返回结果，确保包含results字段
            return {
                "request_id": request_id,
                "status": result.get("status", "completed"),
                "results": result.get("results", {}),
                "errors": result.get("errors", []),
                "workflow_name": request.workflow_name
            }
        except Exception as e:
            print(f"工作流执行失败: {e}")
            # 返回错误结果，包含已执行的节点结果
            return {
                "request_id": request_id,
                "status": "failed",
                "results": {},  # 重置结果，避免部分执行结果导致问题
                "errors": [str(e)],
                "workflow_name": request.workflow_name
            }
    
    def get_workflow_config(self, workflow_name: str) -> WorkflowConfig:
        """获取工作流配置"""
        if workflow_name not in self.workflow_configs:
            raise ValueError(f"Workflow not found: {workflow_name}")
        return self.workflow_configs[workflow_name]
    
    def list_workflows(self) -> List[Dict[str, Any]]:
        """列出所有注册的工作流"""
        return [
            {
                "name": name,
                "description": config.description,
                "nodes_count": len(config.nodes),
                "edges_count": len(config.edges)
            }
            for name, config in self.workflow_configs.items()
        ]
    
    def update_workflow(self, config: WorkflowConfig) -> str:
        """更新工作流"""
        return self.register_workflow(config)
    
    def delete_workflow(self, workflow_name: str) -> bool:
        """删除工作流"""
        if workflow_name in self.workflows:
            del self.workflows[workflow_name]
            del self.workflow_configs[workflow_name]
            return True
        return False
