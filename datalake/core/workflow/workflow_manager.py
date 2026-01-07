# 欲买桂花同载酒，
# 终不似、少年游。
# Copyright (c) VernonSong. All rights reserved.
# ======================================================================================================================
from typing import Dict, Any, List
from uuid import uuid4
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from datalake.core.workflow.models import WorkflowState, WorkflowConfig, LakeIngestionRequest
from datalake.core.nodes import NODE_MAPPING


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
        # 对于table_check节点，我们使用一个统一的条件函数来处理所有条件分支
        table_check_edges = [edge for edge in config.edges if edge['start'] == 'table_check' and edge.get('condition')]
        if table_check_edges:
            # 定义统一的条件函数
            def table_check_conditional(state):
                table_check_result = state.get('results', {}).get('table_check', {})
                status = table_check_result.get('status')

                # 根据状态返回不同的分支
                if status == 'failed':
                    # 找到失败分支的目标节点
                    failed_edge = next(e for e in table_check_edges if e['condition']['type'] == 'table_check_failed')
                    return failed_edge['end']
                elif status == 'success':
                    # 找到成功分支的目标节点
                    success_edge = next(e for e in table_check_edges if e['condition']['type'] == 'table_check_passed')
                    return success_edge['end']
                else:
                    return None

            # 创建分支映射
            branches = {}
            for edge in table_check_edges:
                branches[edge['end']] = edge['end']

            # 添加条件边
            workflow.add_conditional_edges(
                'table_check',
                table_check_conditional,
                branches
            )

        # 处理其他节点的条件边
        other_condition_edges = [edge for edge in config.edges if
                                 edge.get('condition') and edge['start'] != 'table_check']
        for edge in other_condition_edges:
            start = edge["start"]
            end = edge["end"]
            condition = edge['condition']

            # 定义条件函数
            def condition_func(state, cond=condition):
                if cond.get('type') == 'equals':
                    # 等于条件
                    return state.get(cond.get('field')) == cond.get('value')
                elif cond.get('type') == 'not_equals':
                    # 不等于条件
                    return state.get(cond.get('field')) != cond.get('value')
                else:
                    # 默认返回 True
                    return True

            # 添加条件边
            workflow.add_conditional_edges(
                start,
                condition_func,
                {True: end}
            )

        # 2. 处理其他边（非条件边）
        for edge in config.edges:
            start = edge["start"]
            end = edge["end"]

            # 跳过已经处理过的条件边
            if edge.get('condition'):
                continue

            if edge.get('parallel'):
                # 并行边 - 同时执行多个节点
                workflow.add_edge(start, end)
            elif edge.get('loop'):
                # 循环边
                workflow.add_edge(start, end)
            else:
                # 普通边
                workflow.add_edge(start, end)

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

        # 执行工作流
        result = workflow.invoke(
            initial_state,
            config={"configurable": {"thread_id": request_id}}
        )

        # 返回结果
        return {
            "request_id": request_id,
            "status": result.get("status", "completed"),
            "results": result.get("results", {}),
            "errors": result.get("errors", []),
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