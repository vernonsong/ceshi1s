# 入湖系统工作流编排Demo

## 项目概述

本项目是一个基于langgraph的工作流编排Demo，用于验证入湖系统原子能力的编排可行性。项目提供了入湖系统所需的各种原子能力节点，用户可以根据需要自由编排这些节点，形成固定的工作流，每次入湖时只需选择相应的工作流并提供原始输入即可。

## 项目架构

### 核心组件

1. **模型层** (`models.py`)
   - 定义了工作流配置、入湖请求、工作流状态等数据模型
   - 支持自定义节点配置参数

2. **节点层** (`nodes.py`)
   - 实现了14个入湖系统原子能力节点
   - 每个节点对应入湖流程中的一个步骤

3. **工作流管理层** (`workflow_manager.py`)
   - 负责工作流的注册、执行和管理
   - 支持工作流的创建、更新、删除和查询

4. **演示层** (`demo.py`)
   - 展示工作流的注册和执行过程
   - 提供示例数据和配置

### 原子能力节点

| 节点名称 | 功能描述 | 可配置参数 |
|---------|---------|-----------|
| excel_import | Excel入湖单导入 | - |
| page_submit | 页面提单 | - |
| table_check | 入湖表检查 | 检查规则列表 |
| sql_generate | SQL生成 | SQL模板 |
| sql_execute | SQL执行 | - |
| integration_task_generate | 集成任务生成 | 并行度 |
| integration_task_deploy | 集成任务部署 | - |
| schedule_task_generate | 调度任务生成 | 调度 cron 表达式 |
| schedule_task_deploy | 调度任务部署 | - |
| metadata_register | 元数据注册 | - |
| init_task_generate | 初始化任务生成 | 批处理大小 |
| init_task_deploy | 初始化任务部署 | - |
| init_task_execute | 初始化任务执行 | - |
| llm | 大模型分析 | 模型名称 |

## 环境要求

- Python 3.9+
- langgraph
- langchain
- python-dotenv
- pydantic

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

### 1. 注册工作流

```python
from workflow_manager import WorkflowManager
from models import WorkflowConfig

# 初始化工作流管理器
workflow_manager = WorkflowManager()

# 定义工作流配置
workflow_config = WorkflowConfig(
    name="standard_lake_ingestion",
    description="标准入湖工作流",
    nodes=[
        "excel_import",
        "page_submit",
        "table_check",
        # ... 其他节点
    ],
    edges=[
        {"start": "excel_import", "end": "page_submit"},
        {"start": "page_submit", "end": "table_check"},
        # ... 其他边
    ],
    node_configs={
        "table_check": {
            "table_check_rules": ["not_null", "data_type", "primary_key"]
        },
        "integration_task_generate": {
            "integration_parallelism": 2
        },
        # ... 其他节点配置
    }
)

# 注册工作流
workflow_manager.register_workflow(workflow_config)
```

### 2. 执行工作流

```python
from models import LakeIngestionRequest

# 定义入湖请求
lake_request = LakeIngestionRequest(
    workflow_name="standard_lake_ingestion",
    source_data={
        "tables": [
            {
                "name": "user_info",
                "columns": [
                    {"name": "user_id", "type": "int", "primary_key": True},
                    {"name": "user_name", "type": "string", "not_null": True},
                    # ... 其他列
                ]
            }
        ],
        "submit_user": "demo_user",
        "data_sample": [
            # ... 示例数据
        ]
    },
    custom_params={
        "extra_info": "This is a demo request"
    }
)

# 执行工作流
result = workflow_manager.execute_workflow(lake_request)
```

## 运行演示

```bash
python demo.py
```

## 工作流执行流程

1. 接收入湖请求，选择对应的工作流
2. 初始化工作流状态
3. 按照工作流配置的节点顺序执行每个原子能力
4. 每个节点执行完成后，将结果传递给下一个节点
5. 所有节点执行完成后，返回最终结果
6. 支持工作流状态的持久化和恢复

## 扩展能力

1. **添加新节点**：在 `nodes.py` 中实现新的节点函数，并添加到 `NODE_MAPPING` 中
2. **自定义工作流**：根据业务需求，自由组合节点形成新的工作流
3. **扩展配置参数**：在 `NodeConfig` 模型中添加新的配置字段
4. **集成外部系统**：每个节点可以集成外部系统的API或服务

## 优势

1. **灵活性**：支持自由编排原子能力，适应不同的入湖场景
2. **可配置性**：每个节点支持自定义参数，满足不同的业务需求
3. **可扩展性**：支持添加新的原子能力节点
4. **可视化**：基于langgraph，支持工作流的可视化展示
5. **可复用性**：编排好的工作流可以重复使用，提高效率
6. **可监控性**：支持工作流执行状态的监控和追踪

## 应用场景

1. **标准化入湖流程**：定义标准的入湖工作流，确保所有入湖操作遵循统一的流程和规则
2. **定制化入湖需求**：根据不同的业务需求，定制不同的工作流
3. **自动化入湖操作**：通过工作流自动化执行入湖流程，减少人工干预
4. **批量入湖**：支持批量执行入湖操作，提高效率
5. **复杂入湖场景**：处理包含多个系统和步骤的复杂入湖场景

## 技术栈

- **Python**：主要开发语言
- **langgraph**：工作流编排框架
- **langchain**：大模型集成框架
- **pydantic**：数据模型验证
- **dotenv**：环境变量管理

## 未来规划

1. 支持工作流的可视化编辑
2. 支持工作流执行的实时监控和日志查询
3. 支持工作流的版本管理
4. 支持工作流的权限控制
5. 支持更多的原子能力节点
6. 支持与更多外部系统的集成
