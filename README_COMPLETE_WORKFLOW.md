# 完整工作流流程文档

## 1. 概述

本项目实现了一个完整的工作流生成和验证流程，包括：
- 用户需求输入
- 工作流编排智能体
- 验收智能体
- 循环验证机制

## 2. 核心组件

### 2.1 完整工作流流程 (CompleteWorkflowProcess)

**文件**：`complete_workflow_process.py`

**功能**：管理完整的工作流生成和验证流程

**主要方法**：
- `run()`: 运行完整工作流流程
- `_generate_workflow()`: 生成工作流（支持实际workflow_agent和模拟模式）
- `_validate_workflow()`: 验证工作流（拆解、计划、执行）
- `_clean_test_environment()`: 清空测试环境
- `_parse_验收_criteria()`: 拆解验收标准
- `_generate_test_plan()`: 生成测试计划
- `_execute_test_plan()`: 执行测试计划
- `_generate_验收_report()`: 生成验收报告
- `_generate_feedback()`: 生成反馈给编排智能体
- `_update_requirement()`: 更新用户需求

### 2.2 工作流编排智能体 (WorkflowAgent)

**文件**：`datalake/core/agents/workflow_agent.py`

**功能**：根据用户需求生成工作流JSON

**主要方法**：
- `generate_workflow_json()`: 根据用户需求生成工作流JSON
- `_build_prompt_template()`: 构建提示词模板
- `validate_workflow_json()`: 验证工作流JSON的有效性
- `optimize_workflow_json()`: 优化工作流JSON

### 2.3 验证工具 (ValidationTools)

**文件**：`datalake/services/validation_tools.py`

**功能**：提供工作流验证所需的工具

**主要工具**：
- `delete_table()`: 删除表
- `delete_integration_task()`: 删除集成任务
- `query_integration_task()`: 查询集成任务
- `get_table_ddl()`: 获取表DDL

## 3. 工作流程

### 3.1 完整工作流流程

```
用户输入 → 编排智能体 → 生成工作流 → 验收智能体 → 清空测试环境 → 拆解验收标准 → 生成测试计划 → 执行测试 → 生成验收报告
       ↓                                                           ↑
       └───────────────────────────────────────────────────────────┘
                     失败时重新编排（最多n次）
```

### 3.2 循环验证机制

1. 用户输入：需求、验收标准、测试用例
2. 编排智能体生成工作流
3. 验收智能体验证工作流
   - 清空测试环境
   - 拆解验收标准
   - 生成测试计划
   - 执行测试
   - 生成验收报告
4. 判断结果
   - 成功：输出工作流
   - 失败：生成反馈，更新需求，重新编排
5. 最大循环次数：3次

## 4. 使用方法

### 4.1 运行完整工作流流程

```python
from complete_workflow_process import CompleteWorkflowProcess

# 1. 准备用户需求、验收标准和测试用例
user_requirement = "创建一个数据集成工作流，从源表获取数据并创建Hive表"

验收_criteria = """
1. 工作流必须包含表检查、SQL生成和SQL执行三个节点
2. 工作流必须能够正确调用验证工具
3. 工作流执行结果必须符合预期
4. 工作流结构完整，包含节点、边、起始和结束节点
"""

test_cases = [
    {
        "test_case_id": "TC_001",
        "name": "表检查节点验证",
        "description": "验证工作流包含table_check节点",
        "expected_result": "节点存在且配置正确"
    },
    {
        "test_case_id": "TC_002",
        "name": "SQL执行验证",
        "description": "验证SQL执行节点能够正确执行建表语句",
        "expected_result": "表创建成功"
    }
]

# 2. 创建并运行完整流程
process = CompleteWorkflowProcess(max_iterations=3)
result = process.run(user_requirement, 验收_criteria, test_cases)

# 3. 输出结果
print(f"\n=== 最终结果 ===")
print(f"成功：{result['success']}")
if result['success']:
    print(f"工作流：{json.dumps(result['workflow'], ensure_ascii=False, indent=2)}")
else:
    print(f"失败原因：{result['last_validation_result']['message']}")
```

### 4.2 运行示例

```bash
python3 complete_workflow_process.py
```

## 5. 扩展方法

### 5.1 扩展工作流编排智能体

1. 修改 `datalake/core/agents/workflow_agent.py`
2. 添加新的节点类型支持
3. 优化提示词模板

### 5.2 扩展验证工具

1. 修改 `datalake/services/validation_tools.py`
2. 添加新的工具函数
3. 更新工具注册表

### 5.3 扩展完整工作流流程

1. 修改 `complete_workflow_process.py`
2. 添加新的测试步骤
3. 优化验证逻辑

## 6. 环境配置

### 6.1 安装依赖

```bash
# 使用pip安装依赖
pip install langchain langchain-openai openai pydantic

# 或使用项目的依赖管理
pip install -e .
```

### 6.2 环境变量

创建 `.env` 文件，配置必要的环境变量：

```
OPENAI_API_KEY=your_openai_api_key
```

## 7. 故障排除

### 7.1 依赖问题

如果遇到依赖问题（如 `No module named 'langchain_openai'`）：

```bash
# 确保安装了所有依赖
pip install langchain langchain-openai openai pydantic
```

### 7.2 SSL证书问题

如果遇到SSL证书问题：

```bash
# 对于macOS
pip install certifi
```

### 7.3 工作流生成失败

如果工作流生成失败：

1. 检查用户需求是否清晰
2. 检查大模型配置是否正确
3. 检查节点类型是否支持

## 8. 示例工作流

### 8.1 简单数据集成工作流

```json
{
  "nodes": [
    {
      "id": "table_check",
      "type": "table_check",
      "name": "检查表是否存在",
      "inputs": [
        {
          "name": "database_name",
          "source_type": "raw_input",
          "input_key": "source_database"
        },
        {
          "name": "table_name",
          "source_type": "raw_input",
          "input_key": "source_table"
        }
      ]
    },
    {
      "id": "sql_generate",
      "type": "sql_generate",
      "name": "生成建表SQL",
      "inputs": [
        {
          "name": "database_name",
          "source_type": "raw_input",
          "input_key": "source_database"
        },
        {
          "name": "table_name",
          "source_type": "raw_input",
          "input_key": "source_table"
        }
      ]
    },
    {
      "id": "sql_execute",
      "type": "sql_execute",
      "name": "执行建表SQL",
      "inputs": [
        {
          "name": "database_name",
          "source_type": "raw_input",
          "input_key": "source_database"
        },
        {
          "name": "sql",
          "source_type": "node_output",
          "node_id": "sql_generate",
          "output_field": "generated_sql"
        }
      ]
    }
  ],
  "edges": [
    {
      "source": "table_check",
      "target": "sql_generate"
    },
    {
      "source": "sql_generate",
      "target": "sql_execute"
    }
  ],
  "start_node": "table_check",
  "end_nodes": ["sql_execute"]
}
```

## 9. 未来计划

1. 集成更多类型的节点
2. 优化工作流生成算法
3. 添加更多验证工具
4. 支持更复杂的工作流逻辑
5. 提供Web界面

## 10. 贡献

欢迎贡献代码和提出建议！