from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from datalake.core.workflow.models import register_node, NodeOutputParameter, NodeInputParameter, NodeMetadata
import os
from dotenv import load_dotenv
from typing import Any, Dict, List, Optional
from langchain_openai import OpenAI
from langchain_core.prompts import PromptTemplate
import json

# 加载环境变量
load_dotenv()

# 定义输入参数结构
class SourceField(BaseModel):
    """源表字段信息"""
    name: str = Field(..., description="字段名称")
    type: str = Field(..., description="字段类型")
    length: Optional[int] = Field(None, description="字段长度")
    precision: Optional[int] = Field(None, description="字段精度")
    nullable: bool = Field(..., description="是否可空")
    primary_key: bool = Field(..., description="是否为主键")
    comment: Optional[str] = Field(None, description="字段注释")

class SQLGenerateInput(BaseModel):
    """SQL生成节点输入参数"""
    source_db_type: str = Field(..., description="源表数据库类型")
    source_fields: List[SourceField] = Field(..., description="源表字段列表")
    lake_db_type: str = Field(..., description="湖库类型")
    lake_schema: str = Field(..., description="湖库schema")
    lake_table: str = Field(..., description="湖库表名")

# 定义输出参数结构
class SQLGenerateResult(BaseModel):
    """SQL生成结果"""
    status: str = Field(..., description="生成状态")
    generated_sql: str = Field(..., description="生成的SQL语句")
    sql_type: str = Field(..., description="SQL类型")
    execution_plan: Optional[str] = Field(None, description="执行计划")


# SQL生成节点
@register_node(
    name="sql_generate",
    description="SQL生成节点，根据源表信息和湖库信息生成湖表的建表DDL",
    type="task",
    inputs=[
        {"name": "source_db_type", "description": "源表数据库类型", "data_type": "string", "required": True},
        {"name": "source_fields", "description": "源表字段列表", "data_type": "list", "required": True},
        {"name": "lake_db_type", "description": "湖库类型", "data_type": "string", "required": True},
        {"name": "lake_schema", "description": "湖库schema", "data_type": "string", "required": True},
        {"name": "lake_table", "description": "湖库表名", "data_type": "string", "required": True},
    ],
    outputs=[
        NodeOutputParameter(
            name="status",
            description="生成状态",
            data_type="string"
        ),
        NodeOutputParameter(
            name="generated_sql",
            description="生成的SQL语句",
            data_type="string"
        ),
        NodeOutputParameter(
            name="sql_type",
            description="SQL类型",
            data_type="string"
        ),
        NodeOutputParameter(
            name="execution_plan",
            description="执行计划",
            data_type="string"
        )
    ],
    category="transformation"
)
def sql_generate_node(state: Dict[str, Any]) -> Dict[str, Any]:
    print(f"Executing SQL Generate Node for request: {state.get('request_id')}")
    
    # 从状态中获取输入参数
    results = state.get("results", {})
    source_data = state.get("source_data", {})
    
    # 收集输入参数
    # 从不同节点获取必要信息
    source_db_type = None
    source_fields = []
    lake_db_type = None
    lake_schema = None
    lake_table = None
    
    # 从数据库类型查询节点获取源表数据库类型
    if "db_type_query" in results:
        db_type_result = results["db_type_query"]
        source_db_type = db_type_result.get("db_type")
    
    # 从表字段查询节点获取源表字段列表
    if "table_field_query" in results:
        table_field_result = results["table_field_query"]
        source_fields = table_field_result.get("fields", [])
    
    # 从源数据和结果中获取湖库信息
    if "page_submit" in results:
        page_submit_result = results["page_submit"]
        lake_schema = page_submit_result.get("lake_schema")
        lake_table = page_submit_result.get("lake_table")
    
    # 如果没有从结果中获取到，尝试从source_data中获取
    if not source_db_type:
        source_db_type = source_data.get("source_db_type", "mysql")
    
    if not lake_db_type:
        lake_db_type = source_data.get("lake_db_type", "hive")
    
    if not lake_schema:
        lake_schema = source_data.get("lake_schema", "default")
    
    if not lake_table:
        lake_table = source_data.get("lake_table", "target_table")
    
    # 如果源字段列表为空，使用默认模拟数据
    if not source_fields:
        source_fields = [
            {"name": "id", "type": "int", "length": 11, "precision": None, "nullable": False, "primary_key": True, "comment": "主键"},
            {"name": "name", "type": "varchar", "length": 50, "precision": None, "nullable": True, "primary_key": False, "comment": "名称"},
            {"name": "age", "type": "int", "length": 3, "precision": None, "nullable": True, "primary_key": False, "comment": "年龄"},
            {"name": "create_time", "type": "datetime", "length": None, "precision": None, "nullable": False, "primary_key": False, "comment": "创建时间"}
        ]
    
    try:
        # 准备大模型调用参数
        aliyun_key = os.getenv("ALIYUN_KEY")
        if not aliyun_key:
            raise ValueError("ALIYUN_KEY not found in environment variables")
        
        # 构建大模型提示词
        fields_str = "\n".join([
            f"- {field['name']} {field['type']} {field.get('length', '')} {field.get('precision', '')} {field.get('comment', '')}{' 主键' if field.get('primary_key') else ''}"
            for field in source_fields
        ])
        
        prompt = f"""
我需要生成湖库表的建表DDL语句，具体要求如下：

源表信息：
- 数据库类型：{source_db_type}
- 字段列表：
{fields_str}

湖库信息：
- 湖库类型：{lake_db_type}
- Schema：{lake_schema}
- 表名：{lake_table}

请生成符合湖库语法的建表DDL语句，需要：
1. 根据源表字段信息创建湖库表结构
2. 进行适当的字段类型映射（源表到湖库）
3. 添加合理的表注释和字段注释
4. 配置符合湖库最佳实践的存储格式、分区策略等
5. 如果是Hive表，考虑ORC或Parquet格式
6. 对于主键字段：
   - 如果湖库类型支持主键约束（如Spark SQL、Iceberg等），请显式添加PRIMARY KEY定义
   - 如果是Hive表（对主键支持有限），请在表属性或注释中明确标记主键字段

请返回以下格式的JSON：
{{
  "generated_sql": "生成的建表DDL语句",
  "sql_type": "create_table",
  "execution_plan": "建表执行计划描述"
}}
"""
        
        print("调用阿里云大模型生成SQL...")
        print(f"提示词：{prompt[:200]}...")
        
        # 调用阿里云大模型API
        # 使用langchain调用阿里云大模型
        try:
            from langchain_openai import ChatOpenAI
            from langchain_core.prompts import ChatPromptTemplate
            from langchain_core.messages import HumanMessage
            
            # 初始化ChatOpenAI客户端
            llm = ChatOpenAI(
                model="qwen-plus",
                api_key=aliyun_key,
                base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
                temperature=0.3,
                max_tokens=1000
            )
            
            # 直接使用HumanMessage创建提示，避免模板变量解析问题
            messages = [HumanMessage(content=prompt)]
            prompt_template = ChatPromptTemplate.from_messages(messages)
            
            # 构建chain
            chain = prompt_template | llm
            
            # 调用模型生成SQL
            print(f"调用langchain生成SQL，模型: qwen-plus...")
            response = chain.invoke({})
            
            # 查看完整响应内容
            print(f"大模型完整响应: {response}")
            
            # 处理模型响应
            generated_sql = None
            sql_type = "create_table"
            execution_plan = None
            
            if response.content:
                try:
                    response_content = response.content.strip()
                    # 移除可能的代码块标记
                    if response_content.startswith('```json'):
                        response_content = response_content[7:]
                    if response_content.endswith('```'):
                        response_content = response_content[:-3]
                    response_content = response_content.strip()
                    model_result = json.loads(response_content)
                    generated_sql = model_result.get("generated_sql")
                    sql_type = model_result.get("sql_type", "insert")
                    execution_plan = model_result.get("execution_plan")
                except json.JSONDecodeError as e:
                    print(f"JSON解析异常: {e}")
                    print(f"响应内容: {repr(response.content)}")
            
            if generated_sql is None or generated_sql.strip() == "":
                # 大模型调用失败，直接抛出异常
                raise ValueError("大模型调用失败，未生成有效的SQL语句")
                
        except Exception as e:
            # 大模型调用异常，直接抛出
            raise Exception(f"大模型调用异常: {str(e)}") from e
        
        # 创建结果对象
        result = SQLGenerateResult(
            status="success",
            generated_sql=generated_sql,
            sql_type=sql_type,
            execution_plan=execution_plan
        )
        
        return {
            "request_id": state.get('request_id'),
            "workflow_config": state.get('workflow_config'),
            "source_data": state.get('source_data'),
            "results": {
                **state.get('results', {}),
                "sql_generate": result.model_dump()
            },
            "current_node": "sql_generate"
        }
        
    except Exception as e:
        print(f"SQL生成失败: {str(e)}")
        return {
            "request_id": state.get('request_id'),
            "workflow_config": state.get('workflow_config'),
            "source_data": state.get('source_data'),
            "results": {
                **state.get('results', {}),
                "sql_generate": {
                    "status": "failed",
                    "generated_sql": "",
                    "sql_type": "",
                    "execution_plan": f"生成失败: {str(e)}"
                }
            },
            "current_node": "sql_generate"
        }


# SQL生成节点元数据
sql_generate_metadata = NodeMetadata(
    name="sql_generate",
    description="SQL生成节点，根据源表信息和湖库信息生成湖表的建表DDL",
    inputs=[
        NodeInputParameter(
            name="source_db_type",
            description="源表数据库类型",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="source_fields",
            description="源表字段列表",
            data_type="list",
            required=True
        ),
        NodeInputParameter(
            name="lake_db_type",
            description="湖库类型",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="lake_schema",
            description="湖库schema",
            data_type="string",
            required=True
        ),
        NodeInputParameter(
            name="lake_table",
            description="湖库表名",
            data_type="string",
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
            name="generated_sql",
            description="生成的SQL语句",
            data_type="string"
        ),
        NodeOutputParameter(
            name="sql_type",
            description="SQL类型",
            data_type="string"
        ),
        NodeOutputParameter(
            name="execution_plan",
            description="执行计划",
            data_type="string"
        )
    ],
    version="1.0.0"
)