#!/usr/bin/env python3

"""
测试sql_generate节点
"""

from datalake.core.nodes.sql_generate import sql_generate_node

def test_sql_generate_node():
    """测试SQL生成节点"""
    print("测试SQL生成节点开始...")
    
    # 准备测试状态
    test_state = {
        'request_id': 'test123',
        'source_data': {
            'source_db_type': 'mysql',
            'lake_db_type': 'hive',
            'lake_schema': 'test_schema',
            'lake_table': 'test_table'
        },
        'results': {
            'db_type_query': {
                'db_type': 'mysql'
            },
            'table_field_query': {
                'fields': [
                    {
                        'name': 'id',
                        'type': 'int',
                        'length': 11,
                        'precision': None,
                        'nullable': False,
                        'primary_key': True,
                        'comment': '主键'
                    },
                    {
                        'name': 'name',
                        'type': 'varchar',
                        'length': 50,
                        'precision': None,
                        'nullable': True,
                        'primary_key': False,
                        'comment': '名称'
                    },
                    {
                        'name': 'age',
                        'type': 'int',
                        'length': 3,
                        'precision': None,
                        'nullable': True,
                        'primary_key': False,
                        'comment': '年龄'
                    },
                    {
                        'name': 'create_time',
                        'type': 'datetime',
                        'length': None,
                        'precision': None,
                        'nullable': False,
                        'primary_key': False,
                        'comment': '创建时间'
                    }
                ]
            },
            'page_submit': {
                'lake_schema': 'test_schema',
                'lake_table': 'test_table'
            }
        }
    }
    
    try:
        # 调用SQL生成节点
        result = sql_generate_node(test_state)
        
        print("\n测试结果:")
        print(f"状态: {result['results']['sql_generate']['status']}")
        print(f"SQL类型: {result['results']['sql_generate']['sql_type']}")
        print(f"生成的SQL:")
        print(result['results']['sql_generate']['generated_sql'])
        print(f"执行计划:")
        print(result['results']['sql_generate']['execution_plan'])
        
        # 检查生成状态
        if result['results']['sql_generate']['status'] == 'failed':
            print("\n测试失败! SQL生成失败")
            return False
        else:
            print("\n测试通过!")
            return True
    except Exception as e:
        print(f"\n测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_sql_generate_node()
    exit(0 if success else 1)