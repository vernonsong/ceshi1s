# 欲买桂花同载酒，
# 终不似、少年游。
# Copyright (c) VernonSong. All rights reserved.
# ======================================================================================================================
"""
数据湖服务主入口
"""

from datalake.server import server


if __name__ == "__main__":
    """
    启动数据湖服务
    """
    print("Starting DataLake Server...")
    print("API Documentation: http://localhost:8000/docs")
    print("ReDoc Documentation: http://localhost:8000/redoc")
    server.run(host="0.0.0.0", port=8000, reload=True)