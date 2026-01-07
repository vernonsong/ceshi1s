# 欲买桂花同载酒，
# 终不似、少年游。
# Copyright (c) VernonSong. All rights reserved.
# ======================================================================================================================
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datalake.api.routes import router
from datalake.core.workflow.workflow_manager import WorkflowManager
from datalake.core.workflow.models import WorkflowConfig


class DataLakeServer:
    """
    数据湖服务启动类
    """
    
    def __init__(self):
        """
        初始化数据湖服务
        """
        self.app = FastAPI(
            title="DataLake API",
            description="数据湖服务API",
            version="1.0.0",
            docs_url="/docs",
            redoc_url="/redoc"
        )
        
        # 配置CORS
        self._configure_cors()
        
        # 注册路由
        self._register_routes()
    
    def _configure_cors(self):
        """
        配置CORS中间件
        """
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],  # 在生产环境中应该设置具体的域名
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    
    def _register_routes(self):
        """
        注册API路由
        """
        self.app.include_router(router, prefix="/api")

    
    def run(self, host="0.0.0.0", port=8000, reload=False):
        """
        启动服务
        
        Args:
            host: 服务主机地址
            port: 服务端口
            reload: 是否启用热重载
        """
        uvicorn.run(
            "datalake.server:app",
            host=host,
            port=port,
            reload=reload,
            workers=1
        )


# 创建服务实例
server = DataLakeServer()

# 导出应用实例，供uvicorn使用
app = server.app


if __name__ == "__main__":
    # 直接运行时启动服务
    server.run(host="0.0.0.0", port=8000, reload=True)