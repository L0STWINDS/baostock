from fastapi import APIRouter
from app.utils.retry import async_retry_with_timeout
router = APIRouter(tags=["健康检查"])

@router.get("/health")
@async_retry_with_timeout()  # 添加装饰器
async def health_check():
    """
    健康检查接口，用于验证API服务是否正常运行
    """
    return {
        "status": "ok",
        "message": "服务运行正常"
    }