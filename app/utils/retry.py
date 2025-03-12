from functools import wraps
import asyncio
from typing import Callable, Any
import logging

logger = logging.getLogger(__name__)

def async_retry_with_timeout(timeout_seconds: int = 60, max_retries: int = 3):
    """
    异步函数超时重试装饰器
    
    参数:
        timeout_seconds: 超时时间（秒）
        max_retries: 最大重试次数
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            task = None  # 初始化task变量
            for attempt in range(max_retries):
                try:
                    # 创建一个任务
                    task = asyncio.create_task(func(*args, **kwargs))
                    # 等待任务完成，设置超时时间
                    result = await asyncio.wait_for(task, timeout=timeout_seconds)
                    return result
                except asyncio.TimeoutError:
                    logger.warning(f"函数 {func.__name__} 执行超时 (尝试 {attempt + 1}/{max_retries})")
                    if attempt == max_retries - 1:
                        logger.error(f"函数 {func.__name__} 达到最大重试次数")
                        raise
                    # 取消当前任务
                    if task:  # 确保task已定义再执行cancel
                        task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                except Exception as e:
                    logger.error(f"函数 {func.__name__} 执行出错: {str(e)}")
                    raise
            return None
        return wrapper
    return decorator