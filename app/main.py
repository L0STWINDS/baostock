from fastapi import FastAPI
import uvicorn
from app.api import health, candlestick, indicator

# 创建FastAPI应用
app = FastAPI(
    title="Stock data query API",
    description="股票数据查询API",
    version="0.0.1"
)

# 注册路由
app.include_router(health.router)
app.include_router(candlestick.router)
app.include_router(indicator.router)

def main():
    # 打印版本信息
    print(f"Quant trading system starting... Version: {app.version}")
    # 启动FastAPI应用
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()