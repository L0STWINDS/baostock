from fastapi import APIRouter, Body
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import baostock as bs
from typing import Dict, Any, Optional
from pydantic import BaseModel
import logging
from app.utils.retry import async_retry_with_timeout

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

router = APIRouter(tags=["技术指标"])

class KDJRequest(BaseModel):
    code: str

@router.post("/indicator/kdj/weekly")
@async_retry_with_timeout()  # 添加装饰器
async def get_weekly_kdj(request: KDJRequest = Body(...)):
    """
    计算并返回指定股票的周线KDJ值
    
    请求体示例:
    {
        "code": "sh.600000"
    }
    """
    logger.info(f"开始处理KDJ请求: {request.code}")
    
    # 计算前推180天的日期
    today = datetime.now()
    start_date = (today - timedelta(days=180)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    logger.info(f"查询日期范围: {start_date} 至 {end_date}")
    
    # 获取日K线数据
    logger.info("开始获取日K线数据...")
    daily_data = get_daily_data(request.code, start_date, end_date)
    
    if not daily_data:
        logger.error("获取日K线数据失败: 返回数据为空")
        return {"error": "获取日K线数据失败: 返回数据为空"}
    
    if isinstance(daily_data, dict) and "error" in daily_data:
        logger.error(f"获取日K线数据失败: {daily_data['error']}")
        return daily_data
    
    logger.info(f"成功获取日K线数据，数据条数: {len(daily_data)}")
    
    # 将日K线数据转换为DataFrame
    try:
        df = pd.DataFrame(daily_data)
        logger.info(f"DataFrame列: {df.columns.tolist()}")
        
        # 转换数据类型
        for col in ['open', 'high', 'low', 'close', 'preclose']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col])
            else:
                logger.error(f"列 '{col}' 不在DataFrame中")
                return {"error": f"数据格式错误: 缺少列 '{col}'"}
        
        # 设置日期索引
        if 'date' not in df.columns:
            logger.error("列 'date' 不在DataFrame中")
            return {"error": "数据格式错误: 缺少列 'date'"}
            
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        # 将日K线数据转换为周K线数据
        logger.info("开始转换为周K线数据...")
        weekly_df = df.resample('W').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'code': 'first'
        })
        logger.info(f"周K线数据条数: {len(weekly_df)}")
        
        # 计算KDJ指标
        logger.info("开始计算KDJ指标...")
        kdj_df = calculate_kdj(weekly_df)
        logger.info(f"KDJ计算完成，数据条数: {len(kdj_df)}")
        
        # 获取最新的KDJ值
        latest_kdj = kdj_df.iloc[-1].to_dict()
        
        # 处理nan值，将其替换为None，这样在JSON序列化时会变成null
        for key in ['K', 'D', 'J']:
            if pd.isna(latest_kdj[key]):
                latest_kdj[key] = None
                logger.warning(f"检测到{key}值为NaN，已替换为None")
        
        # 构造返回结果，只有在值不为None时才进行四舍五入
        result = {
            "code": request.code,
            "date": kdj_df.index[-1].strftime("%Y-%m-%d"),
            "k": round(latest_kdj['K'], 2) if latest_kdj['K'] is not None else None,
            "d": round(latest_kdj['D'], 2) if latest_kdj['D'] is not None else None,
            "j": round(latest_kdj['J'], 2) if latest_kdj['J'] is not None else None
        }
        
        logger.info(f"返回结果: {result}")
        return result
    
    except Exception as e:
        logger.exception(f"处理数据时发生错误: {str(e)}")
        return {"error": f"处理数据时发生错误: {str(e)}"}

def get_daily_data(code: str, start_date: str, end_date: str):
    """
    直接使用baostock获取日K线数据
    """
    try:
        logger.info(f"使用baostock获取数据: 代码={code}, 开始日期={start_date}, 结束日期={end_date}")
        
        # 登录系统
        lg = bs.login()
        if lg.error_code != '0':
            logger.error(f"baostock登录失败: {lg.error_msg}")
            return {"error": f"baostock登录失败: {lg.error_msg}"}
        
        try:
            # 查询历史数据
            fields = "date,code,open,high,low,close,preclose"
            rs = bs.query_history_k_data_plus(
                code, fields,
                start_date=start_date, 
                end_date=end_date,
                frequency="d",  # 日线
                adjustflag="2"  # 前复权
            )
            
            if rs.error_code != '0':
                logger.error(f"baostock查询失败: {rs.error_msg}")
                return {"error": f"baostock查询失败: {rs.error_msg}"}
            
            # 处理结果集
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            # 转换为DataFrame
            result = pd.DataFrame(data_list, columns=rs.fields)
            
            # 转换为字典列表
            data = result.to_dict('records')
            logger.info(f"成功获取数据，条数: {len(data)}")
            return data
            
        finally:
            # 确保登出系统
            bs.logout()
    
    except Exception as e:
        logger.exception(f"获取日K线数据失败: {str(e)}")
        return {"error": f"获取日K线数据失败: {str(e)}"}

def calculate_kdj(df, n=9, m1=3, m2=3):
    """
    计算KDJ指标
    
    参数:
    df: DataFrame, 包含OHLC数据的DataFrame
    n: int, RSV的周期
    m1: int, K值的周期
    m2: int, D值的周期
    
    返回:
    DataFrame, 包含KDJ值的DataFrame
    """
    df = df.copy()
    
    # 添加调试信息，检查输入数据
    logger.info(f"KDJ计算输入数据形状: {df.shape}")
    logger.info(f"KDJ计算输入数据前5行: \n{df.head().to_string()}")
    logger.info(f"KDJ计算输入数据后5行: \n{df.tail().to_string()}")
    
    # 检查是否有缺失值
    missing_values = df[['high', 'low', 'close']].isna().sum()
    logger.info(f"输入数据缺失值统计: {missing_values.to_dict()}")
    
    # 计算n日内的最高价和最低价
    df['high_n'] = df['high'].rolling(n).max()
    df['low_n'] = df['low'].rolling(n).min()
    
    # 检查rolling计算结果
    logger.info(f"Rolling计算后缺失值统计: high_n={df['high_n'].isna().sum()}, low_n={df['low_n'].isna().sum()}")
    
    # 计算RSV，添加更多错误处理
    denominator = df['high_n'] - df['low_n']
    zero_denominator = (denominator == 0).sum()
    logger.info(f"分母为零的数量: {zero_denominator}")
    
    # 安全地计算RSV
    df['RSV'] = np.where(
        denominator != 0,
        100 * (df['close'] - df['low_n']) / denominator,
        50  # 当最高价等于最低价时，使用默认值50
    )
    
    # 对前n个值（无法计算rolling的）使用NaN
    df.loc[df.index[:n-1], 'RSV'] = np.nan
    
    logger.info(f"RSV计算后的缺失值数量: {df['RSV'].isna().sum()}")
    logger.info(f"RSV值范围: 最小={df['RSV'].min()}, 最大={df['RSV'].max()}")
    
    # 初始化K、D值
    df['K'] = 50.0
    df['D'] = 50.0
    
    # 计算K、D值
    valid_indices = df.index[~df['RSV'].isna()]
    logger.info(f"有效RSV值的数量: {len(valid_indices)}")
    
    if len(valid_indices) > 0:
        # 确保第一个有效值使用初始值50
        first_valid_idx = df.index.get_loc(valid_indices[0])
        if first_valid_idx > 0:
            df.loc[valid_indices[0], 'K'] = (m1-1) / m1 * 50 + 1 / m1 * df.loc[valid_indices[0], 'RSV']
            df.loc[valid_indices[0], 'D'] = (m2-1) / m2 * 50 + 1 / m2 * df.loc[valid_indices[0], 'K']
        
        # 计算其余K、D值
        for i in range(1, len(df)):
            if i >= len(df) or i-1 >= len(df):
                logger.warning(f"索引越界: i={i}, df长度={len(df)}")
                continue
                
            if pd.isna(df.iloc[i]['RSV']):
                logger.debug(f"跳过索引 {i}，因为RSV为NaN")
                continue
                
            try:
                df.loc[df.index[i], 'K'] = (m1-1) / m1 * df.loc[df.index[i-1], 'K'] + 1 / m1 * df.loc[df.index[i], 'RSV']
                df.loc[df.index[i], 'D'] = (m2-1) / m2 * df.loc[df.index[i-1], 'D'] + 1 / m2 * df.loc[df.index[i], 'K']
            except Exception as e:
                logger.error(f"计算K/D值时出错，索引={i}: {str(e)}")
    else:
        logger.warning("没有有效的RSV值，无法计算KDJ")
    
    # 计算J值
    df['J'] = 3 * df['K'] - 2 * df['D']
    
    # 检查最终结果
    logger.info(f"KDJ计算结果前5行: \n{df[['code', 'K', 'D', 'J']].head().to_string()}")
    logger.info(f"KDJ计算结果后5行: \n{df[['code', 'K', 'D', 'J']].tail().to_string()}")
    logger.info(f"KDJ结果中的NaN值数量: K={df['K'].isna().sum()}, D={df['D'].isna().sum()}, J={df['J'].isna().sum()}")
    
    return df[['code', 'K', 'D', 'J']]