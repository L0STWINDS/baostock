from fastapi import APIRouter, Body
import baostock as bs
import pandas as pd
from typing import Dict, Any, List
from pydantic import BaseModel
from app.utils.retry import async_retry_with_timeout

router = APIRouter(tags=["股票K线数据"])

class StockDailyRequest(BaseModel):
    code: str
    start_date: str
    end_date: str
    adjustflag: str = "3"

@router.post("/candlestick/daily")
@async_retry_with_timeout()  # 添加装饰器
async def get_stock_daily(
    request: StockDailyRequest = Body(...)
):
    """
    获取股票日线K线数据
    
    请求体示例:
    {
        "code": "sh.600000",
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "adjustflag": "3"
    }
    """
    # 登录系统
    lg = bs.login()
    if lg.error_code != '0':
        return {"error": f"登录失败: {lg.error_msg}"}
    
    try:
        # 日线数据字段
        # fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST"
        fields = "date,code,open,high,low,close,preclose"
        
        # 查询历史数据
        rs = bs.query_history_k_data_plus(
            request.code, fields,
            start_date=request.start_date, 
            end_date=request.end_date,
            frequency="d",  # 固定为日线
            adjustflag=request.adjustflag
        )
        
        if rs is None or not hasattr(rs, 'error_code') or rs.error_code != '0':
            error_msg = getattr(rs, 'error_msg', '未知错误')
            return {"error": f"查询失败: {error_msg}"}
        
        # 处理结果集
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        # 检查是否有数据返回
        if not data_list:
            return {"error": "未查询到数据", "code": request.code}
            
        # 转换为DataFrame
        result = pd.DataFrame(data_list, columns=rs.fields)
        
        # 字段映射：英文到中文
        field_mapping = {
            'date': '日期',
            'code': '代码',
            'open': '开盘价',
            'high': '最高价',
            'low': '最低价',
            'close': '收盘价',
            'preclose': '昨收价',
            'volume': '成交量',
            'amount': '成交额',
            'adjustflag': '复权状态',
            'turn': '换手率',
            'tradestatus': '交易状态',
            'pctChg': '涨跌幅',
            'peTTM': '市盈率',
            'psTTM': '市销率',
            'pcfNcfTTM': '市现率',
            'pbMRQ': '市净率',
            'isST': '是否ST'
        }
        
        # 重命名列
        result.rename(columns=field_mapping, inplace=True)
        
        # 转换为JSON格式
        return result.to_dict('records')
    
    finally:
        # 确保登出系统
        bs.logout()

# 添加周线和月线数据请求模型
class StockPeriodRequest(BaseModel):
    code: str
    start_date: str
    end_date: str
    adjustflag: str = "3"

@router.post("/candlestick/weekly")
@async_retry_with_timeout()  # 添加装饰器
async def get_stock_weekly(
    request: StockPeriodRequest = Body(...)
):
    """
    获取股票周线K线数据
    
    请求体示例:
    {
        "code": "sh.600000",
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
        "adjustflag": "3"
    }
    """
    # 登录系统
    lg = bs.login()
    if lg is None or lg.error_code != '0':
        return {"error": f"登录失败: {lg.error_msg if lg is not None else '未知错误'}"}
    
    try:
        # 周线数据字段
        fields = "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"
        
        # 查询历史数据
        rs = bs.query_history_k_data_plus(
            request.code, fields,
            start_date=request.start_date, 
            end_date=request.end_date,
            frequency="w",  # 固定为周线
            adjustflag=request.adjustflag
        )
        
        if rs is None or rs.error_code != '0':
            return {"error": f"查询失败: {rs.error_msg if rs is not None else '未知错误'}"}
        
        # 处理结果集
        data_list = []
        fields_list = rs.fields if hasattr(rs, 'fields') else fields.split(',')
        
        while rs is not None and hasattr(rs, 'error_code') and rs.error_code == '0' and rs.next():
            data_list.append(rs.get_row_data())
        
        # 检查是否有数据返回
        if not data_list:
            return {"error": "未查询到数据", "code": request.code}
            
        # 转换为DataFrame
        result = pd.DataFrame(data_list, columns=fields_list)
        
        # 字段映射：英文到中文
        field_mapping = {
            'date': '日期',
            'code': '代码',
            'open': '开盘价',
            'high': '最高价',
            'low': '最低价',
            'close': '收盘价',
            'volume': '成交量',
            'amount': '成交额',
            'adjustflag': '复权状态',
            'turn': '换手率',
            'pctChg': '涨跌幅'
        }
        
        # 重命名列
        result.rename(columns=field_mapping, inplace=True)
        
        # 转换为JSON格式
        return result.to_dict('records')
    
    finally:
        # 确保登出系统
        if 'bs' in globals() and hasattr(bs, 'logout'):
            bs.logout()

@router.post("/candlestick/monthly")
@async_retry_with_timeout()  # 添加装饰器
async def get_stock_monthly(
    request: StockPeriodRequest = Body(...)
):
    """
    获取股票月线K线数据
    
    请求体示例:
    {
        "code": "sh.600000",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "adjustflag": "3"
    }
    """
    # 登录系统
    lg = bs.login()
    if lg.error_code != '0':
        return {"error": f"登录失败: {lg.error_msg}"}
    
    try:
        # 月线数据字段
        fields = "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"
        
        # 查询历史数据
        rs = bs.query_history_k_data_plus(
            request.code, fields,
            start_date=request.start_date, 
            end_date=request.end_date,
            frequency="m",  # 固定为月线
            adjustflag=request.adjustflag
        )
        
        if rs is None or not hasattr(rs, 'error_code') or rs.error_code != '0':
            error_msg = getattr(rs, 'error_msg', '未知错误')
            return {"error": f"查询失败: {error_msg}"}
        
        # 处理结果集
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        # 检查是否有数据返回
        if not data_list:
            return {"error": "未查询到数据", "code": request.code}
            
        # 转换为DataFrame
        result = pd.DataFrame(data_list, columns=rs.fields)
        
        # 字段映射：英文到中文
        field_mapping = {
            'date': '日期',
            'code': '代码',
            'open': '开盘价',
            'high': '最高价',
            'low': '最低价',
            'close': '收盘价',
            'volume': '成交量',
            'amount': '成交额',
            'adjustflag': '复权状态',
            'turn': '换手率',
            'pctChg': '涨跌幅'
        }
        
        # 重命名列
        result.rename(columns=field_mapping, inplace=True)
        
        # 转换为JSON格式
        return result.to_dict('records')
    
    finally:
        # 确保登出系统
        bs.logout()