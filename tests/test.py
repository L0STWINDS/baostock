import baostock as bs
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import matplotlib.dates as mdates

# 计算KDJ指标
def calculate_kdj(df, n=9, m1=3, m2=3):
    df = df.copy()
    
    # 计算RSV
    low_list = df['low'].rolling(window=n, min_periods=1).min()
    high_list = df['high'].rolling(window=n, min_periods=1).max()
    rsv = (df['close'] - low_list) / (high_list - low_list) * 100
    
    # 计算K值、D值、J值
    df['K'] = rsv.ewm(alpha=1/m1, adjust=False).mean()
    df['D'] = df['K'].ewm(alpha=1/m2, adjust=False).mean()
    df['J'] = 3 * df['K'] - 2 * df['D']
    
    return df

# 主函数
def main(stock_code, start_date, end_date):
    # 登录系统
    lg = bs.login()
    print('登录响应错误代码:' + lg.error_code)
    print('登录响应错误信息:' + lg.error_msg)
    
    # 获取沪深A股历史K线数据
    rs = bs.query_history_k_data_plus(stock_code,
        "date,code,open,high,low,close",
        start_date=start_date, end_date=end_date,
        frequency="w", adjustflag="2")
    print('查询历史K线数据响应错误代码:' + rs.error_code)
    print('查询历史K线数据响应错误信息:' + rs.error_msg)
    
    # 处理结果集
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    
    # 转换数据类型
    numeric_cols = ['open', 'high', 'low', 'close']
    for col in numeric_cols:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors='coerce')
    
    # 计算KDJ指标
    result = calculate_kdj(result)
    
    # 初始化回测参数
    initial_capital = 10000  # 初始资金
    cash = initial_capital   # 当前现金
    shares = 0              # 持有股数
    buy_records = []        # 买入记录 [价格, 股数]
    trade_history = []      # 交易历史
    
    # 回测策略
    for i in range(len(result)):
        date = result['date'].iloc[i]
        close_price = result['close'].iloc[i]
        j_value = result['J'].iloc[i]
        
        # 买入信号: J值小于等于-5
        if j_value <= -5 and cash > 0:
            buy_amount = cash * 0.25  # 使用25%的现金
            buy_shares = int(buy_amount / close_price)
            
            if buy_shares > 0:
                cash -= buy_shares * close_price
                shares += buy_shares
                buy_records.append([close_price, buy_shares])
                
                trade_record = {
                    'date': date,
                    'action': 'BUY',
                    'price': close_price,
                    'shares': buy_shares,
                    'value': buy_shares * close_price,
                    'cash': cash,
                    'total_shares': shares,
                    'j_value': j_value
                }
                trade_history.append(trade_record)
                
        # 卖出信号: J值大于等于105
        elif j_value >= 105 and shares > 0 and buy_records:
            # 卖出最前一次买入的股数
            sell_price = close_price
            sell_shares = buy_records[0][1]
            
            cash += sell_shares * sell_price
            shares -= sell_shares
            buy_records.pop(0)  # 移除最早的买入记录
            
            trade_record = {
                'date': date,
                'action': 'SELL',
                'price': sell_price,
                'shares': sell_shares,
                'value': sell_shares * sell_price,
                'cash': cash,
                'total_shares': shares,
                'j_value': j_value
            }
            trade_history.append(trade_record)
    
    # 计算最终资产和收益率
    final_assets = cash + shares * result['close'].iloc[-1]
    profit_rate = (final_assets - initial_capital) / initial_capital * 100
    
    # 打印交易历史
    print("\n===== 交易历史 =====")
    for record in trade_history:
        action_text = "买入" if record['action'] == 'BUY' else "卖出"
        print(f"日期: {record['date']}, 操作: {action_text}, 价格: {record['price']:.2f}, "
              f"数量: {record['shares']}, 交易额: {record['value']:.2f}, "
              f"现金: {record['cash']:.2f}, 持股数: {record['total_shares']}, J值: {record['j_value']:.2f}")
    
    # 打印回测结果
    print("\n===== 回测结果 =====")
    print(f"初始资金: {initial_capital:.2f}")
    print(f"最终资产: {final_assets:.2f}")
    print(f"收益率: {profit_rate:.2f}%")
    print(f"剩余现金: {cash:.2f}")
    print(f"持有股票: {shares}股, 价值: {shares * result['close'].iloc[-1]:.2f}")
    
    # 绘制K线图和KDJ指标
    plot_results(result, trade_history, initial_capital, final_assets, stock_code)
    
    # 登出系统
    bs.logout()

# 绘制结果图表
def plot_results(df, trade_history, initial_capital, final_assets, stock_code):
    # 转换日期格式
    df['date'] = pd.to_datetime(df['date'])
    
    # 创建图表
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
    
    # 绘制K线图
    ax1.plot(df['date'], df['close'], label='Close Price')
    
    # 标记买入点和卖出点
    for record in trade_history:
        date = pd.to_datetime(record['date'])
        price = record['price']
        if record['action'] == 'BUY':
            ax1.scatter(date, price, color='green', marker='^', s=100)
        else:  # SELL
            ax1.scatter(date, price, color='red', marker='v', s=100)
    
    # 设置K线图标题和标签
    ax1.set_title(f'Stock Price and Trading Signals - {stock_code}')
    ax1.set_ylabel('Price')
    ax1.legend()
    ax1.grid(True)
    
    # 绘制KDJ指标
    ax2.plot(df['date'], df['K'], label='K')
    ax2.plot(df['date'], df['D'], label='D')
    ax2.plot(df['date'], df['J'], label='J')
    
    # 添加J值阈值线
    ax2.axhline(y=-5, color='g', linestyle='--', alpha=0.7)
    ax2.axhline(y=105, color='r', linestyle='--', alpha=0.7)
    
    # 设置KDJ图标题和标签
    ax2.set_title('KDJ Indicator')
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Value')
    ax2.legend()
    ax2.grid(True)
    
    # 格式化x轴日期
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    plt.xticks(rotation=45)
    
    # 添加回测结果文本
    profit_rate = (final_assets - initial_capital) / initial_capital * 100
    result_text = f'Initial: {initial_capital:.2f}, Final: {final_assets:.2f}, Return: {profit_rate:.2f}%'
    fig.text(0.5, 0.01, result_text, ha='center', fontsize=12)
    
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.1)
    
    # 保存图表
    plt.savefig(f'/opt/1panel/docker/compose/baostock/tests/图表_{stock_code.replace(".", "_")}.png')
    print(f"结果图表已保存为 图表_{stock_code.replace('.', '_')}.png")

# 在这里修改参数
if __name__ == "__main__":
    # 股票代码
    STOCK_CODE = "sh.600418"
    # 起始日期
    START_DATE = '2024-03-13'
    # 结束日期（空字符串表示当前日期）
    END_DATE = ''
    
    main(STOCK_CODE, START_DATE, END_DATE)