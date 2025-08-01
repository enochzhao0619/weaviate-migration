import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import os
from datetime import datetime
import numpy as np

# 设置中文字体支持
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

def load_test_data():
    """加载测试数据"""
    csv_file = 'results/timing_test_results.csv'
    
    if not os.path.exists(csv_file):
        print(f"数据文件不存在: {csv_file}")
        print("请先运行测试脚本生成数据")
        return None
    
    try:
        df = pd.read_csv(csv_file)
        df['test_time'] = pd.to_datetime(df['test_time'])
        return df
    except Exception as e:
        print(f"读取数据文件时出错: {e}")
        return None

def create_timing_charts(df):
    """创建时间分析图表"""
    if df is None or df.empty:
        print("没有数据可以展示")
        return
    
    # 过滤完整的测试数据
    complete_data = df[df['status'] == 'completed'].copy()
    
    if complete_data.empty:
        print("没有完整的测试数据")
        return
    
    # 创建图表
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('API响应时间分析报告', fontsize=16, fontweight='bold')
    
    # 1. 时间序列图
    ax1 = axes[0, 0]
    ax1.plot(complete_data['test_time'], complete_data['first_message_chunk_ms'], 
             'o-', label='First Message Chunk', color='blue', linewidth=2, markersize=6)
    ax1.plot(complete_data['test_time'], complete_data['first_follow_up_questions_ms'], 
             's-', label='First Follow-up Questions', color='red', linewidth=2, markersize=6)
    ax1.set_title('响应时间趋势图')
    ax1.set_xlabel('测试时间')
    ax1.set_ylabel('响应时间 (ms)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # 2. 时间差分布直方图
    ax2 = axes[0, 1]
    time_diffs = complete_data['time_difference_ms'].dropna()
    if not time_diffs.empty:
        ax2.hist(time_diffs, bins=min(10, len(time_diffs)), alpha=0.7, color='green', edgecolor='black')
        ax2.set_title('时间差分布')
        ax2.set_xlabel('时间差 (ms)')
        ax2.set_ylabel('频次')
        ax2.grid(True, alpha=0.3)
        
        # 添加统计信息
        mean_diff = time_diffs.mean()
        ax2.axvline(mean_diff, color='red', linestyle='--', linewidth=2, label=f'平均值: {mean_diff:.1f}ms')
        ax2.legend()
    
    # 3. 箱线图
    ax3 = axes[1, 0]
    data_for_box = [
        complete_data['first_message_chunk_ms'].dropna(),
        complete_data['first_follow_up_questions_ms'].dropna(),
        complete_data['time_difference_ms'].dropna()
    ]
    labels = ['Message Chunk', 'Follow-up Questions', 'Time Difference']
    
    box_plot = ax3.boxplot(data_for_box, labels=labels, patch_artist=True)
    colors = ['lightblue', 'lightcoral', 'lightgreen']
    for patch, color in zip(box_plot['boxes'], colors):
        patch.set_facecolor(color)
    
    ax3.set_title('响应时间分布箱线图')
    ax3.set_ylabel('时间 (ms)')
    ax3.grid(True, alpha=0.3)
    
    # 4. 统计摘要表
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    # 计算统计数据
    stats_data = []
    metrics = ['first_message_chunk_ms', 'first_follow_up_questions_ms', 'time_difference_ms']
    metric_names = ['Message Chunk (ms)', 'Follow-up Questions (ms)', 'Time Difference (ms)']
    
    for metric, name in zip(metrics, metric_names):
        data = complete_data[metric].dropna()
        if not data.empty:
            stats_data.append([
                name,
                f"{data.mean():.1f}",
                f"{data.median():.1f}",
                f"{data.std():.1f}",
                f"{data.min():.1f}",
                f"{data.max():.1f}"
            ])
    
    if stats_data:
        table = ax4.table(cellText=stats_data,
                         colLabels=['指标', '平均值', '中位数', '标准差', '最小值', '最大值'],
                         cellLoc='center',
                         loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.5)
        
        # 设置表格样式
        for i in range(len(stats_data) + 1):
            for j in range(6):
                cell = table[(i, j)]
                if i == 0:  # 标题行
                    cell.set_facecolor('#4CAF50')
                    cell.set_text_props(weight='bold', color='white')
                else:
                    cell.set_facecolor('#f0f0f0' if i % 2 == 0 else 'white')
    
    ax4.set_title('统计摘要', fontweight='bold', pad=20)
    
    plt.tight_layout()
    
    # 保存图表
    output_dir = 'results'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    chart_file = os.path.join(output_dir, f'timing_analysis_{timestamp}.png')
    plt.savefig(chart_file, dpi=300, bbox_inches='tight')
    print(f"图表已保存到: {chart_file}")
    
    plt.show()

def print_summary_report(df):
    """打印文本摘要报告"""
    if df is None or df.empty:
        print("没有数据可以分析")
        return
    
    complete_data = df[df['status'] == 'completed']
    
    print("\n" + "="*60)
    print("API响应时间测试报告")
    print("="*60)
    
    print(f"总测试次数: {len(df)}")
    print(f"成功测试次数: {len(complete_data)}")
    print(f"成功率: {len(complete_data)/len(df)*100:.1f}%")
    
    if not complete_data.empty:
        print(f"\n时间统计 (基于 {len(complete_data)} 次成功测试):")
        print("-" * 40)
        
        for metric, name in [
            ('first_message_chunk_ms', 'First Message Chunk'),
            ('first_follow_up_questions_ms', 'First Follow-up Questions'),
            ('time_difference_ms', 'Time Difference')
        ]:
            data = complete_data[metric].dropna()
            if not data.empty:
                print(f"{name}:")
                print(f"  平均值: {data.mean():.2f} ms")
                print(f"  中位数: {data.median():.2f} ms")
                print(f"  标准差: {data.std():.2f} ms")
                print(f"  范围: {data.min():.2f} - {data.max():.2f} ms")
                print()
    
    # 状态分布
    status_counts = df['status'].value_counts()
    print("测试状态分布:")
    print("-" * 20)
    for status, count in status_counts.items():
        print(f"{status}: {count} 次")
    
    print("="*60)

def generate_detailed_report():
    """生成详细的HTML报告"""
    df = load_test_data()
    if df is None:
        return
    
    complete_data = df[df['status'] == 'completed']
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>API响应时间测试报告</title>
        <meta charset="utf-8">
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            h1, h2 {{ color: #333; }}
            table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
            th {{ background-color: #f2f2f2; font-weight: bold; }}
            .summary {{ background-color: #f9f9f9; padding: 20px; border-radius: 5px; }}
            .metric {{ margin: 10px 0; }}
        </style>
    </head>
    <body>
        <h1>API响应时间测试报告</h1>
        <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        
        <div class="summary">
            <h2>测试摘要</h2>
            <div class="metric">总测试次数: {len(df)}</div>
            <div class="metric">成功测试次数: {len(complete_data)}</div>
            <div class="metric">成功率: {len(complete_data)/len(df)*100:.1f}%</div>
        </div>
        
        <h2>详细测试数据</h2>
        {df.to_html(index=False, table_id='test-data')}
        
        <h2>统计分析</h2>
        {complete_data.describe().to_html() if not complete_data.empty else '<p>没有完整的测试数据</p>'}
    </body>
    </html>
    """
    
    output_dir = 'results'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    html_file = os.path.join(output_dir, f'timing_report_{timestamp}.html')
    
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"详细HTML报告已保存到: {html_file}")

def main():
    """主函数"""
    print("API响应时间数据分析工具")
    print("=" * 40)
    
    # 加载数据
    df = load_test_data()
    
    if df is None:
        return
    
    # 打印摘要报告
    print_summary_report(df)
    
    # 创建图表
    try:
        create_timing_charts(df)
    except Exception as e:
        print(f"创建图表时出错: {e}")
    
    # 生成HTML报告
    try:
        generate_detailed_report()
    except Exception as e:
        print(f"生成HTML报告时出错: {e}")

if __name__ == "__main__":
    main()