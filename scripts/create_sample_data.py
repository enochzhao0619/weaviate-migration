import csv
import os
import random
from datetime import datetime, timedelta

def create_sample_data():
    """创建示例测试数据"""
    results_dir = 'results'
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    
    csv_file = os.path.join(results_dir, 'timing_test_results.csv')
    
    # 生成示例数据
    sample_data = []
    base_time = datetime.now() - timedelta(hours=2)
    
    for i in range(20):
        test_time = base_time + timedelta(minutes=i * 5)
        
        # 模拟一些变化的响应时间
        message_chunk_time = random.uniform(800, 1500)  # 800-1500ms
        follow_up_time = message_chunk_time + random.uniform(500, 2000)  # 额外500-2000ms
        
        # 偶尔添加一些失败的测试
        if random.random() < 0.1:  # 10%失败率
            status = 'error'
            message_chunk_time = None
            follow_up_time = None
            time_diff = None
        else:
            status = 'completed'
            time_diff = follow_up_time - message_chunk_time if message_chunk_time else None
        
        sample_data.append({
            'test_time': test_time.strftime('%Y-%m-%d %H:%M:%S'),
            'start_timestamp': test_time.timestamp(),
            'first_message_chunk_ms': message_chunk_time,
            'first_follow_up_questions_ms': follow_up_time,
            'time_difference_ms': time_diff,
            'status': status
        })
    
    # 写入CSV文件
    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        fieldnames = [
            'test_time', 'start_timestamp', 'first_message_chunk_ms', 
            'first_follow_up_questions_ms', 'time_difference_ms', 'status'
        ]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sample_data)
    
    print(f"已创建 {len(sample_data)} 条示例数据")
    print(f"文件保存至: {csv_file}")

if __name__ == "__main__":
    create_sample_data()