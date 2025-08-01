import requests
import json
import time
import csv
import os
from datetime import datetime


def test_streaming_response():
    """
    测试流式响应，记录收到第一个message chunk和第一个follow_up_questions的时间
    """
    url = 'https://astra-api-service.watiapp.io/api/v1/chat-preview'
    
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        'baggage': 'sentry-environment=production,sentry-public_key=c441c7b41c87fe54fc74906a5d059763,sentry-trace_id=db97b7c47c46ce66bf214bcb94efa3fe,sentry-transaction=GET%20%2Fadmin%2F%5Ball%5D,sentry-sampled=true,sentry-sample_rand=0.4889861371100064,sentry-sample_rate=1',
        'cache-control': 'no-cache',
        'content-type': 'text/plain;charset=UTF-8',
        'origin': 'https://dev-astra.watiapp.io',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://dev-astra.watiapp.io/admin/agents/723a24fe-d365-4936-992b-38a9c53af128/config',
        'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sentry-trace': 'db97b7c47c46ce66bf214bcb94efa3fe-8c22ca59cbb133e2-1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        'Cookie': '_ga=GA1.1.478570873.1718938918; ajs_user_id=null; ajs_group_id=null; ajs_anonymous_id=%22219555d3-b818-4956-b253-58316d4c8b88%22; _hp5_event_props.1035095721=%7B%7D; OUTFOX_SEARCH_USER_ID_NCOO=674412543.393003; _hp5_meta.1035095721=%7B%22userId%22%3A%222384670325846482%22%2C%22identity%22%3A%22enoch.zhao%40clare.ai%22%2C%22sessionId%22%3A%221122689549879304%22%2C%22sessionProperties%22%3A%7B%22id%22%3A%221122689549879304%22%2C%22referrer%22%3A%22%22%2C%22utm%22%3A%7B%22source%22%3A%22%22%2C%22medium%22%3A%22%22%2C%22term%22%3A%22%22%2C%22content%22%3A%22%22%2C%22campaign%22%3A%22%22%7D%2C%22initial_pageview_info%22%3A%7B%22id%22%3A%22%22%2C%22url%22%3A%7B%22domain%22%3A%22%22%2C%22path%22%3A%22%22%2C%22query%22%3A%22%22%2C%22hash%22%3A%22%22%7D%7D%7D%2C%22lastEventTime%22%3A1730972710302%7D; intercom-id-e6an2jb4=515f3fc0-63b7-42f2-93c9-32bc4bb765cf; intercom-device-id-e6an2jb4=52756ae0-22a4-4bf1-8168-78a16e1bf5b2; intercom-id-at2ayd3s=d48be6f5-8edf-4577-b0d9-0024741a9d4c; intercom-device-id-at2ayd3s=d505a45c-0119-4294-aad2-d0ab46d3b3f2; mp_8fb3ee5b2ea042b664c0b74f689c572b_mixpanel=%7B%22distinct_id%22%3A%20%22%24device%3A1947386bba61cfe-0e20539e4c58e-1e525636-29b188-1947386bba61cfe%22%2C%22%24device_id%22%3A%20%221947386bba61cfe-0e20539e4c58e-1e525636-29b188-1947386bba61cfe%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%7D; _hp2_id.1035095721=%7B%22userId%22%3A%222979050270959065%22%2C%22pageviewId%22%3A%223457053395010420%22%2C%22sessionId%22%3A%224968313085228241%22%2C%22identity%22%3A%22keyi%2B1%40clare.ai%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3Anull%2C%22isIdentified%22%3A1%2C%22oldIdentity%22%3Anull%7D; _gcl_au=1.1.890705350.1753251864; _fbp=fb.1.1753251864422.261324587462506610; hubspotutk=e1fdf139edc13988005c73b977e19c4c; __hssrc=1; cf_clearance=vHDzi5_Jeef7tk1Hp..Xileoald_UquKFsO.XcPDHbk-1753349324-1.2.1.1-VXJYgiYr.eClqqzhc0z.NVEpZnJFnIrLrjvbrDhqRDTj5Fn10tKVax9tOrNGf7g4YcrgHCqcy66UThTOcNLH7k4K97k7N71QW17u0v7NlA7iOukw1DfoB_8io9e2SOX1fwqzvXlM3g8TpWQX7dE30.jCnGx83ZWSfjwGALM_ucd5vdkEUhwwr5w4dKfXaGdGlYSfCKIPUZeZgXnNf31.5kLCbXA59ClpVj78IdaRBpY; __hstc=108146013.e1fdf139edc13988005c73b977e19c4c.1753251865046.1753251865046.1753349326986.2; _hp2_id.1546758222=%7B%22userId%22%3A%22859279843549397%22%2C%22pageviewId%22%3A%227854676287121335%22%2C%22sessionId%22%3A%222060272681232149%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D; _ga_G2E2VQFXP0=GS2.1.s1753412905$o4$g0$t1753412905$j60$l0$h0; _ga_HYL717ZD73=GS2.1.s1753412905$o67$g0$t1753412905$j60$l0$h0; sidebar_state=true; _clck=mtieq5%7C2%7Cfy2%7C0%7C2028; ID=54961936-4750-4bcd-8089-138c5251a7b9; intercom-session-at2ayd3s=R0NsclV0NGZmWGhseGRNUkd2ZXhISzB3SFNiK2RoTktwRmMySkswamNNSk9SNDYwZTlsSENvaVgwK3VHMTZxRWlzQnhQY2dOOW5pOThnazU3V0tpdEdsL3BXb0Y0NXpheG55bDN5OWQ1WGM9LS1vQ1FaZE11OGhWQVlXNVBOTldMVzNBPT0=--9f1e480fc5aacec374aeb7d77bd69cd5c4fb361c; Token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiNTAwMzlkZGYtZWUxYS00ZGYxLWE4YzItZmNlNzI5YWM3OGQyIiwiZXhwIjoxNzUzOTQ5Mjc1LCJpc3MiOiJTRUxGX0hPU1RFRCIsInN1YiI6IkNvbnNvbGUgQVBJIFBhc3Nwb3J0In0.YuBoyY3nKjEWmsA5PnFxngmyjpfMTkS3irVq4nDcyBQ; _clsk=1322s9a%7C1753949048237%7C7%7C1%7Cb.clarity.ms%2Fcollect',
        'X-Astra-Token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiNTAwMzlkZGYtZWUxYS00ZGYxLWE4YzItZmNlNzI5YWM3OGQyIiwiZXhwIjoxNzUzOTUwNDAwLCJpc3MiOiJTRUxGX0hPU1RFRCIsInN1YiI6IkNvbnNvbGUgQVBJIFBhc3Nwb3J0In0.Xem6f_eIb1fXGHr8IUKuLaodAQXsQRxlTfJPflZAdxI'
    }
    
    data = {
        "agent_id": "f59180f9-e4c0-4390-96cb-2840e9a8f3bb",
        "external_user_id": "cfef3271751bbbe073f4a181c331e30c",
        "tenant_id": "8fca58b4-b24e-4fc5-9181-0284e5455c4d",
        "message_content": "Hi",
        "response_mode": "streaming",
        "conversation_id": "",
        "source": "ai_bar",
        "inputs": {
            "first_name": "Enoch",
            "last_name": "Zhao",
            "email": "enoch.zhao@clare.ai",
            "custom_fields": "{\"phone\":\"\"}",
            "conversation_id": ""
        }
    }
    
    # 记录开始时间
    start_time = time.time()
    print(f"请求开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
    
    # 时间记录变量
    first_message_chunk_time = None
    first_follow_up_questions_time = None
    
    # 数据收集列表
    test_results = []
    current_test = {
        'test_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'start_timestamp': start_time,
        'first_message_chunk_ms': None,
        'first_follow_up_questions_ms': None,
        'time_difference_ms': None,
        'status': 'running'
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, stream=True)
        response.raise_for_status()
        
        print(f"响应状态码: {response.status_code}")
        print("开始接收流式数据...")
        print("-" * 50)
        
        current_event = None
        
        for line in response.iter_lines(decode_unicode=True):
            if line:
                current_time = time.time()
                elapsed_time = (current_time - start_time) * 1000  # 转换为毫秒
                
                # 处理SSE格式的数据
                if line.startswith('event:'):
                    current_event = line[6:].strip()  # 移除 'event:' 前缀
                    print(f"[{elapsed_time:.2f}ms] 事件: {current_event}")
                    
                elif line.startswith('data:'):
                    data_content = line[5:].strip()  # 移除 'data:' 前缀
                    
                    if data_content.strip() == '[DONE]':
                        print(f"[{elapsed_time:.2f}ms] 流式响应结束")
                        break
                    
                    try:
                        json_data = json.loads(data_content)
                        
                        # 检查是否是message事件
                        if current_event == 'message' or (json_data.get('event') == 'message'):
                            if first_message_chunk_time is None:
                                first_message_chunk_time = elapsed_time
                                current_test['first_message_chunk_ms'] = elapsed_time
                                print(f"✅ 第一个message事件时间: {elapsed_time:.2f}ms")
                                print(f"   消息ID: {json_data.get('message_id', 'N/A')}")
                                print(f"   对话ID: {json_data.get('conversation_id', 'N/A')}")
                                answer = json_data.get('answer', '')
                                if answer:
                                    print(f"   内容: {answer[:100]}...")
                                else:
                                    print(f"   内容: (空答案)")
                            
                        # 检查是否是follow_up_questions事件
                        elif current_event == 'follow_up_questions' or (json_data.get('event') == 'follow_up_questions'):
                            if first_follow_up_questions_time is None:
                                first_follow_up_questions_time = elapsed_time
                                current_test['first_follow_up_questions_ms'] = elapsed_time
                                print(f"✅ 第一个follow_up_questions事件时间: {elapsed_time:.2f}ms")
                                questions = json_data.get('follow_up_questions', [])
                                print(f"   问题数量: {len(questions)}")
                                for i, q in enumerate(questions[:3]):  # 只显示前3个问题
                                    print(f"   问题{i+1}: {q[:50]}...")
                        
                        # 检查是否是message_end事件
                        elif current_event == 'message_end' or (json_data.get('event') == 'message_end'):
                            print(f"[{elapsed_time:.2f}ms] 消息结束事件")
                            print(f"   最终答案: {json_data.get('answer', '')[:100]}...")
                        
                        # 打印其他事件类型
                        else:
                            event_type = json_data.get('event', current_event or 'unknown')
                            print(f"[{elapsed_time:.2f}ms] 其他事件类型: {event_type}")
                            
                    except json.JSONDecodeError:
                        print(f"[{elapsed_time:.2f}ms] 无法解析JSON: {data_content[:100]}...")
                
                # 如果两个时间都已记录，可以选择提前结束
                if first_message_chunk_time and first_follow_up_questions_time:
                    print("已记录到所有关键时间点")
                    # 可以选择在这里break，或者继续监听完整响应
        
        # 输出总结
        print("\n" + "=" * 50)
        print("时间测试总结:")
        print(f"请求开始时间: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        
        if first_message_chunk_time:
            print(f"第一个message chunk: {first_message_chunk_time:.2f}ms")
        else:
            print("❌ 未收到message chunk")
            
        if first_follow_up_questions_time:
            print(f"第一个follow_up_questions: {first_follow_up_questions_time:.2f}ms")
        else:
            print("❌ 未收到follow_up_questions")
            
        if first_message_chunk_time and first_follow_up_questions_time:
            time_diff = first_follow_up_questions_time - first_message_chunk_time
            current_test['time_difference_ms'] = time_diff
            print(f"两者时间差: {time_diff:.2f}ms")
            
        # 更新测试状态
        current_test['status'] = 'completed' if (first_message_chunk_time and first_follow_up_questions_time) else 'partial'
        
        # 保存测试结果到CSV文件
        save_test_results(current_test)
        
    except requests.exceptions.RequestException as e:
        print(f"请求错误: {e}")
        current_test['status'] = 'error'
        save_test_results(current_test)
    except KeyboardInterrupt:
        print("\n手动停止测试")
        current_test['status'] = 'interrupted'
        save_test_results(current_test)
    except Exception as e:
        print(f"其他错误: {e}")
        current_test['status'] = 'error'
        save_test_results(current_test)


def save_test_results(test_data):
    """保存测试结果到CSV文件"""
    results_dir = 'results'
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
    
    csv_file = os.path.join(results_dir, 'timing_test_results.csv')
    file_exists = os.path.isfile(csv_file)
    
    with open(csv_file, 'a', newline='', encoding='utf-8') as file:
        fieldnames = [
            'test_time', 'start_timestamp', 'first_message_chunk_ms', 
            'first_follow_up_questions_ms', 'time_difference_ms', 'status'
        ]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(test_data)
    
    print(f"测试结果已保存到: {csv_file}")


def run_multiple_tests(num_tests=5):
    """运行多次测试"""
    print(f"开始运行 {num_tests} 次测试...")
    print("=" * 60)
    
    for i in range(num_tests):
        print(f"\n第 {i+1}/{num_tests} 次测试:")
        print("-" * 40)
        test_streaming_response()
        
        if i < num_tests - 1:
            print("等待3秒后进行下一次测试...")
            time.sleep(3)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'multiple':
        num_tests = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        run_multiple_tests(num_tests)
    else:
        test_streaming_response()
