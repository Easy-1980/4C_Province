# 爬取B站视频的基础信息、评论和弹幕数据，并保存到 CSV 文件中
# 输出了指定文件夹下的三个 CSV 文件：video_info.csv、comments_data.csv、danmaku_data.csv

import requests
import time
import datetime
import csv
import os
import pandas as pd
import re  

# 填入Cookie，保持IP属地抓取正常
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Cookie": "buvid3=xxxxx; b_nut=xxxx; i-wanna-go-back=-1; CURRENT_FNVAL=16; CURRENT_QUALITY=80; SESSDATA=xxxx; bili_jct=xxxx; DedeUserID=xxxx; DedeUserID__ckMd5=xxxx; sid=xxxx"
}

# 设定数据保存的文件夹名称
SAVE_DIR = "data3"
# 如果 data 文件夹不存在，自动创建它
os.makedirs(SAVE_DIR, exist_ok=True)


def load_tasks_from_excel(excel_path):
    """【新增功能】从 Excel 表格中读取剧种、BV号和省份"""
    print(f"正在加载 Excel 文件: {excel_path} ...")
    try:
        df = pd.read_excel(excel_path)
        
        # 过滤掉空行（防止 Excel 里有空行导致报错）
        df = df.dropna(subset=['opera', 'bvid'])
        
        tasks = []
        for index, row in df.iterrows():
            tasks.append({
                "bvid": str(row['bvid']).strip(),
                "opera_type": str(row['opera']).strip(),
                "province": str(row['province']).strip() 
            })
        print(f"成功加载了 {len(tasks)} 个抓取任务！\n")
        return tasks
    except Exception as e:
        print(f"读取 Excel 失败，请检查文件路径和表头是否正确: {e}")
        return []


def save_to_csv(filename, headers, row_data):
    """通用保存函数：将一行数据追加写入指定的 CSV 文件"""
    filepath = os.path.join(SAVE_DIR, filename)
    file_exists = os.path.isfile(filepath)
    
    with open(filepath, mode='a', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(headers) 
        writer.writerow(row_data)


def get_bilibili_data(task):
    bvid = task["bvid"]
    opera_type = task["opera_type"]
    province = task["province"] 
    
    print(f"开始抓取 [{province} - {opera_type}] 视频：{bvid} ...")

    # 1. 视频基础信息
    info_url = f"https://api.bilibili.com/x/web-interface/view?bvid={bvid}"
    response = requests.get(info_url, headers=HEADERS).json()
    
    if response['code'] != 0:
        print(f"获取视频信息失败: {response['message']}")
        return

    data = response['data']
    aid = data['aid']
    cid = data['cid']
    
    clean_desc = data['desc'].replace('\n', ' ')
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    save_to_csv(
        "video_info.csv",
        ["省份", "剧种", "BV号", "标题", "简介", "播放量", "点赞数", "投币数", "收藏数", "弹幕总数", "抓取时间"], 
        [province, opera_type, bvid, data['title'], clean_desc, data['stat']['view'], data['stat']['like'], data['stat']['coin'], data['stat']['favorite'], data['stat']['danmaku'], current_time] 
    )
    print("视频基础信息已保存")

    # 2. 评论数据 (抓取前 60 条)
    target_count = 60
    current_count = 0
    next_offset = 0

    while current_count < target_count:
        reply_url = f"https://api.bilibili.com/x/v2/reply/main?type=1&oid={aid}&mode=3&next={next_offset}"
        reply_res = requests.get(reply_url, headers=HEADERS).json()
        
        if reply_res['code'] == 0 and reply_res['data']['replies']:
            replies = reply_res['data']['replies']
            for reply in replies:
                if current_count >= target_count:
                    break
                
                content = reply['content']['message'].replace('\n', ' ')
                like_count = reply['like']
                ctime = datetime.datetime.fromtimestamp(reply['ctime']).strftime('%Y-%m-%d %H:%M:%S')
                location = reply.get('reply_control', {}).get('location', '未知')
                
                save_to_csv(
                    "comments_data.csv",
                    ["省份", "剧种", "BV号", "评论时间", "点赞数", "IP属地", "评论内容"], 
                    [province, opera_type, bvid, ctime, like_count, location, content] 
                )
                current_count += 1
            
            if current_count >= target_count:
                break

            cursor = reply_res['data'].get('cursor', {})
            is_end = cursor.get('is_end', True)
            next_offset = cursor.get('next', 0)
            
            if is_end or not next_offset:
                break
                
            time.sleep(2) # 翻页防风控停顿
        else:
            break
            
    print(f"评论已保存 ({current_count} 条)")
    time.sleep(1)

    # 3. 弹幕数据（使用正则解析，避免脏数据导致崩溃）
    danmaku_url = f"https://api.bilibili.com/x/v1/dm/list.so?oid={cid}"
    dm_res = requests.get(danmaku_url, headers=HEADERS)
    dm_res.encoding = 'utf-8'
    
    # 正则提取：无视格式错误直接抓内容
    danmakus = re.findall(r'<d p="(.*?)">(.*?)</d>', dm_res.text)
    count_dm = 0
    
    for p_str, text in danmakus:
        p_attrs = p_str.split(',')
        if len(p_attrs) > 0:
            appear_time = float(p_attrs[0])
            
            if text:
                clean_text = text.replace('\n', ' ')
                save_to_csv(
                    "danmaku_data.csv",
                    ["省份", "剧种", "BV号", "视频进度(秒)", "弹幕内容"], 
                    [province, opera_type, bvid, appear_time, clean_text] 
                )
                count_dm += 1
            
    print(f"    ✔️ 弹幕已保存 ({count_dm} 条)\n")


# 自动去重
def clean_duplicate_comments():
    """在所有爬虫任务结束后，使用 Pandas 对评论文件进行去重"""
    filepath = os.path.join(SAVE_DIR, "comments_data.csv")
    if not os.path.isfile(filepath):
        return
        
    print("\n" + "="*40)
    print("开始进行评论去重")
    
    df = pd.read_csv(filepath)
    original_count = len(df)
    
    df_cleaned = df.drop_duplicates(subset=['BV号', '评论内容'], keep='first')
    
    cleaned_count = len(df_cleaned)
    duplicates_removed = original_count - cleaned_count
    
    df_cleaned.to_csv(filepath, index=False, encoding='utf-8-sig')
    
    print(f"清洗完成。清洗前共 {original_count} 条，去除了 {duplicates_removed} 条重复数据，最终剩余 {cleaned_count} 条。")
    print("="*40 + "\n")


if __name__ == "__main__":
    
    excel_file_path = "bilibili_tasks.xlsx"  
    
    VIDEO_TASKS = load_tasks_from_excel(excel_file_path)
    
    if not VIDEO_TASKS:
        print("未获取到任务，程序即将退出...")
    else:
        # 1. 遍历任务清单，依次执行爬取
        for task in VIDEO_TASKS:
            get_bilibili_data(task)
            time.sleep(3) # 两个视频任务之间，停顿 3 秒，避免请求过于密集
            
        # 2. 所有任务爬完之后，执行一次统一的数据清洗（去重）
        clean_duplicate_comments()
        
        # 使用动态变量匹配文件夹名称，避免困惑
        print(f"Mission Succeeded, indeed you are a top agent.\n数据已存储在 {SAVE_DIR} 文件夹下。")