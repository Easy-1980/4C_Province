# 生成全国剧种弹幕词云数据的脚本
# 输出了一个 JSON 文件 2_nationalWordCloud.json，包含了全国剧种的高频词汇及其出现次数，格式适合直接用于 ECharts 的词云图展示。
# 这个文件不能直接用,需要进行一些人为调整再使用,因为它是从弹幕中提取的,可能会包含一些无意义的词汇,需要进行清洗和过滤————2_nationalWordCloud_Selected.json

import pandas as pd
import jieba
from collections import Counter
import json
import os
import re

# ==========================================
# 0. 配置路径
# ==========================================
DATA_DIR = "data3"
info_csv_path = os.path.join(DATA_DIR, "video_info.csv")
danmu_csv_path = os.path.join(DATA_DIR, "danmaku_data.csv")
output_json_path = "2_nationalWordCloud.json"

# 停用词过滤（过滤掉意义不大的词）
STOP_WORDS = { "哈哈哈哈", "哈哈", "这个", "那个", "每个", "一个", "两个", "有些",
    "就是", "不是", "还是", "但是", "而且", "然后", "所以", "因为", "所以",
    "真的", "确实", "其实", "简直", "有点", "好像", "一样", "这样", "那样",
    "怎么", "什么", "为什么", "哪里", "现在", "刚才", "之后", "之前",
    "感觉", "觉得", "认为", "以为", "发现", "知道", "看到", "起来", "出来",
    "你们", "我们", "他们", "人家", "自己", "这里", "那里",
    "每周", "必看", "弹幕", "视频", "画质", "老师", "演员", "前面", "后面", "这段",
    "可以", "没有", "还有", "应该", "可能", "意思", "应该", "真是", "好好"}

print("开始生成全国弹幕词云数据...")

# ==========================================
# 1. 寻找每个剧种弹幕数最多的视频 BV 号
# ==========================================
df_info = pd.read_csv(info_csv_path, encoding='utf-8-sig')
df_info['弹幕总数'] = pd.to_numeric(df_info['弹幕总数'], errors='coerce').fillna(0)

# 每个剧种取弹幕最多的那一行
top_video_indexes = df_info.groupby('剧种')['弹幕总数'].idxmax()
top_bvs = df_info.loc[top_video_indexes, 'BV号'].unique().tolist()

print(f"已锁定 {len(top_bvs)} 个核心剧种视频作为数据源。")

# ==========================================
# 2. 提取并汇总这些视频的弹幕内容
# ==========================================
df_danmu = pd.read_csv(danmu_csv_path, encoding='utf-8-sig')
# 过滤：只保留这几个 Top 视频的弹幕
df_target_danmu = df_danmu[df_danmu['BV号'].isin(top_bvs)]

# 将所有弹幕内容合并为一个长字符串
all_text = "".join(df_target_danmu['弹幕内容'].astype(str).tolist())

# ==========================================
# 3. 分词与频率统计
# ==========================================
# 使用 jieba 进行分词
words = jieba.lcut(all_text)

# 清洗词汇：去掉单字、停用词、标点符号、以及空格
clean_words = []
for word in words:
    # 过滤掉单字（如“好”、“美”）、停用词、纯数字、以及非中文字符串
    if len(word) > 1 and word not in STOP_WORDS and not re.match(r'^[0-9]+$', word):
        # 进一步过滤纯标点符号
        if re.match(r'[\u4e00-\u9fa5]+', word):
            clean_words.append(word)

# 统计频率并取前 100 个
word_counts = Counter(clean_words).most_common(100)

# ==========================================
# 4. 格式化并导出 JSON
# ==========================================
# 转换为 ECharts 要求的格式: [{"name": "词语", "value": 100}, ...]
word_cloud_data = [{"name": word, "value": count} for word, count in word_counts]

# 为了方便你直接在合并脚本里引用，我们输出一个包含 wordCloud 键的对象
output_obj = {
    "全国": {
        "wordCloud": word_cloud_data
    }
}

with open(output_json_path, 'w', encoding='utf-8') as f:
    json.dump(output_obj, f, ensure_ascii=False, indent=2)

print(f"词云数据生成成功！共提取 {len(word_cloud_data)} 个高频词汇。")
print(f"输出文件: {output_json_path}")