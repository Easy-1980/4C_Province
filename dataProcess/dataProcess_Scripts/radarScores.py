# 从评论里提取关键词，计算每个维度的得分，生成雷达图数据
# 输出了一个 JSON 文件：4_radarScores.json，包含了每个省份的雷达图维度得分，以及一个全国总盘的综合得分

import pandas as pd
import json
import os
import math

# ==========================================
# 1. 核心配置：构建 6 大维度的“语义映射词典”
# ==========================================
DIMENSIONS = [
    {"name": "服化道审美", "keywords": ["衣服", "扮相", "妆容", "头饰", "绝美", "好看", "漂亮", "服饰", "美轮美奂", "审美"]},
    {"name": "二创与整活", "keywords": ["哈哈", "梗", "鬼畜", "整活", "搞笑", "离谱", "笑死", "绝了", "魔性", "二创", "联动", "出圈"]},
    {"name": "名场面打卡", "keywords": ["名场面", "打卡", "终于", "高能", "前方", "经典", "来了", "啊啊啊", "名段", "名篇"]},
    {"name": "传统文化底蕴", "keywords": ["国粹", "非遗", "传承", "老祖宗", "文化", "底蕴", "艺术", "致敬", "传统", "瑰宝"]},
    {"name": "剧情与价值观", "keywords": ["感人", "泪目", "剧情", "故事", "爱情", "三观", "感动", "因果", "封建"]},
    {"name": "唱腔与身段", "keywords": ["唱腔", "好听", "嗓音", "身段", "功底", "基本功", "台步", "动作", "眼神", "绝活", "转音"]}
]

DATA_DIR = "data3"  
INFO_CSV = os.path.join(DATA_DIR, "video_info.csv")
COMMENTS_CSV = os.path.join(DATA_DIR, "comments_data.csv") 
OUTPUT_JSON = "4_radarScores.json"

print("开始利用 NLP 词频映射计算雷达图维度得分...")

# ==========================================
# 2. 读取数据并整合
# ==========================================
df_info = pd.read_csv(INFO_CSV, encoding='utf-8-sig')
df_comments = pd.read_csv(COMMENTS_CSV, encoding='utf-8-sig')

df_merge = pd.merge(df_comments, df_info[['BV号', '省份']], on='BV号', how='inner')
if '省份_x' in df_merge.columns:
    df_merge.rename(columns={'省份_x': '省份'}, inplace=True)

radar_output = {}

# --- 公共算法函数：将一段长文本转换为 6 维度得分 ---
def calculate_radar_scores(text_data):
    raw_scores = []
    for dim in DIMENSIONS:
        hit_count = sum(text_data.count(kw) for kw in dim["keywords"])
        raw_scores.append(hit_count)
        
    if sum(raw_scores) == 0:
        return [60, 60, 60, 60, 60, 60]
    else:
        log_scores = [math.log(score + 1) for score in raw_scores]
        max_log = max(log_scores)
        final_scores = []
        for l_score in log_scores:
            normalized = 55 + (l_score / max_log) * (98 - 55)
            final_scores.append(int(round(normalized)))
        return final_scores

# ==========================================
# 3. 计算【全国】大盘总得分 (新增部分)
# ==========================================
print(" 正在计算【全国】综合维度数据...")
all_national_text = "".join(df_merge['评论内容'].astype(str).tolist())
national_scores = calculate_radar_scores(all_national_text)
radar_output["全国"] = {
    "radarData": national_scores
}
print(f"全国得分: {national_scores}")

# ==========================================
# 4. 计算【各省份】单独得分
# ==========================================
for prov, group in df_merge.groupby('省份'):
    prov_text = "".join(group['评论内容'].astype(str).tolist())
    prov_scores = calculate_radar_scores(prov_text)
            
    radar_output[prov] = {
        "radarData": prov_scores
    }
    print(f"{prov} 计算完成: {prov_scores}")

# ==========================================
# 5. 导出 JSON
# ==========================================
with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
    json.dump(radar_output, f, ensure_ascii=False, indent=2)

print(f"\n雷达图数据生成成功！(已包含全国总盘数据)，已导出为: {OUTPUT_JSON}")