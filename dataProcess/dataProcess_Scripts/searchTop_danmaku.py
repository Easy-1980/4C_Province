# 本脚本负责从爬虫产出的 CSV 文件中，提取每个剧种弹幕最多的 BV 号，并分析其弹幕趋势，最终生成一个结构化的 JSON 文件，供前端展示使用。
# 用于放在心电图里
# 输出了一个 JSON 文件：2_danmakuTrend.json，包含了每个省份的弹幕趋势数据（时间轴、弹幕密度、爆点弹幕内容等）

import pandas as pd
import json
import re
import os

# ==========================================
# 0. 文件路径配置 (对接上一步的爬虫输出文件夹)
# ==========================================
DATA_DIR = "data3"
info_csv_path = os.path.join(DATA_DIR, "video_info.csv")
danmu_csv_path = os.path.join(DATA_DIR, "danmaku_data.csv")
output_json_path = "2_danmakuTrend.json"

# ==========================================
# 1. 读取基础信息表，寻找每个剧种弹幕最多的 BV 号
# ==========================================
print("正在读取并分析视频基础信息...")
df_info = pd.read_csv(info_csv_path, encoding='utf-8-sig')

# 确保“弹幕总数”列是数值类型，防止由于字符串对比导致找错最大值
df_info['弹幕总数'] = pd.to_numeric(df_info['弹幕总数'], errors='coerce').fillna(0)

# 【核心逻辑】：按“剧种”分组，找到每组中“弹幕总数”最大值所在的行索引
top_video_indexes = df_info.groupby('剧种')['弹幕总数'].idxmax()
# 提取出这些“Top 1”视频的完整信息行
df_top_videos = df_info.loc[top_video_indexes]

print(f"成功提取出 {len(df_top_videos)} 个剧种的 Top 1 弹幕视频名单！\n")

# ==========================================
# 2. 读取完整的弹幕库数据
# ==========================================
print("正在加载海量弹幕数据库...")
# 注意：爬虫产出的 csv 是有表头的，所以直接读取即可
df_danmu = pd.read_csv(danmu_csv_path, encoding='utf-8-sig')
df_danmu['视频进度(秒)'] = pd.to_numeric(df_danmu['视频进度(秒)'], errors='coerce')
df_danmu = df_danmu.dropna(subset=['视频进度(秒)'])
df_danmu['时间窗口'] = (df_danmu['视频进度(秒)'] // 10) * 10

# 准备一个大字典，用来存放所有省份的生成结果
final_json_output = {}

# ==========================================
# 3. 循环处理每一个 Top 1 视频
# ==========================================
for index, row in df_top_videos.iterrows():
    target_bv = row['BV号']
    ju_zhong = row['剧种']
    province = row['省份']
    video_title = row['标题']
    total_dm = int(row['弹幕总数'])
    
    print(f"[{province}] 开始分析 {ju_zhong} - {target_bv} (总弹幕:{total_dm})")

    # 从大表中过滤出当前视频的弹幕
    df_single = df_danmu[df_danmu['BV号'] == target_bv]
    
    if df_single.empty:
        print(f"  ⚠️ 未找到该视频的弹幕明细，跳过。\n")
        continue

    # --- 3.1 提取剧目名称 ---
    match = re.search(r'《(.*?)》', str(video_title))
    if match:
        opera_name = f"《{match.group(1)}》" 
    else:
        opera_name = "" 
        
    final_opera_name = f"{ju_zhong}{opera_name} - {target_bv}"

    # --- 3.2 计算折线图数据 (times 和 counts) ---
    timeline_df = df_single.groupby('时间窗口').size().reset_index(name='弹幕密度')
    timeline_df = timeline_df.sort_values(by='时间窗口')

    # 如果有时间窗口空缺，其实可以用 reindex 补全，但前端 Echarts 能自动连线，目前这样也可以
    times_list = timeline_df['时间窗口'].apply(lambda x: f"{int(x // 60):02d}:{int(x % 60):02d}").tolist()
    counts_list = timeline_df['弹幕密度'].tolist()

    # --- 3.3 提取“最高能10秒”的弹幕真实内容 ---
    max_count = timeline_df['弹幕密度'].max()
    peak_time = timeline_df[timeline_df['弹幕密度'] == max_count]['时间窗口'].iloc[0]

    peak_danmu_df = df_single[df_single['时间窗口'] == peak_time]
    max_danmakus_list = peak_danmu_df['弹幕内容'].astype(str).tolist()

    print(f" 爆点抓取: {int(peak_time//60):02d}:{int(peak_time%60):02d} | 瞬间弹幕量: {max_count} 条")

    # --- 3.4 组装单省数据结构 ---
    # 顺便把之前约定好的 aiInsight 和 decision 坑位留好，置空备用
    danmaku_trend_obj = {
        "operaName": final_opera_name,
        "times": times_list,
        "counts": counts_list,
        "maxDanmakus": max_danmakus_list,
        "aiInsight": "", 
        "decision": ""
    }
    
    # 以外层省份作为 Key，非常方便你后期合并到前端的 Province_data.json
    final_json_output[province] = {
        "danmakuTrend": danmaku_trend_obj
    }
    
    print(" 处理完成\n")

# ==========================================
# 4. 批量精确排版导出 JSON (对象换行，数组单行)
# ==========================================
json_lines = ["{"]
items = list(final_json_output.items())

for i, (prov, data) in enumerate(items):
    # 外层：省份
    json_lines.append(f'  "{prov}": {{')
    json_lines.append('    "danmakuTrend": {')
    
    trend = data["danmakuTrend"]
    
    # 核心：使用 json.dumps 单独序列化每一个字段，这样既不换行，又能完美处理特殊字符和引号转义
    opera_str = json.dumps(trend["operaName"], ensure_ascii=False)
    times_str = json.dumps(trend["times"], ensure_ascii=False)
    counts_str = json.dumps(trend["counts"], ensure_ascii=False)
    danmakus_str = json.dumps(trend["maxDanmakus"], ensure_ascii=False)
    insight_str = json.dumps(trend["aiInsight"], ensure_ascii=False)
    decision_str = json.dumps(trend["decision"], ensure_ascii=False)
    
    # 手动拼装，让每个属性严格各占一行
    json_lines.append(f'      "operaName": {opera_str},')
    json_lines.append(f'      "times": {times_str},')
    json_lines.append(f'      "counts": {counts_str},')
    json_lines.append(f'      "maxDanmakus": {danmakus_str},')
    json_lines.append(f'      "aiInsight": {insight_str},')
    json_lines.append(f'      "decision": {decision_str}')
    
    json_lines.append('    }')
    
    # 判断是否是最后一个省份，决定要不要加逗号
    if i < len(items) - 1:
        json_lines.append("  },")
    else:
        json_lines.append("  }")

json_lines.append("}")

# 将拼装好的字符串写入文件
with open(output_json_path, 'w', encoding='utf-8') as f:
    f.write("\n".join(json_lines))

print(f"JSON 已生成至: {output_json_path}")