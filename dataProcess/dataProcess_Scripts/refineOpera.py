# 这个脚本的目标是从 Excel 中提取全国各省的戏曲剧种数据，进行清洗和归类，并生成一个结构化的 JSON 文件。
# 输出了一个基础的 JSON 数据结构，包含了全国 Top 10 省份的剧种数量排行榜，以及每个省份的朝代分布和剧种名录————1_base_operas.json。

import pandas as pd
import re
import json

# 1. 读取单个 Excel 总表
excel_file = "allOperas_Unprocessed.xlsx"
try:
    df = pd.read_excel(excel_file)
    # 向下填充“省份”列，解决 Excel 中的合并单元格问题
    df['省份'] = df['省份'].ffill()
except Exception as e:
    print(f"读取 Excel 失败，请检查文件是否存在且未被打开: {e}")
    exit()

# 2. 辅助函数：解析省份和总数 (如 "河南（28）" -> "河南", 28)
def parse_province(text):
    if pd.isna(text): return None, 0
    text = str(text).strip()
    match = re.search(r'(.+?)(?:省|市|维吾尔自治区|壮族自治区|回族自治区|自治区|特别行政区)?\s*[（\(](\d+)[）\)]', text)
    if match:
        return match.group(1).strip(), int(match.group(2))
    return text.replace('省', '').replace('市', ''), 0

# 3. 辅助函数：朝代归类 (将民国和现代合并为“近现代”)
def map_dynasty(time_str):
    time_str = str(time_str).strip()
    if pd.isna(time_str) or time_str == 'nan': return "未知"
    if any(x in time_str for x in ['宋', '元', '金', '汉', '唐']): return "元代" 
    if '明' in time_str: return "明代"
    if '清' in time_str or '十九世纪' in time_str: return "清代"
    if any(x in time_str for x in ['民国', '19', '20', '现代', '近现代', '二十世纪']): return "近现代"
    return "其他"

# 3.1 辅助函数：清洗并统一产生年代展示格式
def clean_dynasty_text(text):
    if pd.isna(text) or str(text).strip() in ['', 'nan']:
        return "未知"
    text = str(text).strip()
    text = re.sub(r'[\(（\)）\s]', '', text)
    text = re.sub(r'(唐|宋|金|元|明|清|汉)中叶', r'\1代中叶', text)
    match = re.search(r'(唐宋|宋金|宋元|金元|元明|明清)', text)
    if match:
        text = match.group(1)[0]
    core_text = text.replace('时期', '').replace('之际', '')
    if core_text in ['唐', '宋', '金', '元', '明', '清', '汉']:
        return core_text + "代"
    if core_text in ['唐代', '宋代', '金代', '元代', '明代', '清代', '汉代']:
        return core_text
    return text

# 4. 辅助函数：解析非遗级别 (适配新的“级别”表头)
def parse_ich(text):
    if pd.isna(text) or text == 'nan': return False, ""
    text = str(text)
    if '人类' in text or '世界' in text: return True, "世界级"
    if '国家' in text: return True, "国家级"
    if '省' in text: return True, "省级"
    if '市' in text: return True, "市级"
    return True, "未计入"

# ==================== 开始构建 JSON ====================

# 提取全国 Top 10 省份剧种数量
province_counts = {}
for text in df['省份'].unique():
    name, count = parse_province(text)
    if name: province_counts[name] = count

sorted_provinces = sorted(province_counts.items(), key=lambda x: x[1])
top_10 = sorted_provinces[-10:]

# 全国数据只保留排行榜和预留的词云，剔除假数据
final_json = {
    "全国": {
        "topProvinces": {
            "names": [x[0] for x in top_10],
            "values": [x[1] for x in top_10]
        },
        "wordCloud": [{"name": "戏曲", "value": 100}, {"name": "文化", "value": 80}]
    }
}

# 按照解析后的省份名称分组
df['parsed_prov'] = df['省份'].apply(lambda x: parse_province(x)[0])

for prov, group in df.groupby('parsed_prov'):
    if not prov: continue

    operas = []
    dynasty_counts = {"元代": 0, "明代": 0, "清代": 0, "近现代": 0}

    for _, row in group.iterrows():
        raw_time = str(row['产生时间']).strip()
        cleaned_time = clean_dynasty_text(raw_time)
        
        dynasty_bucket = map_dynasty(cleaned_time)
        if dynasty_bucket in dynasty_counts:
            dynasty_counts[dynasty_bucket] += 1

        name = str(row['剧种']).strip()
        match = re.search(r'^([^\(（]+?)\s*([\(（])([^\)）]+)([\)）])', name)
        if match:
            inside_text = match.group(3).strip()
            if any(inside_text.endswith(kw) for kw in ['戏', '剧', '腔', '调', '词', '歌', '落', '传', '梆', '曲', '子']):
                name = name.replace(match.group(0), inside_text)

        ich_raw = str(row['级别']).strip() if '级别' in row else ""
        is_ich, level = parse_ich(ich_raw)

        operas.append({
            "name": name,
            "dynasty": cleaned_time,
            "dynastyBucket": dynasty_bucket,
            "isICH": is_ich,
            "level": level
        })

    top_3_operas = operas[:3]

    # 各省数据只输出基础的朝代分布和剧种名录，极其清爽
    final_json[prov] = {
        "dynastyDistribution": {
            "dynasties": ["元代", "明代", "清代", "近现代"],
            "counts": [dynasty_counts["元代"], dynasty_counts["明代"], dynasty_counts["清代"], dynasty_counts["近现代"]]
        },
        "operas": top_3_operas,
        "allOperas": operas
    }

# 改名为流水线底座文件
output_json = '1_base_operas.json'
with open(output_json, 'w', encoding='utf-8') as f:
    json.dump(final_json, f, ensure_ascii=False, indent=2)

print(f"基础数据生成大成功！已抓取 {len(province_counts)} 个省份的数据。")
print(f"请将其作为底座，用于后续与弹幕、TGI 数据的合并。输出文件: {output_json}")