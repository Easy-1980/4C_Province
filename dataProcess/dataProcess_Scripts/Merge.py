# 该脚本负责将之前各个模块（底座、弹幕折线、TGI画像、雷达图）生成的JSON数据进行缝合，最终输出一个包含所有信息的综合数据文件，供前端使用。
# 输出了一个名为 Province_data.json 的文件，里面包含了每个省份的基础数据、弹幕趋势、TGI画像和雷达图得分等信息。

import json
import os
import re

# ==========================================
# 0. 配置文件路径
# ==========================================
BASE_FILE = "data/1_base_operas.json"           # 底座数据
DANMAKU_FILE = "data/2_danmakuTrend.json"    # 弹幕折线数据
TGI_FILE = "data/3_Portrait.json"              # TGI 和受众画像文件
RADAR_FILE = "data/4_radarScores.json"         # 雷达图得分文件
FINAL_OUTPUT = "data/Province_data.json"        # 最终给Qwen分析使用的文件

print("开始执行数据中台合并流水线...")

# ==========================================
# 1. 读取各模块数据
# ==========================================
if not os.path.exists(BASE_FILE):
    print(f"找不到底座文件 {BASE_FILE}，请先运行基础数据提取脚本！")
    exit()
with open(BASE_FILE, 'r', encoding='utf-8') as f:
    final_data = json.load(f)

def load_json_safe(filepath, name):
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            print(f"成功加载 [{name}] 数据")
            return json.load(f)
    print(f"未找到 {filepath}，将跳过 {name} 合并。")
    return {}

danmaku_data = load_json_safe(DANMAKU_FILE, "B站弹幕折线")
tgi_data = load_json_safe(TGI_FILE, "受众TGI与画像")
radar_data = load_json_safe(RADAR_FILE, "多维雷达图")

# ==========================================
# 2. 开始缝合数据 (修复了全国被跳过的Bug)
# ==========================================
merge_count_danmaku = 0
merge_count_tgi = 0
merge_count_radar = 0

for prov, data in final_data.items():
    # 1. 拼装弹幕数据
    if prov in danmaku_data and "danmakuTrend" in danmaku_data[prov]:
        data["danmakuTrend"] = danmaku_data[prov]["danmakuTrend"]
        merge_count_danmaku += 1
        
    # 2. 拼装 TGI 与受众数据
    if prov in tgi_data:
        if "tgiData" in tgi_data[prov]:
            data["tgiData"] = tgi_data[prov]["tgiData"]
        if "ageGender" in tgi_data[prov]:
            data["ageGender"] = tgi_data[prov]["ageGender"]
        merge_count_tgi += 1

    # 3. 拼装雷达图数据（现在"全国"也能毫无阻碍地执行这一步了！）
    if prov in radar_data and "radarData" in radar_data[prov]:
        data["radarData"] = radar_data[prov]["radarData"]
        merge_count_radar += 1

# ==========================================
# 3. 排版优化并导出
# ==========================================
json_str = json.dumps(final_data, ensure_ascii=False, indent=2)

# 正则排版优化：折叠数组为单行，保持文件不臃肿
json_str = re.sub(r'\[\s+', '[', json_str)
json_str = re.sub(r',\s+', ', ', json_str) 
json_str = re.sub(r'\s+\]', ']', json_str)

with open(FINAL_OUTPUT, 'w', encoding='utf-8') as f:
    f.write(json_str)

print(f"\n缝合完成！")
print(f"   - 成功接入 {merge_count_danmaku} 个省份的弹幕折线数据")
print(f"   - 成功接入 {merge_count_tgi} 个省份的 TGI 画像数据")
print(f"   - 成功接入 {merge_count_radar} 个省份的雷达图数据 (包含全国！)")
print(f"终极版前端数据已生成，请使用：{FINAL_OUTPUT}")