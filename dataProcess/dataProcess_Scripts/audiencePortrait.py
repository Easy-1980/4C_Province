# 输入：audiencePortrait.xlsx（包含各省代表剧种及其受众数据）
# 输出：3_Portrait.json（包含每个省份第一个代表剧种的受众数据，适用于 TGI 弹窗展示）

import pandas as pd
import json

# 1. 配置输入输出文件名
input_excel = "audiencePortrait.xlsx" 
output_json = "3_Portrait.json"

print(f"正在读取 Excel 文件: {input_excel} ...")

try:
    # 使用 read_excel 读取 .xlsx 文件
    df = pd.read_excel(input_excel)
except Exception as e:
    print(f"读取失败，请检查文件名或确保已安装 openpyxl 库: {e}")
    exit()

# 2. 清洗数据：向下填充省份列
df['省份'] = df['省份'].ffill()

# 3. 核心需求：提取每个省份的“第一个”代表剧种
df_first = df.groupby('省份').first().reset_index()

final_output = {}

# 4. 遍历提取后的数据，构建 JSON 结构
for index, row in df_first.iterrows():
    prov = str(row['省份']).strip()
    opera_name = str(row['各省代表剧种']).strip()
    
    # --- A. 提取 TGI 弹窗需要的原数据 ---
    age_categories = ["≤19岁", "20-29岁", "30-39岁", "40-49岁", "≥50岁"]
    age_percents = [row['≤19岁占比'], row['20-29岁占比'], row['30-39岁占比'], row['40-49岁占比'], row['≥50岁占比']]
    age_tgis = [row['≤19岁TGI'], row['20-29岁TGI'], row['30-39岁TGI'], row['40-49岁TGI'], row['≥50岁TGI']]
    
    gender_categories = ["男性", "女性"]
    gender_percents = [row['男性占比'], row['女性占比']]
    gender_tgis = [row['男性TGI'], row['女性TGI']]
    
    # --- B. 智能生成数据洞察文案 (加分项) ---
    max_age_tgi = max(age_tgis)
    max_age_name = age_categories[age_tgis.index(max_age_tgi)]
    
    max_gender_tgi = max(gender_tgis)
    max_gender_name = gender_categories[gender_tgis.index(max_gender_tgi)]
    
    analysis_text = (f"【数据洞察】：百度指数显示，{opera_name}在【{max_gender_name}】群体中表现出更高偏好度（TGI={max_gender_tgi}）；"
                     f"在年龄圈层上，【{max_age_name}】人群关注度最为集中（TGI={max_age_tgi}）。")
    
    # --- C. 计算右下角横向堆叠柱状图所需的拆分数据 ---
    # 前端 y 轴是倒序的 ["≥50岁", "40-49岁", "30-39岁", "20-29岁", "≤19岁"]
    male_ratio = row['男性占比'] / 100
    female_ratio = row['女性占比'] / 100
    
    age_gender_categories = ["≥50岁", "40-49岁", "30-39岁", "20-29岁", "≤19岁"]
    male_stack = [
        round(row['≥50岁占比'] * male_ratio, 2), round(row['40-49岁占比'] * male_ratio, 2),
        round(row['30-39岁占比'] * male_ratio, 2), round(row['20-29岁占比'] * male_ratio, 2),
        round(row['≤19岁占比'] * male_ratio, 2)
    ]
    female_stack = [
        round(row['≥50岁占比'] * female_ratio, 2), round(row['40-49岁占比'] * female_ratio, 2),
        round(row['30-39岁占比'] * female_ratio, 2), round(row['20-29岁占比'] * female_ratio, 2),
        round(row['≤19岁占比'] * female_ratio, 2)
    ]

    # --- D. 组装当前省份的数据 ---
    final_output[prov] = {
        "ageGender": {
            "categories": age_gender_categories,
            "male": male_stack,
            "female": female_stack
        },
        "tgiData": {
            "age": {
                "categories": age_categories,
                "percent": age_percents,
                "tgi": age_tgis
            },
            "gender": {
                "categories": gender_categories,
                "percent": gender_percents,
                "tgi": gender_tgis
            },
            "analysis": analysis_text
        }
    }

# 5. 导出 JSON 文件
with open(output_json, 'w', encoding='utf-8') as f:
    json.dump(final_output, f, ensure_ascii=False, indent=2)

print(f"提取成功！已为 {len(df_first)} 个省份提取了首个剧种的受众数据。")
print(f"数据已导出至：{output_json}")