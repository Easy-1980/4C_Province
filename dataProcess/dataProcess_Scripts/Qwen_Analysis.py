# 该脚本负责调用阿里云 DashScope 的 Qwen 模型对前端数据进行 AI 赋能，生成专业的分析文本和洞察建议。
# 输出结果会直接写回原数据文件（Province_data.json）和生成新的分析文件（danmakuWordAnalysis.json），供前端展示使用。
import json
import requests
import time
import re
import os
import random

# ==========================================
# 0. 全局配置
# ==========================================
# 填入阿里云 DashScope API Key
QWEN_API_KEY = "sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxx"  # 请替换为你的实际 API Key
# 调用qwen-turbo模型，速度更快，成本更低，适合大规模数据分析任务
QWEN_MODEL = "qwen-turbo" 

PROVINCE_FILE = "data/Province_data.json"
WORDCLOUD_FILE = "data/nationalWordCloud_Selected.json"

WORD_ANALYSIS_OUTPUT = "data/danmakuWordAnalysis.json" # 前端读取的文件

# ==========================================
# 通用大模型调用函数
# ==========================================
def ask_qwen(prompt, system_role="你是一个资深的大数据分析师与戏曲文化推广专家。"):
    url = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {QWEN_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": QWEN_MODEL,
        "messages": [
            {"role": "system", "content": system_role},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=15)
        response.raise_for_status()
        result = response.json()
        return result['choices'][0]['message']['content'].strip()
    except Exception as e:
        print(f"API 请求失败: {e}")
        return None

# ==========================================
# 任务 1：处理省份数据 (TGI分析 & 弹幕爆点洞察)
# ==========================================
def process_provinces():
    print("\n开始执行任务 1：省份大盘 TGI 与弹幕爆点 AI 深度分析...")
    
    if not os.path.exists(PROVINCE_FILE):
        print(f"找不到 {PROVINCE_FILE}，跳过此任务。")
        return

    with open(PROVINCE_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    for prov, prov_data in data.items():
        print(f"\n正在思考 [{prov}] 的数据...")
        
        # --- 1.1 TGI 画像分析 ---
        if "tgiData" in prov_data:
            tgi = prov_data["tgiData"]
            age_cats = tgi["age"]["categories"]
            age_tgis = tgi["age"]["tgi"]
            gender_cats = tgi["gender"]["categories"]
            gender_tgis = tgi["gender"]["tgi"]
            
            prompt_tgi = f"""
            已知{prov}代表剧种的受众百度指数TGI数据如下：
            - 年龄段TGI：{dict(zip(age_cats, age_tgis))}
            - 性别TGI：{dict(zip(gender_cats, gender_tgis))}
            请写一段80字以内的“数据洞察”，要求语气专业客观，指出哪个群体偏好度最高，并简要分析原因。
            格式要求：直接输出文字，不要包含“【数据洞察】”字样，不要换行。
            """
            analysis_text = ask_qwen(prompt_tgi)
            if analysis_text:
                prov_data["tgiData"]["analysis"] = f"{analysis_text}"
                print("  TGI 分析生成完毕")
            time.sleep(1) # 避免触发并发限制

        # --- 1.2 弹幕爆点洞察 (aiInsight & decision) ---
        if "danmakuTrend" in prov_data and prov_data["danmakuTrend"]["maxDanmakus"]:
            trend = prov_data["danmakuTrend"]
            opera_name = trend["operaName"]
            max_danmakus = trend["maxDanmakus"][:15] # 取前15条最具代表性的
            
            prompt_danmaku = f"""
            在B站关于{opera_name}的视频中，某个高能瞬间涌现了大量弹幕，典型弹幕包括：{max_danmakus}。
            请以此分析受众心理，并给传统戏曲创作者提供建议。
            严格按照以下格式输出（保留前缀标识），总字数控制在150字以内：
            洞察：[在这里写受众为什么发这些弹幕的心理分析]
            建议：[在这里写给创作者的二创或宣发建议]
            """
            result_text = ask_qwen(prompt_danmaku)
            if result_text:
                # 使用正则或字符串分割提取两部分
                insight_match = re.search(r'洞察：(.*?)(?=\n建议：|$)', result_text, re.S)
                decision_match = re.search(r'建议：(.*)', result_text, re.S)
                
                if insight_match:
                    trend["aiInsight"] = insight_match.group(1).strip()
                if decision_match:
                    trend["decision"] = decision_match.group(1).strip()
                print("  弹幕洞察与建议生成完毕")
            time.sleep(1)

    # 保存回原文件
    with open(PROVINCE_FILE, 'w', encoding='utf-8') as f:
        # 使用之前的排版优化逻辑保持体积小巧
        json_str = json.dumps(data, ensure_ascii=False, indent=2)
        json_str = re.sub(r'\[\s+', '[', json_str)
        json_str = re.sub(r',\s+', ', ', json_str) 
        json_str = re.sub(r'\s+\]', ']', json_str)
        f.write(json_str)
    print(f"任务 1 完成！已更新 {PROVINCE_FILE}")

# ==========================================
# 任务 2：处理全国词云数据 (带动态打分系统)
# ==========================================
def process_wordcloud():
    print("\n开始执行任务 2：全国弹幕词云 AI 心理剖析...")
    
    if not os.path.exists(WORDCLOUD_FILE):
        print(f"找不到 {WORDCLOUD_FILE}，跳过此任务。")
        return

    with open(WORDCLOUD_FILE, 'r', encoding='utf-8') as f:
        cloud_data = json.load(f)
    
    word_list = cloud_data.get("全国", {}).get("wordCloud", [])
    if not word_list:
        return

    # 只取前 20 个高频词进行 AI 分析
    top_words = [item["name"] for item in word_list[:20]]
    
    analysis_output = {}
    
    for word in top_words:
        print(f"正在分析词汇 [{word}]...")
        # 【修改点1】：让大模型只输出整数，并给出清晰的打分梯队
        prompt_word = f"""
        “{word}” 是B站戏曲视频弹幕中的高频词汇。
        请对这个词进行深度情感分析。严格按以下 JSON 格式输出：
        {{
            "score_int": 请结合该词的情感给出 -100 到 100 的整数打分。(规则：极其震撼/惊艳类给 90-99，普通赞美给 75-89，搞笑解压给 50-74，中性探讨给 10-49，负面给负数),
            "sentiment": "极度正向/高度喜爱/幽默解压/中立探讨/负面吐槽 等4个字的精准概括",
            "analysis": "结合当代年轻人心理，分析为什么在看戏曲时会发这个词，字数50字以内。"
        }}
        """
        result_text = ask_qwen(prompt_word)
        if result_text:
            try:
                clean_json = result_text.replace('```json', '').replace('```', '').strip()
                parsed_res = json.loads(clean_json)
                
                # 【修改点2】：打分引擎核心逻辑
                raw_score = int(parsed_res.get("score_int", 80)) # 获取大模型的整数打分
                jitter = random.randint(-4, 4) # 引入微小扰动，打破同质化
                
                final_score = (raw_score + jitter) / 100.0
                final_score = max(-1.0, min(1.0, final_score)) # 确保不会超过极限值
                
                # 转换回前端需要的格式，例如 "+0.92"
                parsed_res["score"] = f"{'+' if final_score > 0 else ''}{final_score:.2f}"
                
                # 删除多余的整数键
                if "score_int" in parsed_res:
                    del parsed_res["score_int"]
                    
                analysis_output[word] = parsed_res
                print(f"  分析成功 | AI基准分:{raw_score} -> 最终得分:{parsed_res['score']}")
                
            except Exception as e:
                print(f"  JSON 解析失败，跳过: {e}")
        time.sleep(1)

    os.makedirs(os.path.dirname(WORD_ANALYSIS_OUTPUT) or '.', exist_ok=True)
    with open(WORD_ANALYSIS_OUTPUT, 'w', encoding='utf-8') as f:
        json.dump(analysis_output, f, ensure_ascii=False, indent=2)
    print(f"任务 2 完成！已生成带有动态打分系统的 {WORD_ANALYSIS_OUTPUT}")

# ==========================================
# 执行流水线
# ==========================================
if __name__ == "__main__":
    if QWEN_API_KEY == "sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxx":
        print("警告：请先在代码中填入 Qwen API Key！")
    else:
        process_provinces()
        process_wordcloud()
        print("\n AI 数据赋能任务完成！")