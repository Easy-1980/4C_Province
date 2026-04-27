戏韵传数：基于 NLP 与 LLM 的中国传统戏曲受众行为图谱可视化平台

1. 项目背景与核心价值
本项目致力于解决传统文化在数字化传播中的“受众盲区”问题。我们不仅整合了全国 34 个代表性剧种的基础档案，更创新性地引入了 B站海量弹幕/评论数据与受众 TGI 画像指数。

通过 NLP（自然语言处理）技术和阿里云通义千问（Qwen）大模型的深度赋能，本项目将原本扁平的统计数据，升维成了具有情感温度和营销指导意义的“受众心理图谱”，为传统戏曲的“破圈”传播提供精准的 AI 数据支持。

2. 核心技术亮点

    1. 构建自动化数据中台 (ETL Pipeline)
        - 编写多维爬虫脚本，抓取视频热点弹幕、评论及用户受众画像（TGI）。
        - 基于 Python (Pandas) 构建了一套自动化的数据流转管线，实现数据清洗、整合与 JSON 序列化输出。
    2. NLP 语义映射与多维雷达算法
        - 摒弃主观打分，通过 `Jieba` 分词构建 6 大维度的“戏曲传播语义映射词典”（如：服化道、二创整活、文化底蕴等）。
        - 对数万条评论进行对数平滑与区间归一化计算，自动生成客观、真实的雷达图画像。
    3. 大语言模型 (LLM) 离线预推理赋能
        - 接入阿里云 Qwen API，根据弹幕高频词和 TGI 数据，自动生成深度的“受众心理洞察 (aiInsight)”与“宣发建议 (decision)”。
        - 采用“后端预计算 + 前端秒加载”的架构，兼顾了大模型的智能性与前端交互的极致流畅（彻底解决演示延迟与跨域报错问题）。
    4. 模块化前端架构
        - 采用 CSS Grid 矩阵布局与 CSS 变量系统，实现极简且高可维护的科技风大屏。
        - 彻底重构 Vanilla JS，将数据层 (data.js)、视图层 (ui.js)、图表层 (charts.js) 与控制台 (main.js) 解耦，实现组件的高效复用。

3. 工程目录结构

ChineseOpera_BigData/
│
├── dataProcess/                 # 数据处理（数据抓取、清洗、AI赋能）
│   ├── dataProcess_Scripts/     # Python 核心脚本群
│   │   ├── audiencePortrait.py  # 受众画像 TGI 处理
│   │   ├── getData_bilibili.py  # B站弹幕/评论抓取
│   │   ├── Merge.py             # 核心数据合并
│   │   ├── nationalWordCloud.py # NLP 弹幕分词与词云生成
│   │   ├── Qwen_Analysis.py     # 调用通义千问 API 生成洞察
│   │   ├── radarScores.py       # 基于 NLP 的雷达图维度打分
│   │   ├── refineOpera.py       # 基础剧种档案清洗
│   │   └── searchTop_danmaku.py # 高能弹幕爆点提取
│   ├── finalData/               # 最终输出给前端的成品数据
|   |   ├── danmakuWordAnalysis.json # 词云 AI 解析数据
│   │   └── Province_data.json   # 综合大盘数据
│   ├── tempData/                # 中间数据
│   │   ├── 1_base_operas.json   # 提取出的基础数据
│   │   ├── 2_danmakuTrend.json  # 高能弹幕数据
│   │   ├── 3_Portrait.json      # 受众人群画像数据
│   │   ├── 4_radarScores.json   # 雷达图数据
│   │   ├── nationalWordCloud_Selected.json # 人为筛选过的数据
│   │   ├── nationalWordCloud.json  # NLP 输出的词云数据
│   │   └── Province_data.json   # 合并得到的大盘数据
│   └── rawData/                 # 原始 Excel 与 CSV 数据池
│       ├── allOperas_Unprocessed.xlsx  # 剧种未处理全表
│       ├── audiencePortrait.xlsx       # 受众画像原始数据
│       ├── bilibili_tasks.xlsx         # B站爬虫任务列表
│       └── getData_bilibili/           # 爬虫抓取落库文件夹
│           ├── comments_data.csv       # 原始评论库
│           ├── danmaku_data.csv        # 原始弹幕库
│           └── video_info.csv          # 视频基础信息库
|
└── Frontend/                    # 前端展示层（纯静态离线大屏）
    ├── index.html               # 大屏主入口
    ├── css/
    │   └── style.css            # 网页样式
    ├── data/                    # 前端静态数据源
    │   ├── China.geojson        # 中国地图 GeoJSON 数据源，来自高德开放平台 (阿里云 DataV) | 审图号：GS(2025)5996
    │   ├── Province_data.json   # 综合大盘数据
    │   └── danmakuWordAnalysis.json # 词云 AI 解析数据
    └── js/                      # 模块化 JS 架构
        ├── charts.js            # ECharts 渲染配置中心
        ├── data.js              # 全局状态与数据字典
        ├── main.js              # 全局调度引擎
        ├── ui.js                # 弹窗交互与 DOM 控制
        ├── echarts.min.js       # ECharts 核心库 (离线本地化)
        ├── echarts.min_1.js       # ECharts 核心库 (优化后，去除了多余库)
        └── echarts-wordcloud.min.js # 词云插件 (离线本地化)