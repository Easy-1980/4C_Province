// 全局数据与状态管理
let globalProvinceData = {};
let globalWordAnalysis = {};
let globalDashboardData = {};
let globalVideoAnalysisData = {};
let currentActiveProvince = '全国'; 
let currentActiveFullName = ''; 

// AI 文本暂存变量
let currentDanmakuAI = { insight: "", decision: "" };
let currentTgiAnalysis = ""; 

// 地图全局散点数据（供取消选中时使用）
let globalScatterData = []; 

// 提取主题色系
const themeColors = ['#00eaff', '#0075ff', '#ffb020', '#ff4d4f', '#00ffaa'];

// 地图坐标与名称映射字典
const geoCoordMap = {
    "北京": [116.46, 39.92], "天津": [117.2, 39.13], "河北": [114.48, 38.03],
    "山西": [111.53, 36.87], "内蒙古": [110.65, 41.9], "辽宁": [123.38, 41.8],
    "吉林": [125.35, 43.88], "黑龙江": [126.63, 45.75], "上海": [121.48, 31.22],
    "江苏": [118.78, 33.04], "浙江": [120.19, 29.26], "安徽": [117.27, 31.86],
    "福建": [118.3, 26.08], "江西": [114.89, 28.68], "山东": [117.0, 36.65],
    "河南": [112.65, 33.76], "湖北": [112.31, 30.52], "湖南": [111.0, 28.21],
    "广东": [113.23, 23.16], "广西": [108.0, 22.84], "海南": [110.35, 20.02],
    "重庆": [106.54, 29.59], "四川": [103.06, 30.67], "贵州": [106.71, 26.57],
    "云南": [101.73, 25.04], "西藏": [91.11, 29.97], "陕西": [108.95, 34.27],
    "甘肃": [103.73, 36.03], "青海": [101.74, 36.56], "宁夏": [106.27, 38.47],
    "新疆": [87.68, 43.77], "台湾": [121.5, 25.04], "香港": [114.17, 22.28],
    "澳门": [113.54, 22.19]
};

const getMapFullName = (shortName) => {
    const specialMap = {
        "北京": "北京市", "天津": "天津市", "上海": "上海市", "重庆": "重庆市",
        "内蒙古": "内蒙古自治区", "广西": "广西壮族自治区", "西藏": "西藏自治区",
        "宁夏": "宁夏回族自治区", "新疆": "新疆维吾尔自治区", 
        "香港": "香港特别行政区", "澳门": "澳门特别行政区", "台湾": "台湾省"
    };
    return specialMap[shortName] || shortName + "省";
};

const getShortProvinceName = (name) => String(name || '')
    .replace(/省|市|维吾尔自治区|壮族自治区|回族自治区|自治区|特别行政区/g, '')
    .trim();

const toNumber = (value, fallback = 0) => {
    const num = Number(value);
    return Number.isFinite(num) ? num : fallback;
};

const normalizeWordCloudItems = (items) => {
    const list = Array.isArray(items) ? items : [];
    return list.map(item => {
        const word = String((item && (item.name || item.word)) || '').trim();
        const value = Math.max(0, Math.round(toNumber(item && (item.value ?? item.count), 0)));
        return {
            name: word,
            value: value,
            word: word,
            count: value,
            sentiment: String((item && item.sentiment) || '中性'),
            analysis: String((item && item.analysis) || '暂无分析'),
            score: String((item && item.score) || '0.00')
        };
    }).filter(item => item.name);
};

const buildWordAnalysisIndex = (wordCloudItems) => {
    const map = {};
    (Array.isArray(wordCloudItems) ? wordCloudItems : []).forEach(item => {
        if (!item || !item.name) return;
        map[item.name] = {
            score: String(item.score || '0.00'),
            sentiment: String(item.sentiment || '中性'),
            analysis: String(item.analysis || '暂无分析')
        };
    });
    return map;
};

const getRadarScores = (radarObj) => {
    const scores = radarObj && Array.isArray(radarObj.scores) ? radarObj.scores : [];
    if (!scores.length) return [60, 60, 60, 60, 60, 60];
    return scores.map(v => toNumber(v, 60));
};

const buildLegacyTgiData = (audiencePortrait, tgiList, tgiAnalysis) => {
    const portrait = audiencePortrait && typeof audiencePortrait === 'object' ? audiencePortrait : {};
    const tgiArray = Array.isArray(tgiList) ? tgiList : [];
    const ageDistribution = portrait.ageDistribution && typeof portrait.ageDistribution === 'object'
        ? portrait.ageDistribution
        : {};
    const genderRatio = portrait.genderRatio && typeof portrait.genderRatio === 'object'
        ? portrait.genderRatio
        : {};

    const ageCategories = Array.isArray(ageDistribution.categories) ? ageDistribution.categories : [];
    const agePercent = Array.isArray(ageDistribution.values)
        ? ageDistribution.values.map(v => toNumber(v, 0))
        : ageCategories.map(() => 0);

    const ageTgiMap = {};
    const genderTgiMap = {};
    tgiArray.forEach(row => {
        if (!row || typeof row !== 'object') return;
        const group = String(row.group || '');
        const category = String(row.category || '');
        const value = toNumber(row.tgi, 0);
        if (group === '年龄') ageTgiMap[category] = value;
        if (group === '性别') genderTgiMap[category] = value;
    });

    const analysisObj = tgiAnalysis && typeof tgiAnalysis === 'object' ? tgiAnalysis : {};
    const analysisText = String(
        analysisObj.analysis
        || analysisObj.insight
        || '暂无分析'
    );

    return {
        analysis: analysisText,
        age: {
            categories: ageCategories,
            percent: agePercent,
            tgi: ageCategories.map(cat => toNumber(ageTgiMap[cat], 0))
        },
        gender: {
            categories: ['男性', '女性'],
            percent: [
                toNumber(genderRatio.male, 0),
                toNumber(genderRatio.female, 0)
            ],
            tgi: [
                toNumber(genderTgiMap['男性'], 0),
                toNumber(genderTgiMap['女性'], 0)
            ]
        }
    };
};

const expandLabelsFromCountMap = (countMap) => {
    const result = [];
    const obj = countMap && typeof countMap === 'object' ? countMap : {};
    Object.keys(obj).forEach(label => {
        const count = Math.max(0, Math.floor(toNumber(obj[label], 0)));
        for (let i = 0; i < count; i += 1) result.push(label);
    });
    return result;
};

const buildOperaObjects = (provinceData) => {
    const rawNames = Array.isArray(provinceData && provinceData.operas) ? provinceData.operas : [];
    const operaNames = rawNames
        .map(name => String(name || '').trim())
        .filter(Boolean);
    if (!operaNames.length) return [];

    const dynastyPool = expandLabelsFromCountMap(provinceData && provinceData.originDynasty);
    const levelPool = expandLabelsFromCountMap(provinceData && provinceData.heritageLevel);
    return operaNames.map((name, index) => {
        const dynasty = dynastyPool.length ? dynastyPool[index % dynastyPool.length] : '未知';
        const level = levelPool.length ? levelPool[index % levelPool.length] : '未计入';
        return {
            name: name,
            dynasty: dynasty,
            dynastyBucket: dynasty,
            level: level
        };
    });
};

const buildLegacyDanmakuTrend = (video) => {
    if (!video || typeof video !== 'object') {
        return {
            operaName: '暂未录入代表剧目',
            times: ['00:00', '00:10', '00:20', '00:30'],
            counts: [0, 0, 0, 0],
            maxDanmakus: [],
            aiInsight: '暂无分析',
            decision: '暂无建议'
        };
    }
    const trend = video.danmakuTrend && typeof video.danmakuTrend === 'object' ? video.danmakuTrend : {};
    const ai = video.aiAnalysis && typeof video.aiAnalysis === 'object' ? video.aiAnalysis : {};
    const opera = String(video.opera || '未知剧种').trim();
    const bvid = String(video.bvid || '').trim();
    return {
        operaName: `${opera}${bvid ? ` - ${bvid}` : ''}`,
        times: Array.isArray(trend.times) ? trend.times : [],
        counts: Array.isArray(trend.counts) ? trend.counts : [],
        maxDanmakus: Array.isArray(trend.maxDanmakus) ? trend.maxDanmakus : [],
        aiInsight: String(ai.insight || '暂无分析'),
        decision: String(ai.advice || '暂无建议')
    };
};

const pickBestVideoByProvince = (videos, provinceName) => {
    const target = getShortProvinceName(provinceName);
    const filtered = (Array.isArray(videos) ? videos : []).filter(video => {
        return getShortProvinceName(video && video.province) === target;
    });
    if (!filtered.length) return null;
    filtered.sort((a, b) => {
        const scoreA = toNumber(a && a.indexes && a.indexes.score, 0);
        const scoreB = toNumber(b && b.indexes && b.indexes.score, 0);
        return scoreB - scoreA;
    });
    return filtered[0];
};

const normalizeMapData = (nationalMapData, provinceObj) => {
    const mapArray = Array.isArray(nationalMapData) ? nationalMapData : [];
    let source = mapArray;
    if (!source.length) {
        const provinces = provinceObj && typeof provinceObj === 'object' ? provinceObj : {};
        source = Object.keys(provinces).map(name => ({
            name: name,
            value: toNumber(provinces[name] && provinces[name].operaCount, 0)
        }));
    }
    return source
        .map(item => ({
            name: getShortProvinceName(item && item.name),
            value: Math.max(0, toNumber(item && (item.value ?? item.operaCount), 0))
        }))
        .filter(item => item.name)
        .sort((a, b) => b.value - a.value);
};

const buildTopProvinceData = (mapData) => {
    const top10 = (Array.isArray(mapData) ? mapData : [])
        .slice()
        .sort((a, b) => toNumber(b.value, 0) - toNumber(a.value, 0))
        .slice(0, 10);
    return {
        names: top10.map(item => item.name),
        values: top10.map(item => toNumber(item.value, 0))
    };
};

const buildLegacyProvinceData = (dashboardData, videoAnalysisData) => {
    const dashboard = dashboardData && typeof dashboardData === 'object' ? dashboardData : {};
    const nationalRaw = dashboard.national && typeof dashboard.national === 'object'
        ? dashboard.national
        : {};
    const provincesRawInput = dashboard.provinces && typeof dashboard.provinces === 'object'
        ? dashboard.provinces
        : {};
    const provincesRaw = {};
    Object.keys(provincesRawInput).forEach(name => {
        const shortName = getShortProvinceName(name);
        if (!shortName) return;
        provincesRaw[shortName] = provincesRawInput[name];
    });
    const videos = Array.isArray(videoAnalysisData && videoAnalysisData.videos)
        ? videoAnalysisData.videos
        : [];

    const mapData = normalizeMapData(nationalRaw.mapData, provincesRaw);
    const legacy = {};

    const nationalWordCloud = normalizeWordCloudItems(nationalRaw.wordCloud);
    const nationalAudience = nationalRaw.audiencePortrait && typeof nationalRaw.audiencePortrait === 'object'
        ? nationalRaw.audiencePortrait
        : {};
    legacy['全国'] = {
        mapData: mapData,
        topProvinces: buildTopProvinceData(mapData),
        wordCloud: nationalWordCloud,
        radarData: getRadarScores(nationalRaw.radarScores),
        ageGender: nationalAudience.ageGender && typeof nationalAudience.ageGender === 'object'
            ? nationalAudience.ageGender
            : { categories: [], male: [], female: [] },
        tgiData: buildLegacyTgiData(nationalAudience, nationalRaw.tgi, nationalRaw.tgiAnalysis),
        tgi: Array.isArray(nationalRaw.tgi) ? nationalRaw.tgi : [],
        tgiAnalysis: nationalRaw.tgiAnalysis || {},
        operas: [],
        allOperas: []
    };

    const provinceNameSet = new Set([
        ...Object.keys(provincesRaw),
        ...mapData.map(item => item.name)
    ]);

    provinceNameSet.forEach(provinceName => {
        const raw = provincesRaw[provinceName] && typeof provincesRaw[provinceName] === 'object'
            ? provincesRaw[provinceName]
            : {};
        const mapItem = mapData.find(item => item.name === provinceName);
        const operaObjects = buildOperaObjects(raw);
        const originDynasty = raw.originDynasty && typeof raw.originDynasty === 'object'
            ? raw.originDynasty
            : {};
        const dynastyNames = Object.keys(originDynasty);
        const dynastyCounts = dynastyNames.map(name => toNumber(originDynasty[name], 0));
        const audiencePortrait = raw.audiencePortrait && typeof raw.audiencePortrait === 'object'
            ? raw.audiencePortrait
            : {};
        const bestVideo = pickBestVideoByProvince(videos, provinceName);

        legacy[provinceName] = {
            operaCount: Math.max(
                toNumber(raw.operaCount, 0),
                mapItem ? toNumber(mapItem.value, 0) : 0
            ),
            operas: operaObjects,
            allOperas: operaObjects,
            heritageLevel: raw.heritageLevel && typeof raw.heritageLevel === 'object' ? raw.heritageLevel : {},
            originDynasty: originDynasty,
            dynastyDistribution: {
                dynasties: dynastyNames,
                counts: dynastyCounts
            },
            wordCloud: normalizeWordCloudItems(raw.wordCloud),
            radarData: getRadarScores(raw.radarScores),
            ageGender: audiencePortrait.ageGender && typeof audiencePortrait.ageGender === 'object'
                ? audiencePortrait.ageGender
                : { categories: [], male: [], female: [] },
            tgi: Array.isArray(raw.tgi) ? raw.tgi : [],
            tgiAnalysis: raw.tgiAnalysis || {},
            tgiData: buildLegacyTgiData(audiencePortrait, raw.tgi, raw.tgiAnalysis),
            danmakuTrend: buildLegacyDanmakuTrend(bestVideo),
            spreadStructure: raw.spreadStructure || {}
        };
    });

    return legacy;
};
