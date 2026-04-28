// 1.核心数据并发加载引擎
myChartMap.showLoading({ text: '核心数据融合加载中...', color: '#ffb020', textColor: '#fff', maskColor: 'rgba(6, 13, 31, 0.8)' });

const fetchJsonWithFallback = async (paths) => {
    let lastError = null;
    for (const path of paths) {
        try {
            const resp = await fetch(path);
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            return await resp.json();
        } catch (err) {
            lastError = err;
        }
    }
    throw lastError || new Error('JSON 加载失败');
};

Promise.all([
    fetchJsonWithFallback([
        '../dataProcess/output/dashboard_data.json',
        '/dataProcess/output/dashboard_data.json',
        'dataProcess/output/dashboard_data.json'
    ]),
    fetchJsonWithFallback([
        '../dataProcess/output/video_analysis.json',
        '/dataProcess/output/video_analysis.json',
        'dataProcess/output/video_analysis.json'
    ]),
    fetch('data/China.geojson').then(res => res.json())
]).then(([dashboardData, videoAnalysisData, geoJson]) => {
    myChartMap.hideLoading();

    // 保留原始 JSON，供图表层兜底读取
    globalDashboardData = dashboardData && typeof dashboardData === 'object' ? dashboardData : {};
    globalVideoAnalysisData = videoAnalysisData && typeof videoAnalysisData === 'object' ? videoAnalysisData : {};
    
    // 适配新 JSON 到当前静态页原有字段结构
    globalProvinceData = buildLegacyProvinceData(globalDashboardData, globalVideoAnalysisData);
    const nationalData = globalProvinceData['全国'] || {};
    const nationalRaw = globalDashboardData.national || {};
    const fallbackWordCloud = normalizeWordCloudItems(nationalRaw.wordCloud || []);
    const wordCloudForAnalysis = (nationalData.wordCloud && nationalData.wordCloud.length)
        ? nationalData.wordCloud
        : fallbackWordCloud;
    globalWordAnalysis = buildWordAnalysisIndex(wordCloudForAnalysis);

    // 1. 渲染地图
    renderMap(geoJson);

    // 2. 初始化大盘图表
    updateLeftTopChart('全国', nationalData);
    updateRightTopChart('全国', nationalData);
    updateRadarChart('全国', nationalData);
    updateBarChart('全国', nationalData);
}).catch(err => {
    console.error('系统数据源加载失败，请检查文件路径！', err);
    myChartMap.hideLoading();
});

// 2.绑定图表核心点击事件
// 右上角词云 / 折线图大头针点击
myChartRightTop.on('click', function (params) {
    if (currentActiveProvince === '全国') {
        const word = params.name;
        const nlpData = globalWordAnalysis[word]; 

        document.getElementById('modal-word').innerText = word;
        
        if (nlpData) {
            document.getElementById('modal-score').innerText = nlpData.score;
            document.getElementById('modal-sentiment').innerText = nlpData.sentiment;
            document.getElementById('modal-excerpt').innerText = nlpData.analysis;
            
            const scoreColor = parseFloat(nlpData.score) >= 0 ? '#00eaff' : '#ff4d4f';
            document.getElementById('modal-score').style.color = scoreColor;
            document.getElementById('modal-score').style.textShadow = `0 0 15px ${scoreColor}80`;
        } else {
            document.getElementById('modal-score').innerText = 'N/A';
            document.getElementById('modal-sentiment').innerText = '未知情绪';
            document.getElementById('modal-excerpt').innerText = '暂无该高频词的 AI 深度解析数据。';
            document.getElementById('modal-score').style.color = '#a1b0c8';
            document.getElementById('modal-score').style.textShadow = 'none';
        }
        document.getElementById('nlp-modal').style.display = 'flex';
    } else {
        if (params.componentType === 'markPoint') {
            const provinceData = globalProvinceData[currentActiveProvince];
            if (provinceData && provinceData.danmakuTrend && provinceData.danmakuTrend.maxDanmakus) {
                const trend = provinceData.danmakuTrend;
                let html = '';
                trend.maxDanmakus.forEach(text => {
                    html += `<div style="padding: 10px 0; border-bottom: 1px dashed rgba(255,255,255,0.1); color: #e0e6ed; font-size: 15px; display: flex; align-items: flex-start;">
                                <span style="color: #ff2277; margin-right: 8px;">💬</span><span>${text}</span>
                            </div>`;
                });
                document.getElementById('danmaku-list-body').innerHTML = html;
                
                currentDanmakuAI.insight = trend.aiInsight || "模型分析失败，暂无洞察数据。";
                currentDanmakuAI.decision = trend.decision || "模型分析失败，暂无建议数据。";

                document.getElementById('ai-analysis-trigger').style.display = 'block';
                document.getElementById('ai-analysis-loading').style.display = 'none';
                document.getElementById('ai-analysis-result').style.display = 'none';
                document.getElementById('danmaku-modal').style.display = 'flex';
            } else {
                alert(`系统暂未录入【${currentActiveProvince}】的高能时刻弹幕数据`);
            }
        }
    }
});

// 左上角图表点击 (呼出剧种档案馆)
myChartLeftTop.on('click', function (params) {
    const clickName = params.name; 
    let modalTitle = '';
    let targetOperas = [];

    if (currentActiveProvince === '全国') {
        const provData = globalProvinceData[clickName] || {};
        const allOperas = Array.isArray(provData.allOperas) ? provData.allOperas : [];
        if (allOperas.length > 0) {
            modalTitle = `${clickName} - 全部收录剧种 <span style="font-size:16px;color:#a1b0c8">(${allOperas.length} 个)</span>`;
            targetOperas = allOperas;
        } else {
            alert(`正在抓取【${clickName}】的剧种数据，请稍后...`);
            return;
        }
    } else {
        const provData = globalProvinceData[currentActiveProvince] || {};
        const allOperas = Array.isArray(provData.allOperas) ? provData.allOperas : [];
        if (allOperas.length > 0) {
            targetOperas = allOperas.filter(op => String(op && op.dynastyBucket || '') === String(clickName));
            modalTitle = `${currentActiveProvince} - ${clickName}起源剧种 <span style="font-size:16px;color:#a1b0c8">(${targetOperas.length} 个)</span>`;
        }
        if (targetOperas.length === 0) {
            alert(`【${currentActiveProvince}】暂无收录产生于【${clickName}】的剧种`);
            return;
        }
    }

    let listHtml = '';
    targetOperas.forEach((op, index) => {
        const opName = String(op && op.name || '未知剧种');
        const opDynasty = String(op && op.dynasty || '未知');
        const opLevel = String(op && op.level || '未计入');
        const levelColor = opLevel.includes('国家') || opLevel.includes('世界') ? '#ff2277' : '#00eaff';
        listHtml += `
            <div class="opera-list-item">
                <div class="opera-list-left">
                    <span class="opera-index">${index + 1}</span>
                    <span class="opera-name">${opName}</span>
                    <span class="opera-time">(${opDynasty})</span>
                </div>
                <div class="opera-list-right" style="color: ${levelColor}; border-color: ${levelColor}">
                    ${opLevel}非遗
                </div>
            </div>`;
    });

    document.getElementById('opera-list-title').innerHTML = modalTitle;
    document.getElementById('opera-list-body').innerHTML = listHtml;
    document.getElementById('opera-list-modal').style.display = 'flex';
    setTimeout(() => { document.getElementById('opera-list-body').scrollTop = 0; }, 10);
});

// 地图下钻与交互逻辑
myChartMap.on('selectchanged', function (params) {
    const mapSelection = params.selected.find(s => s.seriesIndex === 0);
    const hasSelection = mapSelection && mapSelection.dataIndex.length > 0;
    
    myChartMap.setOption({
        visualMap: { show: !hasSelection },
        series: [ {}, { data: hasSelection ? [] : globalScatterData } ]
    });
});

myChartMap.on('click', function(params) {
    if (params.seriesType === 'scatter') {
        const fullName = getMapFullName(params.name);
        myChartMap.dispatchAction({ type: 'select', seriesIndex: 0, name: fullName });
        return; // 散点点击交由 selectedMode 处理，下方的省份判断由随后的 click 触发
    }

    const fullName = params.name;
    const shortName = fullName.replace(/省|市|维吾尔自治区|壮族自治区|回族自治区|自治区|特别行政区/g, '');

    if (currentActiveProvince === shortName) {
        currentActiveProvince = '全国';
        currentActiveFullName = ''; 
        if (globalProvinceData['全国']) {
            updateLeftTopChart('全国', globalProvinceData['全国']);
            updateRightTopChart('全国', globalProvinceData['全国']);
            updateRadarChart('全国', globalProvinceData['全国']);
            updateBarChart('全国', globalProvinceData['全国']);
        }
        myChartMap.dispatchAction({ type: 'unselect', name: fullName });
    } else {
        if (globalProvinceData[shortName]) {
            currentActiveProvince = shortName; 
            currentActiveFullName = fullName; 
            const provinceData = globalProvinceData[shortName];
            updateLeftTopChart(shortName, provinceData);
            updateRightTopChart(shortName, provinceData);
            updateRadarChart(shortName, provinceData);
            updateBarChart(shortName, provinceData);
        } else {
            myChartMap.dispatchAction({ type: 'unselect', name: fullName });
        }
    }
});

myChartMap.getZr().on('click', function(event) {
    if (!event.target) { 
        if (currentActiveProvince !== '全国') {
            currentActiveProvince = '全国';
            if (globalProvinceData['全国']) {
                updateLeftTopChart('全国', globalProvinceData['全国']);
                updateRightTopChart('全国', globalProvinceData['全国']);
                updateRadarChart('全国', globalProvinceData['全国']);
                updateBarChart('全国', globalProvinceData['全国']);
            }
            if (currentActiveFullName) {
                myChartMap.dispatchAction({ type: 'unselect', name: currentActiveFullName });
                currentActiveFullName = ''; 
            }
        }
    }
});

// 加载动画关闭逻辑
// 等待页面所有资源（包括图片、大体积 JS）加载完毕后执行
window.addEventListener('load', function() {
    const loader = document.querySelector('.initial-loader-container');
    if (loader) {
        // 先让它变透明（配合 CSS 里的 transition 实现淡出效果）
        loader.style.opacity = '0';
        // 0.5秒淡出动画结束后，把这个节点从 HTML 里彻底删掉
        setTimeout(function() {
            loader.remove();
        }, 500);
    }
});
