// 1.核心数据并发加载引擎
myChartMap.showLoading({ text: '核心数据融合加载中...', color: '#ffb020', textColor: '#fff', maskColor: 'rgba(6, 13, 31, 0.8)' });

Promise.all([
    fetch('data/Province_data.json').then(res => res.json()),
    fetch('data/China.geojson').then(res => res.json()),
    fetch('data/danmakuWordAnalysis.json').then(res => res.json()).catch(() => ({}))
]).then(([provinceData, geoJson, wordAnalysisData]) => {
    myChartMap.hideLoading();
    
    // 注入全局变量
    globalProvinceData = provinceData;
    globalWordAnalysis = wordAnalysisData;

    // 1. 渲染地图
    renderMap(geoJson);

    // 2. 初始化大盘图表
    if (globalProvinceData['全国']) {
        updateLeftTopChart('全国', globalProvinceData['全国']);
        updateRightTopChart('全国', globalProvinceData['全国']);
        updateRadarChart('全国', globalProvinceData['全国']);
        updateBarChart('全国', globalProvinceData['全国']);
    }
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
        const provData = globalProvinceData[clickName];
        if (provData && provData.allOperas) {
            modalTitle = `${clickName} - 全部收录剧种 <span style="font-size:16px;color:#a1b0c8">(${provData.allOperas.length} 个)</span>`;
            targetOperas = provData.allOperas;
        } else {
            alert(`正在抓取【${clickName}】的剧种数据，请稍后...`);
            return;
        }
    } else {
        const provData = globalProvinceData[currentActiveProvince];
        if (provData && provData.allOperas) {
            targetOperas = provData.allOperas.filter(op => op.dynastyBucket === clickName);
            modalTitle = `${currentActiveProvince} - ${clickName}起源剧种 <span style="font-size:16px;color:#a1b0c8">(${targetOperas.length} 个)</span>`;
        }
        if (targetOperas.length === 0) {
            alert(`【${currentActiveProvince}】暂无收录产生于【${clickName}】的剧种`);
            return;
        }
    }

    let listHtml = '';
    targetOperas.forEach((op, index) => {
        const levelColor = op.level.includes('国家') || op.level.includes('世界') ? '#ff2277' : '#00eaff';
        listHtml += `
            <div class="opera-list-item">
                <div class="opera-list-left">
                    <span class="opera-index">${index + 1}</span>
                    <span class="opera-name">${op.name}</span>
                    <span class="opera-time">(${op.dynasty})</span>
                </div>
                <div class="opera-list-right" style="color: ${levelColor}; border-color: ${levelColor}">
                    ${op.level}非遗
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