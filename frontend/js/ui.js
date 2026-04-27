// 词云解析弹窗
window.closeNlpModal = function() {
    document.getElementById('nlp-modal').style.display = 'none';
};
document.getElementById('nlp-modal').addEventListener('click', function(e) {
    if (e.target === this) closeNlpModal();
});

// 弹幕爆点解析弹窗
window.startDanmakuAIAnalysis = function() {
    document.getElementById('ai-analysis-trigger').style.display = 'none';
    document.getElementById('ai-analysis-loading').style.display = 'block';
    
    setTimeout(() => {
        document.getElementById('ai-analysis-loading').style.display = 'none';
        document.getElementById('danmaku-insight').innerText = currentDanmakuAI.insight;
        document.getElementById('danmaku-decision').innerText = currentDanmakuAI.decision;
        document.getElementById('ai-analysis-result').style.display = 'block';
    }, 2500); 
};

window.closeDanmakuModal = function() {
    document.getElementById('danmaku-modal').style.display = 'none';
};
document.getElementById('danmaku-modal').addEventListener('click', function(e) {
    if (e.target === this) closeDanmakuModal();
});

// TGI 画像深度分析弹窗
window.openTgiModal = function() {
    const provinceData = globalProvinceData[currentActiveProvince];
    
    if (provinceData && provinceData.tgiData) {
        const tgiData = provinceData.tgiData;
        
        // 直接填充你原本 HTML 里写好的文本框
        document.getElementById('tgi-modal-title').innerText = currentActiveProvince;
        if (document.getElementById('tgi-analysis-text')) {
            document.getElementById('tgi-analysis-text').innerText = tgiData.analysis;
        }

        // 显示弹窗
        document.getElementById('tgi-modal').style.display = 'flex';

        // 提取一个通用的双 Y 轴图表配置函数
        const getTgiOption = (dataObj) => ({
            tooltip: { trigger: 'axis', axisPointer: { type: 'cross' }, backgroundColor: 'rgba(6, 13, 31, 0.9)', borderColor: '#00eaff', textStyle: { color: '#fff' } },
            legend: { data: ['占比(%)', 'TGI指数', 'TGI基准线'], top: 0, textStyle: { color: '#a1b0c8' } },
            grid: { top: '20%', right: '12%', bottom: '15%', left: '12%' },
            xAxis: { type: 'category', data: dataObj.categories, axisLabel: { color: '#a1b0c8' }, axisTick: { show: false } },
            yAxis: [
                { type: 'value', name: '占比(%)', nameTextStyle: { color: '#a1b0c8' }, splitLine: { show: false }, axisLabel: { color: '#a1b0c8' } },
                { type: 'value', name: 'TGI', nameTextStyle: { color: '#ffb020' }, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.05)' } }, axisLabel: { color: '#ffb020' }, min: 0 }
            ],
            series: [
                { name: '占比(%)', type: 'bar', barWidth: '40%', yAxisIndex: 0, itemStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{ offset: 0, color: '#00eaff' }, { offset: 1, color: '#0075ff' }]), borderRadius: [4, 4, 0, 0] }, data: dataObj.percent },
                { name: 'TGI指数', type: 'line', yAxisIndex: 1, symbol: 'emptyCircle', symbolSize: 6, itemStyle: { color: '#ffb020' }, lineStyle: { width: 3, shadowBlur: 10, shadowColor: '#ffb020' }, data: dataObj.tgi },
                { name: 'TGI基准线', type: 'line', yAxisIndex: 1, itemStyle: { color: '#ff2277' }, lineStyle: { type: 'dashed', width: 2 }, data: [], markLine: { symbol: ['none', 'none'], data: [{ yAxis: 100 }], lineStyle: { color: '#ff2277', type: 'dashed', width: 2, shadowBlur: 5, shadowColor: '#ff2277' }, label: { show: false } } }
            ]
        });

        // 强制使用 window. 调用跨文件图表实例
        window.myChartTgiAge.setOption(getTgiOption(tgiData.age));
        window.myChartTgiGender.setOption(getTgiOption(tgiData.gender));
        
        setTimeout(() => { 
            window.myChartTgiAge.resize(); 
            window.myChartTgiGender.resize(); 
        }, 100);
    } else {
        alert(`系统正在抓取【${currentActiveProvince}】的深度 TGI 偏好数据，请稍后...`);
    }
};

window.closeTgiModal = function() {
    document.getElementById('tgi-modal').style.display = 'none';
};
document.getElementById('tgi-modal').addEventListener('click', function(e) {
    if (e.target === this) closeTgiModal(); 
});


// 左上角剧种名录弹窗
window.closeOperaListModal = function() {
    document.getElementById('opera-list-modal').style.display = 'none';
};
document.getElementById('opera-list-modal').addEventListener('click', function(e) {
    if (e.target === this) closeOperaListModal(); 
});