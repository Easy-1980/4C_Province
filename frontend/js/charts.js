// 初始化所有图表实例
const myChartLeftTop = echarts.init(document.getElementById('chart-left-top'));
const myChartLeftBottom = echarts.init(document.getElementById('chart-left-bottom'));
const myChartMap = echarts.init(document.getElementById('chart-center-map'));
const myChartRightTop = echarts.init(document.getElementById('chart-right-top'));
const myChartRightBottom = echarts.init(document.getElementById('chart-right-bottom'));
window.myChartTgiAge = echarts.init(document.getElementById('tgi-age-chart'));
window.myChartTgiGender = echarts.init(document.getElementById('tgi-gender-chart'));

// 窗口调整大小自适应
window.addEventListener('resize', () => {
    myChartLeftTop.resize(); myChartLeftBottom.resize(); myChartMap.resize(); 
    myChartRightTop.resize(); myChartRightBottom.resize();
});

// 核心更新函数
function updateRightTopChart(provinceName, data) {
    const titleEl = document.querySelector('#chart-right-top').previousElementSibling;
    if (provinceName === '全国') {
        titleEl.innerHTML = `${provinceName} - 剧目弹幕热点词云 <span style="font-size:10px; color:#ffb020;">(B站抓取)</span>`;
        myChartRightTop.setOption({
            tooltip: { show: true, trigger: 'item' },
            xAxis: { show: false }, yAxis: { show: false },
            series: [{
                type: 'wordCloud', shape: 'circle', left: 'center', top: 'center', width: '95%', height: '95%',
                sizeRange: [20, 55], rotationRange: [-45, 45], gridSize: 8,
                textStyle: { color: () => 'rgb(' + [Math.round(Math.random() * 150 + 100), Math.round(Math.random() * 150 + 100), 255].join(',') + ')' },
                data: data.wordCloud
            }]
        }, true);
    } else {
        titleEl.innerHTML = `${provinceName} - 代表剧目弹幕热点折线图 <span style="font-size:10px; color:#ffb020;">(B站抓取)</span>`;
        const trendData = data.danmakuTrend || { operaName: '暂未录入代表剧目', times: ['00:00', '00:10', '00:20', '00:30'], counts: [0, 0, 0, 0] };
        myChartRightTop.setOption({
            grid: { top: '25%', right: '8%', bottom: '15%', left: '15%' },
            tooltip: {
                trigger: 'axis', backgroundColor: 'rgba(6, 13, 31, 0.9)', borderColor: '#00eaff', textStyle: { color: '#fff', fontSize: 12 },
                formatter: function(params) {
                    return `<div style="font-weight:bold; color:#ffb020; margin-bottom:5px;">${trendData.operaName}</div>
                            时间: ${params[0].name}<br/>
                            弹幕数: <span style="color:#00eaff; font-weight:bold; font-size:16px;">${params[0].value}</span> 条`;
                }
            },
            xAxis: { type: 'category', boundaryGap: false, data: trendData.times, axisLabel: { color: '#a1b0c8', fontSize: 10, maxInterval: 5 }, axisLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } }, axisTick: { show: false } },
            yAxis: { type: 'value', splitLine: { lineStyle: { color: 'rgba(255,255,255,0.05)', type: 'dashed' } }, axisLabel: { color: '#a1b0c8', fontSize: 10 } },
            series: [{
                name: '弹幕数', type: 'line', data: trendData.counts, smooth: 0.4, symbol: 'none',
                lineStyle: { color: '#00eaff', width: 2, shadowColor: 'rgba(0, 234, 255, 0.5)', shadowBlur: 10 },
                areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{ offset: 0, color: 'rgba(0, 234, 255, 0.6)' }, { offset: 1, color: 'rgba(0, 234, 255, 0.05)' }]) },
                markPoint: { data: [ { type: 'max', name: 'Max' } ], symbol: 'pin', symbolSize: 40, itemStyle: { color: '#ff2277', shadowBlur: 10, shadowColor: '#ff2277' }, label: { color: '#fff', fontSize: 10 } }
            }]
        }, true);
    }
}

function updateLeftTopChart(provinceName, data) {
    const panelTitle = document.querySelector('#chart-left-top').previousElementSibling;
    if (provinceName === '全国') {
        panelTitle.innerText = '全国剧种数量 Top 10 省份';
        myChartLeftTop.setOption({
            grid: { top: '10%', right: '12%', bottom: '10%', left: '15%' },
            tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
            xAxis: { type: 'value', splitLine: { show: false }, axisLabel: { show: false } },
            yAxis: { type: 'category', data: data.topProvinces.names, axisLabel: { color: '#a1b0c8' }, axisLine: { show: false }, axisTick: { show: false } },
            series: [{
                type: 'bar', data: data.topProvinces.values, label: { show: true, position: 'right', color: '#00eaff', fontSize: 11 },
                itemStyle: { color: new echarts.graphic.LinearGradient(1, 0, 0, 0, [{ offset: 0, color: '#00eaff' }, { offset: 1, color: '#0075ff' }]), borderRadius: [0, 5, 5, 0] }
            }]
        }, true);
    } else {
        panelTitle.innerText = `${provinceName} - 代表剧种起源朝代分布`;
        myChartLeftTop.setOption({
            grid: { top: '25%', right: '5%', bottom: '15%', left: '5%' }, 
            tooltip: {
                trigger: 'axis', backgroundColor: 'rgba(6, 13, 31, 0.9)', borderColor: '#00eaff', textStyle: { color: '#fff' }, axisPointer: { type: 'none' },
                formatter: function (params) {
                    const data = params[0];
                    const customMarker = `<span style="display:inline-block; margin-right:8px; border-radius:50%; width:10px; height:10px; background-color:#00eaff; box-shadow:0 0 10px #00eaff;"></span>`;
                    return `<div style="font-weight:bold; margin-bottom:5px; border-bottom: 1px solid rgba(0,234,255,0.3); padding-bottom: 5px;">${data.name}</div>
                            <div style="display:flex; align-items:center;">${customMarker} <span style="color:#a1b0c8;">剧种数量：</span><span style="color:#00eaff; font-weight:bold; font-size: 16px; margin-left: 5px;">${data.value}</span></div>`;
                }
            },
            xAxis: { type: 'category', data: data.dynastyDistribution.dynasties, axisLabel: { color: '#a1b0c8', fontSize: 11, interval: 0, margin: 15 }, axisLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } }, axisTick: { show: false } },
            yAxis: { type: 'value', splitLine: { lineStyle: { color: 'rgba(255,255,255,0.05)', type: 'dashed' } }, axisLabel: { show: false } },
            series: [
                { name: '剧种数量', type: 'scatter', symbol: 'circle', symbolSize: 12, data: data.dynastyDistribution.counts, itemStyle: { color: '#060d1f', borderColor: '#00eaff', borderWidth: 3, shadowBlur: 12, shadowColor: '#00eaff' }, label: { show: true, position: 'top', color: '#00eaff', fontSize: 16, fontWeight: 'bold', distance: 10 }, zlevel: 2 },
                { name: '剧种数量', type: 'bar', barWidth: 2, data: data.dynastyDistribution.counts, itemStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{ offset: 0, color: '#00eaff' }, { offset: 1, color: 'rgba(0, 234, 255, 0)' }]), borderRadius: 2 }, zlevel: 1 }
            ]
        }, true);
    }
}

function updateRadarChart(provinceName, data) {
    document.querySelector('#chart-left-bottom').previousElementSibling.innerText = `${provinceName} - 代表剧种受众关注维度`;
    myChartLeftBottom.setOption({
        series: [{ data: [{ value: data.radarData, name: `${provinceName}受众特征`, itemStyle: { color: '#00eaff', borderColor: '#fff', borderWidth: 1 }, areaStyle: { color: new echarts.graphic.RadialGradient(0.5, 0.5, 1, [{ color: 'rgba(0, 234, 255, 0.1)', offset: 0 }, { color: 'rgba(0, 234, 255, 0.6)', offset: 1 }]) } }] }]
    });
}

function updateBarChart(provinceName, data) {
    document.querySelector('#chart-right-bottom').previousElementSibling.innerHTML = `${provinceName} - 代表剧种受众画像 <span style="font-size:10px; color:#ffb020; cursor:pointer;" onclick="window.openTgiModal()">[点击查看TGI详情]</span>`;
    myChartRightBottom.setOption({
        yAxis: { data: data.ageGender.categories },
        series: [ { name: '男性', data: data.ageGender.male }, { name: '女性', data: data.ageGender.female } ]
    });
}

// 地图专属渲染逻辑
function renderMap(geoJson) {
    echarts.registerMap('china', geoJson);
    const mapData = [];     
    globalScatterData = []; 
    let maxOperaCount = 0;

    for (const prov in globalProvinceData) {
        if (prov !== '全国' && globalProvinceData[prov].allOperas) {
            const count = globalProvinceData[prov].allOperas.length;
            const fullName = getMapFullName(prov);
            mapData.push({ name: fullName, value: count });
            
            if (geoCoordMap[prov]) {
                globalScatterData.push({ name: prov, value: [...geoCoordMap[prov], count] });
            }
            if (count > maxOperaCount) maxOperaCount = count;
        }
    }
    const heatmapMax = Math.max(10, maxOperaCount);

    const optionMap = {
        visualMap: { show: true, type: 'continuous', min: 0, max: heatmapMax, left: '3%', bottom: '8%', itemWidth: 20, itemHeight: 120, textStyle: { color: '#a1b0c8', fontSize: 12 }, calculable: true, inRange: { color: ['#ffb020', '#ff4d4d', '#ff0000'] }, seriesIndex: 1 },
        tooltip: {
            trigger: 'item', backgroundColor: 'rgba(6, 13, 31, 0.9)', borderColor: '#ffb020', textStyle: { color: '#fff' }, padding: 15,
            formatter: function (params) {
                let shortName = params.name.replace(/省|市|维吾尔自治区|壮族自治区|回族自治区|自治区|特别行政区/g, '');
                const provinceData = globalProvinceData[shortName];
                if (!provinceData || !provinceData.operas || provinceData.operas.length === 0) {
                    return `<div style="font-size: 16px; color: #ffb020;">${params.name} <br><span style="font-size: 12px; color: #a1b0c8;">暂未收录核心剧种数据</span></div>`;
                }
                let html = `<div style="min-width: 300px;"><div style="font-size: 18px; color: #ffb020; font-weight: bold; border-bottom: 1px solid rgba(255, 176, 32, 0.3); padding-bottom: 10px; margin-bottom: 15px;">${params.name} <span style="font-size: 14px; color: #a1b0c8; font-weight: normal;">| 核心代表剧种</span></div>`;
                provinceData.operas.slice(0, 3).forEach((opera, index) => {
                    let levelColor = opera.level.includes('国家') || opera.level.includes('世界') ? '#ff2277' : '#ffb020';
                    html += `<div style="display: flex; align-items: center; margin-bottom: 12px; width: 100%;">
                                <div style="width: 22px; height: 22px; background: #ff4d4d; color: #fff; text-align: center; line-height: 22px; border-radius: 50%; font-size: 12px; margin-right: 10px; flex-shrink: 0;">${index + 1}</div>
                                <div style="color: #fff; font-size: 15px; font-weight: bold; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex-shrink: 1;" title="${opera.name}">${opera.name}</div>
                                <div style="color: #6a7b9d; font-size: 13px; margin-left: 8px; margin-right: auto; white-space: nowrap; flex-shrink: 0;">(${opera.dynasty})</div>
                                <div style="border: 1px solid ${levelColor}; color: ${levelColor}; padding: 2px 6px; border-radius: 4px; font-size: 12px; white-space: nowrap; flex-shrink: 0; margin-left: 15px;">${opera.level}非遗</div>
                            </div>`;
                });
                if (provinceData.operas.length > 3) html += `<div style="color: #a1b0c8; font-size: 12px; text-align: center; margin-top: 10px; border-top: 1px dashed rgba(161, 176, 200, 0.3); padding-top: 10px;">(仅展示前 3 个代表剧种)</div>`;
                html += `</div>`;
                return html;
            }
        },
        geo: {
            map: 'china', roam: true, boundingCoords: [ [73.0, 54.0], [135.0, 15.0] ], zoom: 1.2, scaleLimit: { min: 1, max: 5 },
            itemStyle: { areaColor: 'rgba(0, 234, 255, 0.05)', borderColor: 'rgba(0, 234, 255, 0.4)', borderWidth: 1 },
            emphasis: { itemStyle: { areaColor: 'rgba(0, 234, 255, 0.15)', borderColor: '#00eaff', borderWidth: 1.5 }, label: { show: false, color: '#fff', fontSize: 14, fontWeight: 'bold' } },
            select: {
                itemStyle: { areaColor: 'rgba(63, 191, 202, 0.7)', borderColor: '#fff', borderWidth: 1, shadowBlur: 15, shadowColor: '#00eaff' },
                label: { show: true, color: '#fff', fontSize: 12, fontWeight: 'bold', textBorderColor: 'transparent', textBorderWidth: 0, textShadowBlur: 5, textShadowColor: 'rgba(0, 0, 0, 0.8)' }
            }
        },
        series: [
            { name: '地图触发层', type: 'map', geoIndex: 0, data: mapData, selectedMode: 'single' },
            { name: '剧种点', type: 'effectScatter', coordinateSystem: 'geo', data: globalScatterData, symbolSize: 5, showEffectOn: 'render', 
              rippleEffect: { number: 1, scale: 2, brushType: 'fill', period: 3 },
              label: { show: true, formatter: '{b}', position: 'right', color: '#a1b0c8', fontSize: 11 },
              itemStyle: { shadowBlur: 6, shadowColor: 'rgba(255, 77, 77, 0.8)' },
              emphasis: { scale: true, label: { show: true, color: '#fff', fontWeight: 'bold' } },
              animation: true, animationDuration: 1000, animationEasing: 'cubicOut', animationDelay: idx => idx * 30, animationDurationUpdate: 800, animationEasingUpdate: 'cubicInOut' }
        ]
    };
    myChartMap.setOption(optionMap);
}

// 初始化空图表占位
myChartLeftTop.setOption({ grid: { top: '10%', right: '5%', bottom: '10%', left: '15%' }, xAxis: { type: 'value', splitLine: { show: false }, axisLabel: { color: '#a1b0c8' } }, yAxis: { type: 'category', data: ['广东', '福建', '湖南', '江西', '安徽', '浙江', '陕西', '河南', '江苏', '山西'], axisLabel: { color: '#a1b0c8' } }, series: [{ type: 'bar', data: [12, 15, 18, 20, 22, 25, 28, 32, 35, 42], itemStyle: { color: new echarts.graphic.LinearGradient(1, 0, 0, 0, [{ offset: 0, color: '#00eaff' }, { offset: 1, color: '#0075ff' }]), borderRadius: [0, 5, 5, 0] } }] });
myChartLeftBottom.setOption({ tooltip: { trigger: 'item', backgroundColor: 'rgba(6, 13, 31, 0.9)', borderColor: '#00eaff', borderWidth: 1, textStyle: { color: '#fff', fontSize: 12 } }, radar: { indicator: [ { name: '服化道审美', max: 100 }, { name: '二创与整活', max: 100 }, { name: '名场面打卡', max: 100 }, { name: '传统文化底蕴', max: 100 }, { name: '剧情与价值观', max: 100 }, { name: '唱腔与身段', max: 100 } ], center: ['50%', '55%'], radius: '65%', shape: 'polygon', splitNumber: 4, axisName: { color: '#a1b0c8', fontSize: 11 }, splitLine: { lineStyle: { color: ['rgba(0, 234, 255, 0.6)', 'rgba(0, 234, 255, 0.4)', 'rgba(0, 234, 255, 0.2)', 'rgba(0, 234, 255, 0.1)'].reverse() } }, splitArea: { show: false }, axisLine: { lineStyle: { color: 'rgba(255, 255, 255, 0.15)' } } }, series: [{ name: '关注度得分', type: 'radar', data: [] }] });
myChartRightBottom.setOption({ tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, backgroundColor: 'rgba(6, 13, 31, 0.8)', borderColor: '#00eaff', textStyle: { color: '#fff', fontSize: 12 }, formatter: function(params) { let html = `<strong>${params[0].name}</strong><br/>`; let total = 0; params.forEach(item => { html += `${item.marker} ${item.seriesName}: ${item.value}%<br/>`; total += item.value; }); html += `<hr style="border:none;border-top:1px solid rgba(255,255,255,0.2);margin:5px 0;">该年龄段占比: <strong style="color:#00eaff">${total}%</strong>`; return html; } }, legend: { data: ['男性', '女性'], top: '0%', right: '5%', textStyle: { color: '#a1b0c8', fontSize: 11 }, itemWidth: 12, itemHeight: 12, icon: 'roundRect' }, grid: { top: '15%', right: '5%', bottom: '15%', left: '15%' }, xAxis: { type: 'value', max: 50, splitLine: { lineStyle: { color: 'rgba(255,255,255,0.05)', type: 'dashed' } }, axisLabel: { color: '#a1b0c8', fontSize: 10, formatter: '{value}%' } }, yAxis: { type: 'category', data: ['45岁+', '35-44岁', '26-35岁', '18-25岁'], axisLine: { lineStyle: { color: 'rgba(255,255,255,0.1)' } }, axisLabel: { color: '#a1b0c8', fontSize: 11 } }, series: [ { name: '男性', type: 'bar', stack: 'total', barWidth: '40%', itemStyle: { color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [{ offset: 0, color: '#0075ff' }, { offset: 1, color: '#00eaff' }]) }, data: [] }, { name: '女性', type: 'bar', stack: 'total', itemStyle: { color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [{ offset: 0, color: '#ff4d8f' }, { offset: 1, color: '#ffb020' }]), borderRadius: [0, 4, 4, 0] }, data: [] } ] });
myChartRightTop.setOption({ tooltip: { show: true }, series: [{ type: 'wordCloud', shape: 'circle', left: 'center', top: 'center', width: '95%', height: '95%', sizeRange: [20, 55], rotationRange: [-45, 45], gridSize: 8, textStyle: { color: () => 'rgb(' + [Math.round(Math.random() * 150 + 100), Math.round(Math.random() * 150 + 100), 255].join(',') + ')' }, data: [] }] });