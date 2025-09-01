(function () {
    const SVG_IDS = [
        'cn-annual-return-light', 'cn-annual-return-dark', 'cn-ind-trend-light', 'cn-ind-trend-dark',
        'cn-strategy-light', 'cn-strategy-dark', 'cn-by-position-light', 'cn-by-position-dark',
        'cn-by-pl-light', 'cn-by-pl-dark', 'cn-by-positiondate-light', 'cn-by-positiondate-dark',
        'cn-bypl-date-light', 'cn-bypl-date-dark',
        'us-annual-return-light', 'us-annual-return-dark', 'us-ind-trend-light', 'us-ind-trend-dark',
        'us-strategy-light', 'us-strategy-dark', 'us-by-position-light', 'us-by-position-dark',
        'us-by-pl-light', 'us-by-pl-dark', 'us-by-positiondate-light', 'us-by-positiondate-dark',
        'us-bypl-date-light', 'us-bypl-date-dark',
    ];

    // 基础配置（无前缀）
    const BASE_FONT_SIZE_CONFIG = {
        'annual-return-light': { mobile: '1.7rem', desktop: '1.2rem' },
        'annual-return-dark': { mobile: '1.7rem', desktop: '1.2rem' },
        // 'ind-trend-light': { mobile: '1.7rem', desktop: '1.7rem' },
        // 'ind-trend-dark': { mobile: '1.7rem', desktop: '1.7rem' },
        'strategy-light': { mobile: '1.5rem', desktop: '1.5rem' },
        'strategy-dark': { mobile: '1.5rem', desktop: '1.5rem' },
        'by-position-light': { mobile: '2rem', desktop: '2rem' },
        'by-position-dark': { mobile: '2rem', desktop: '2rem' },
        'by-pl-light': { mobile: '2rem', desktop: '2rem' },
        'by-pl-dark': { mobile: '2rem', desktop: '2rem' },
        'by-positiondate-light': { mobile: '2rem', desktop: '2rem' },
        'by-positiondate-dark': { mobile: '2rem', desktop: '2rem' },
        'bypl-date-light': { mobile: '2rem', desktop: '2rem' },
        'bypl-date-dark': { mobile: '2rem', desktop: '2rem' },
    };

    // 动态生成带前缀的配置
    const FONT_SIZE_CONFIG = {};
    ['cn', 'us'].forEach(prefix => {
        Object.keys(BASE_FONT_SIZE_CONFIG).forEach(key => {
            FONT_SIZE_CONFIG[`${prefix}-${key}`] = BASE_FONT_SIZE_CONFIG[key];
        });
    });

    const CLASS_FONT_SIZE_CONFIG = {
        'xtick': { mobile: '2.2rem', desktop: '2.4rem' },
        'ytick': { mobile: '2.2rem', desktop: '2.2rem' },
        'y2tick': { mobile: '2.2rem', desktop: '2.2rem' },
        'legendtext': { mobile: '2.1rem', desktop: '2.1rem' },
        'gtitle': { mobile: '2.2rem', desktop: '2.4rem' },
        'xtitle': { mobile: '2.2rem', desktop: '2.4rem' }
    };

    function replaceFontSize(element, svgId) {
        const screenWidth = window.innerWidth;

        const config = FONT_SIZE_CONFIG[svgId];
        let fontSize = screenWidth <= 550 ? config.mobile : config.desktop;
        let fontWeight = 400;
        let parent = element.closest('[class]');
        if (parent) {
            parent.classList.forEach(className => {
                if (CLASS_FONT_SIZE_CONFIG[className]) {
                    const classConfig = CLASS_FONT_SIZE_CONFIG[className];
                    fontSize = screenWidth <= 550 ? classConfig.mobile : classConfig.desktop;
                }
            });
        }

        // 如果是特定的 SVG，设置字体加粗
        if (svgId === 'cn-annual-return-light' || svgId === 'cn-annual-return-dark' 
            || svgId === 'us-annual-return-light' || svgId === 'us-annual-return-dark') { 
            // 查找 element 的父级是否以 "table" 为前缀
            const parent = element.closest('[id^="table"]');
            if (parent) {
                // 获取所有 id 以 "text_" 开头的子节点
                const textElements = Array.from(parent.children).filter(child =>
                    child.id && child.id.startsWith("text_")
                );
                const colCount = 3; // 或 4，根据你的表格实际列数设置

                // 找到当前 element 所在 <g> 的索引
                const parentDiv = element.closest('g');
                const idx = textElements.findIndex(child => child === parentDiv);

                // 判断是否为第二列且不是第一行
                if (idx >= colCount && (idx % colCount === 2)) {
                    fontSize = screenWidth <= 550 ? '2.5rem' : '2rem';
                    fontWeight = 500;
                    const textContent = parentDiv.textContent.trim();
                    if (textContent.startsWith('-')) {
                        element.style.setProperty('fill', '#0d876d', 'important');
                    } else {
                        element.style.setProperty('fill', '#D9534F', 'important');
                    }
                }
            }
        }
        element.style.setProperty('font-size', fontSize, 'important');
        element.style.setProperty('font-weight', fontWeight, 'important');
        element.style.setProperty('font-family', '"Helvetica Neue", -apple-system, BlinkMacSystemFont,  "Segoe UI"', 'important');                
    
    }

    function processSvg(obj) {
        try {
            const svgDoc = obj.contentDocument;
            if (!svgDoc || !svgDoc.documentElement) {
                console.warn(`SVG content not loaded for ${obj.id}`);
                return;
            }

            console.log(`Processing SVG: ${obj.id}`);
            const textElements = svgDoc.getElementsByTagName('text');
            for (let text of textElements) {
                replaceFontSize(text, obj.id);
            }
        } catch (e) {
            console.error('Error processing SVG:', e);
        }
    }

    function handleSvgProcessing() {
        SVG_IDS.forEach(id => {
            const obj = document.getElementById(id);
            if (obj) {
                obj.onload = () => {
                    processSvg(obj);
                };

                if (obj.contentDocument) {
                    processSvg(obj);
                }
            }
        });
    }

    function init() {
        console.log('Initializing SVG font size replacement');

        // 监听 DOM 变化
        const observer = new MutationObserver(() => {
            handleSvgProcessing();
        });

        observer.observe(document.body, { childList: true, subtree: true });

        // 初始处理
        handleSvgProcessing();
    }

    // 监听页面加载和切换
    window.addEventListener('load', init);
    window.addEventListener('hashchange', handleSvgProcessing); // 监听页面切换
    window.addEventListener('popstate', handleSvgProcessing); // 监听浏览器导航
})();
