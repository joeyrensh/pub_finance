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
        'us_special-annual-return-light', 'us_special-annual-return-dark', 'us_special-ind-trend-light', 'us_special-ind-trend-dark',
        'us_special-strategy-light', 'us_special-strategy-dark', 'us_special-by-position-light', 'us_special-by-position-dark',
        'us_special-by-pl-light', 'us_special-by-pl-dark', 'us_special-by-positiondate-light', 'us_special-by-positiondate-dark',
        'us_special-bypl-date-light', 'us_special-bypl-date-dark',        
    ];

    // 基础配置（无前缀）
    const BASE_FONT_SIZE_CONFIG = {
        'annual-return-light': { mobile: '2.2rem', desktop: '1.5rem' },
        'annual-return-dark': { mobile: '2.2rem', desktop: '1.5rem' },
        'ind-trend-light': { mobile: 'unset', desktop: 'unset' },
        'ind-trend-dark': { mobile: 'unset', desktop: 'unset' },
        'strategy-light': { mobile: '2rem', desktop: '2rem' },
        'strategy-dark': { mobile: '2rem', desktop: '2rem' },
        'by-position-light': { mobile: '2.2rem', desktop: '2.2rem' },
        'by-position-dark': { mobile: '2.2rem', desktop: '2.2rem' },
        'by-pl-light': { mobile: '2.2rem', desktop: '2.2rem' },
        'by-pl-dark': { mobile: '2.2rem', desktop: '2.2rem' },
        'by-positiondate-light': { mobile: '2rem', desktop: '2rem' },
        'by-positiondate-dark': { mobile: '2rem', desktop: '2rem' },
        'bypl-date-light': { mobile: '2rem', desktop: '2rem' },
        'bypl-date-dark': { mobile: '2rem', desktop: '2rem' },
    };

    // 动态生成带前缀的配置
    const FONT_SIZE_CONFIG = {};
    ['cn', 'us', 'us_special'].forEach(prefix => {
        Object.keys(BASE_FONT_SIZE_CONFIG).forEach(key => {
            FONT_SIZE_CONFIG[`${prefix}-${key}`] = BASE_FONT_SIZE_CONFIG[key];
        });
    });

    const CLASS_FONT_SIZE_CONFIG = {
        'xtick': { mobile: '2rem', desktop: '2rem' },
        'ytick': { mobile: '2rem', desktop: '2rem' },
        'y2tick': { mobile: '2rem', desktop: '2rem' },
        'legendtext': { mobile: '2.2rem', desktop: '2.2rem' },
        'gtitle': { mobile: '2.2rem', desktop: '2.2rem' },
        'xtitle': { mobile: '2.2rem', desktop: '2.2rem' }
    };

    function replaceFontSize(element, svgId) {
        const screenWidth = window.innerWidth;

        const config = FONT_SIZE_CONFIG[svgId] || { mobile: 'unset', desktop: 'unset' };
        let fontSize = screenWidth <= 550 ? config.mobile : config.desktop;
        let fontWeight = 400;
        const clsParent = element.closest('[class]');
        if (clsParent) {
            clsParent.classList.forEach(className => {
                if (CLASS_FONT_SIZE_CONFIG[className]) {
                    const classConfig = CLASS_FONT_SIZE_CONFIG[className];
                    fontSize = screenWidth <= 550 ? classConfig.mobile : classConfig.desktop;
                }
            });
        }

        // 特殊 SVG 的额外逻辑
        if (svgId === 'cn-annual-return-light' || svgId === 'cn-annual-return-dark' 
            || svgId === 'us-annual-return-light' || svgId === 'us-annual-return-dark'
            || svgId === 'us_special-annual-return-light' || svgId === 'us_special-annual-return-dark'
        ) {
            const tableParent = element.closest('[id^="table"]');
            if (tableParent) {
                const textElements = Array.from(tableParent.children).filter(child =>
                    child.id && child.id.startsWith("text_")
                );
                const colCount = 3;
                const parentDiv = element.closest('g');
                const idx = textElements.findIndex(child => child === parentDiv);
                if (idx >= colCount && (idx % colCount === 2)) {
                    fontSize = screenWidth <= 550 ? '3rem' : '2.5rem';
                    fontWeight = 700;
                    const textContent = parentDiv.textContent.trim();
                    if (textContent.startsWith('-')) {
                        element.style.setProperty('fill', '#0d876d', 'important');
                    } else {
                        element.style.setProperty('fill', '#D9534F', 'important');
                    }
                }
            }
            const axisParent = element.closest('[id*="axis_"]');
            if (axisParent) {
                const textElements = Array.from(axisParent.children).filter(child =>
                    child => child.id && child.id.startsWith("text_")
                );
                const parentDiv = element.closest('g');
                const idx = textElements.findIndex(child => child => child === parentDiv);
                if (idx >= 0) {
                    fontSize = screenWidth <= 550 ? '2rem' : '1.5rem';
                }                
            }            
        }

        // 仅在 fontSize 有效且不为 'unset' 时设置
        if (typeof fontSize === 'string' && fontSize !== 'unset' && fontSize.trim() !== '') {
            element.style.setProperty('font-size', fontSize);
            element.style.setProperty('font-weight', String(fontWeight), 'important');
        }

        // 推荐添加通用后备字体
        element.style.setProperty(
            'font-family',
            '"PingFang SC", "Microsoft YaHei", "Noto Sans CJK SC", "Helvetica Neue", Helvetica, "Segoe UI", Roboto, Arial, sans-serif','important'
        );
        element.style.setProperty('-webkit-font-smoothing', 'antialiased', 'important');
        element.style.setProperty('text-rendering', 'optimizeLegibility', 'important');
        element.style.setProperty('letter-spacing', '-.03rem', 'important');
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
                const transform = text.getAttribute('transform');
                if (transform && transform.includes('scale')) {
                    continue; // 跳过包含scale变换的元素
                }
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
