(function () {
    // ========== 1. 完整配置 ==========
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

    const BASE_FONT_SIZE_CONFIG = {
        'annual-return-light': { mobile: '2.2rem', desktop: '1.5rem' },
        'annual-return-dark': { mobile: '2.2rem', desktop: '1.5rem' },
        'ind-trend-light': { mobile: '2rem', desktop: '2rem' },
        'ind-trend-dark': { mobile: '2rem', desktop: '2rem' },
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
        'legendtext': { mobile: '2rem', desktop: '2rem' },
        'gtitle': { mobile: '2.2rem', desktop: '2.2rem' },
        'xtitle': { mobile: '2.2rem', desktop: '2.2rem' }
    };

    // ========== 2. 工具函数 ==========
    function debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }

    // ========== 3. 核心业务函数 ==========
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

        if (svgId.includes('annual-return')) {
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
                const textElements = Array.from(axisParent.querySelectorAll('[id*="text_"]'));
                const parentDiv = element.closest('g');
                const idx = textElements.findIndex(child => child === parentDiv);
                if (idx >= 0) {
                    fontSize = screenWidth <= 550 ? '2rem' : '1.5rem'; 
                }
            }   
            const axesParent = element.closest('[id*="axes_"]');
            if (axesParent) {
                const textElements = Array.from(axesParent.querySelectorAll('[id*="text_"]'));
                const parentDiv = element.closest('g');
                const idx = textElements.findIndex(child => child === parentDiv);
                if (idx >= 0) {
                    const textNode = parentDiv.querySelector('text');
                    if (textNode) {
                        const content = (textNode.textContent || '').trim().toLowerCase();
                        if (content.includes('max')) {
                            fontSize = screenWidth <= 550 ? '2.5rem' : '2rem';
                        }  
                    }                    
                }
            }                   
        }

        try {
            const isIndTrend = svgId.includes('ind-trend');
            if (isIndTrend) {
                const ann = element.classList.contains('annotation-text')
                    ? element
                    : element.closest('.annotation-text');
                if (ann) {
                    const txt = (ann.textContent || '').trim();
                    const isNumeric = /^[+-]?(\d+(\.\d+)?|\.\d+)%?$/.test(txt);
                    if (isNumeric) {
                        const computedStyle = window.getComputedStyle(element);
                        const originalSize = parseFloat(computedStyle.fontSize);
                        const newSize = originalSize * 1.2;
                        fontSize = `${newSize}px`;
                    }
                }
            }
        } catch (e) {
            console.warn('ind-trend annotation-text处理失败', e);
        }

        if (typeof fontSize === 'string' && fontSize !== 'unset' && fontSize.trim() !== '') {
            element.style.setProperty('font-size', fontSize);
            element.style.setProperty('font-weight', String(fontWeight), 'important');
        }

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
            console.log(`[Success] Processing SVG: ${obj.id}`);
            const textElements = svgDoc.getElementsByTagName('text');   
            for (let text of textElements) {
                const transform = text.getAttribute('transform');
                if (transform && transform.includes('scale')) {
                    continue;
                }
                replaceFontSize(text, obj.id);          
            }
        } catch (e) {
            console.error('Error processing SVG:', e);
        }
    }

    // ========== 4. 核心处理与监听逻辑 ==========
    const processedElements = new Map();

    function processSingleSvg(obj) {
        const id = obj.id;
        if (!SVG_IDS.includes(id)) return;

        if (processedElements.has(id)) {
            clearTimeout(processedElements.get(id).timer);
        }

        const doProcess = () => {
            if (obj.contentDocument && obj.contentDocument.documentElement) {
                console.log(`[Success] Processing SVG: ${id} (direct check)`);
                processSvg(obj);
                processedElements.set(id, { status: 'processed', timer: null });
            } else {
                const retryData = processedElements.get(id) || { retryCount: 0 };
                if (retryData.retryCount < 10) {
                    retryData.retryCount++;
                    retryData.timer = setTimeout(doProcess, 300);
                    processedElements.set(id, retryData);
                    console.log(`[Retry] Waiting for SVG ${id}... (attempt ${retryData.retryCount})`);
                } else {
                    console.warn(`[Failed] SVG ${id} failed to load after 10 retries.`);
                    processedElements.set(id, { status: 'failed', timer: null });
                }
            }
        };

        obj.onload = () => {
            console.log(`[OnLoad] SVG ${id} onload event fired.`);
            if (processedElements.has(id)) {
                clearTimeout(processedElements.get(id).timer);
            }
            setTimeout(() => processSvg(obj), 50);
        };

        doProcess();
    }

    function handleSvgProcessing() {
        console.log('[Info] handleSvgProcessing called.');
        SVG_IDS.forEach(id => {
            const obj = document.getElementById(id);
            if (obj) {
                processSingleSvg(obj);
            }
        });
    }

    const debouncedHandleSvgProcessing = debounce(handleSvgProcessing, 100);

    // ========== 5. 初始化 ==========
    function init() {
        console.log('Initializing SVG font size replacement (Direct Approach)');

        setTimeout(() => debouncedHandleSvgProcessing(), 100);
        document.addEventListener('plotly_afterplot', debouncedHandleSvgProcessing);

        const observer = new MutationObserver((mutations) => {
            let svgAdded = false;
            for (const mutation of mutations) {
                for (const node of mutation.addedNodes) {
                    if (node.nodeType === 1 && node.tagName === 'OBJECT' && node.id && SVG_IDS.includes(node.id)) {
                        svgAdded = true;
                        break;
                    }
                    if (node.nodeType === 1 && node.querySelector) {
                        for (const id of SVG_IDS) {
                            if (node.querySelector(`object#${id}`)) {
                                svgAdded = true;
                                break;
                            }
                        }
                    }
                    if (svgAdded) break;
                }
                if (svgAdded) break;
            }
            if (svgAdded) {
                console.log('[Observer] Detected new SVG object.');
                setTimeout(() => debouncedHandleSvgProcessing(), 150);
            }
        });
        observer.observe(document.body, { childList: true, subtree: true });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    window.forceReprocessSVGs = () => {
        console.log('[Manual] Force reprocessing all SVGs.');
        processedElements.clear();
        handleSvgProcessing();
    };

})();