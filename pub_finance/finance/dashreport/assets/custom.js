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

    // ========== 2. 主题检测函数 ==========
    let currentTheme = null;
    
    function detectCurrentTheme() {
        const body = document.body;
        const html = document.documentElement;
        
        if (body.classList.contains('dark') || 
            body.classList.contains('dark-mode') ||
            body.getAttribute('data-theme') === 'dark' ||
            html.classList.contains('dark') ||
            html.getAttribute('data-theme') === 'dark') {
            return 'dark';
        }
        
        if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
            return 'dark';
        }
        
        return 'light';
    }
    
    function getThemeSpecificSVGIds() {
        const detectedTheme = detectCurrentTheme();
        
        if (currentTheme === detectedTheme) {
            const themeSuffix = currentTheme === 'dark' ? '-dark' : '-light';
            return SVG_IDS.filter(id => id.includes(themeSuffix));
        }
        
        const oldTheme = currentTheme;
        currentTheme = detectedTheme;
        const themeSuffix = currentTheme === 'dark' ? '-dark' : '-light';
        
        console.log(`[Theme Change] ${oldTheme || '初始'} -> ${currentTheme}, filtering for suffix: ${themeSuffix}`);
        
        return SVG_IDS.filter(id => id.includes(themeSuffix));
    }

    // ========== 3. 工具函数 ==========
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
    
    // ========== 【核心】生成元素唯一标识符的函数 ==========
    function generateElementIdentifier(element, svgId) {
        // 方案B：DOM路径法（推荐）
        let path = [];
        let current = element;
        
        // 向上遍历父节点，最多到SVG根元素
        while (current && current !== current.ownerDocument.documentElement) {
            let position = 0;
            let sibling = current.previousSibling;
            
            // 计算当前节点在兄弟节点中的位置（只计算同类型元素）
            while (sibling) {
                if (sibling.nodeType === 1 && sibling.tagName === current.tagName) {
                    position++;
                }
                sibling = sibling.previousSibling;
            }
            
            // 如果有id，使用id；否则使用标签名和位置
            const nodeId = current.id ? `#${current.id}` : `${current.tagName.toLowerCase()}[${position}]`;
            path.unshift(nodeId);
            
            // 如果遇到SVG根元素，停止
            if (current.tagName.toLowerCase() === 'svg') {
                break;
            }
            
            current = current.parentNode;
        }
        
        // 返回svgId + DOM路径
        return `${svgId}-${path.join('>')}`;
    }

    // ========== 4. 核心业务函数 ==========
    // 存储已放大的元素标识符（使用新的精确标识符）
    const enlargedElements = new Set();
    
    function replaceFontSize(element, svgId) {
        // 生成精确的唯一标识符
        const elementIdentifier = generateElementIdentifier(element, svgId);
        
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
                    fontSize = screenWidth <= 550 ? '2.6rem' : '2.5rem';
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
                    fontSize = screenWidth <= 550 ? '2.05rem' : '1.5rem'; 
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
                            fontSize = screenWidth <= 550 ? '2.2rem' : '1.5rem';
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
                        // 检查是否已处理过此元素（使用新的精确标识符）
                        if (!enlargedElements.has(elementIdentifier)) {
                            // 第一次处理：计算并放大
                            const computedStyle = window.getComputedStyle(element);
                            const originalSize = parseFloat(computedStyle.fontSize);
                            
                            if (!isNaN(originalSize) && originalSize > 0) {
                                const newSize = originalSize * 1.1;
                                fontSize = `${newSize}px`;
                                // 标记已处理
                                enlargedElements.add(elementIdentifier);
                                console.log(`[FontSize] First-time enlargement for: ${txt} (${elementIdentifier})`);
                            }
                        } else {
                            // 已处理过：跳过放大逻辑
                            fontSize = 'unset';
                            console.log(`[FontSize] Skipping re-enlargement for: ${txt}`);
                        }
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

    // ========== 5. 核心处理与监听逻辑 ==========
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
        const svgIdsToProcess = getThemeSpecificSVGIds();
        console.log(`[Filter] Processing ${svgIdsToProcess.length} SVGs for current theme (skipping ${SVG_IDS.length - svgIdsToProcess.length} from other theme)`);
        
        svgIdsToProcess.forEach(id => {
            const obj = document.getElementById(id);
            if (obj) {
                processSingleSvg(obj);
            }
        });
    }

    const debouncedHandleSvgProcessing = debounce(handleSvgProcessing, 100);

    // ========== 6. 主题变化监听系统 ==========
    function setupThemeChangeListeners() {
        const themeChangeObserver = new MutationObserver((mutations) => {
            for (const mutation of mutations) {
                if (mutation.attributeName === 'class' || mutation.attributeName === 'data-theme') {
                    console.log('[Theme Change] Detected via DOM attribute change, re-processing SVGs.');
                    processedElements.clear();
                    setTimeout(() => debouncedHandleSvgProcessing(), 300);
                    break;
                }
            }
        });
        
        themeChangeObserver.observe(document.body, { attributes: true });
        themeChangeObserver.observe(document.documentElement, { attributes: true });
        
        if (window.matchMedia) {
            const darkModeMediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
            const lightModeMediaQuery = window.matchMedia('(prefers-color-scheme: light)');
            
            const handleSystemThemeChange = (e) => {
                if (e.matches) {
                    console.log('[System Theme Change] Detected via media query, re-processing SVGs.');
                    processedElements.clear();
                    setTimeout(() => debouncedHandleSvgProcessing(), 300);
                }
            };
            
            darkModeMediaQuery.addEventListener('change', handleSystemThemeChange);
            lightModeMediaQuery.addEventListener('change', handleSystemThemeChange);
            
            let lastThemeCheck = detectCurrentTheme();
            setInterval(() => {
                const currentThemeCheck = detectCurrentTheme();
                if (currentThemeCheck !== lastThemeCheck) {
                    console.log('[Theme Polling] Theme changed detected via polling, re-processing SVGs.');
                    lastThemeCheck = currentThemeCheck;
                    processedElements.clear();
                    setTimeout(() => debouncedHandleSvgProcessing(), 300);
                }
            }, 5000);
        }
    }

    // ========== 7. 初始化 ==========
    function init() {
        console.log('Initializing SVG font size replacement with precise element identification');
        
        // 初始化时清除所有标记
        enlargedElements.clear();
        currentTheme = detectCurrentTheme();
        console.log(`[Theme] Initial theme detected: ${currentTheme}`);
        
        setTimeout(() => debouncedHandleSvgProcessing(), 100);
        
        document.addEventListener('plotly_afterplot', debouncedHandleSvgProcessing);

        const observer = new MutationObserver((mutations) => {
            let svgAdded = false;
            const currentThemeSuffix = currentTheme === 'dark' ? '-dark' : '-light';
            
            for (const mutation of mutations) {
                for (const node of mutation.addedNodes) {
                    if (node.nodeType === 1 && node.tagName === 'OBJECT' && node.id && node.id.includes(currentThemeSuffix)) {
                        svgAdded = true;
                        break;
                    }
                    if (node.nodeType === 1 && node.querySelector) {
                        const svgElements = node.querySelectorAll(`object[id$="${currentThemeSuffix}"]`);
                        if (svgElements.length > 0) {
                            svgAdded = true;
                            break;
                        }
                    }
                    if (svgAdded) break;
                }
                if (svgAdded) break;
            }
            if (svgAdded) {
                console.log('[Observer] Detected new SVG object matching current theme.');
                setTimeout(() => debouncedHandleSvgProcessing(), 150);
            }
        });
        observer.observe(document.body, { childList: true, subtree: true });
        
        setupThemeChangeListeners();
    }

    // ========== 8. 启动 ==========
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // ========== 9. 调试工具 ==========
    window.forceReprocessSVGs = () => {
        console.log('[Manual] Force reprocessing all SVGs.');
        enlargedElements.clear();
        processedElements.clear();
        handleSvgProcessing();
    };
    
    window.checkCurrentTheme = () => {
        const theme = detectCurrentTheme();
        console.log(`Current theme: ${theme}`);
        console.log(`SVG IDs to process:`, getThemeSpecificSVGIds());
        return theme;
    };
    
    window.manualThemeSwitch = (theme) => {
        if (theme === 'dark' || theme === 'light') {
            console.log(`[Manual] Switching to ${theme} theme`);
            currentTheme = theme;
            processedElements.clear();
            setTimeout(() => debouncedHandleSvgProcessing(), 100);
        } else {
            console.error('Invalid theme. Use "light" or "dark".');
        }
    };
    
    // 调试工具：查看和清除放大记录
    window.getEnlargedElements = () => {
        console.log(`Enlarged elements count: ${enlargedElements.size}`);
        return Array.from(enlargedElements).slice(0, 10); // 只返回前10个
    };
    
    window.clearEnlargedElements = () => {
        const count = enlargedElements.size;
        enlargedElements.clear();
        console.log(`Cleared ${count} enlarged element records`);
        return count;
    };

})();