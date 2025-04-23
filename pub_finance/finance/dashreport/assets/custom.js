(function () {
    const SVG_IDS = [
        'annual-return-light', 'annual-return-dark', 'ind-trend-light', 'ind-trend-dark',
        'strategy-light', 'strategy-dark', 'by-position-light', 'by-position-dark',
        'by-pl-light', 'by-pl-dark', 'by-positiondate-light', 'by-positiondate-dark',
        'bypl-date-light', 'bypl-date-dark'
    ];

    const FONT_SIZE_CONFIG = {
        'annual-return-light': { mobile: '1.7rem', desktop: '1.3rem' },
        'annual-return-dark': { mobile: '1.7rem', desktop: '1.3rem' },
        'ind-trend-light': { mobile: '1.7rem', desktop: '1.7rem' },
        'ind-trend-dark': { mobile: '1.7rem', desktop: '1.7rem' },
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

    const CLASS_FONT_SIZE_CONFIG = {
        'xtick': { mobile: '2rem', desktop: '2.2rem' },
        'ytick': { mobile: '2rem', desktop: '2.2rem' },
        'y2tick': { mobile: '2rem', desktop: '2.2rem' },
        'legendtext': { mobile: '2rem', desktop: '2.1rem' }
    };

    function replaceFontSize(element, svgId) {
        const screenWidth = window.innerWidth;

        const config = FONT_SIZE_CONFIG[svgId] || { mobile: '1.5rem', desktop: '1.2rem' };
        let fontSize = screenWidth <= 550 ? config.mobile : config.desktop;

        let parent = element.closest('[class]');
        if (parent) {
            parent.classList.forEach(className => {
                if (CLASS_FONT_SIZE_CONFIG[className]) {
                    const classConfig = CLASS_FONT_SIZE_CONFIG[className];
                    fontSize = screenWidth <= 550 ? classConfig.mobile : classConfig.desktop;
                }
            });
        }

        element.style.setProperty('font-size', fontSize, 'important');
        element.style.setProperty('font-family', '-apple-system', 'important');
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