(function () {
    // console.log('Custom JavaScript loaded');

    const SVG_IDS = ['annual-return-light', 'annual-return-dark', 'ind-trend-light', 'ind-trend-dark',
        'strategy-light', 'strategy-dark', 'by-position-light', 'by-position-dark', 'by-pl-light', 'by-pl-dark',
        'by-positiondate-light', 'by-positiondate-dark', 'bypl-date-light', 'bypl-date-dark'
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
        // 动态配置特殊 class 的字体大小
        'xtick': { mobile: '2rem', desktop: '2.2rem' },
        'ytick': { mobile: '2rem', desktop: '2.2rem' },
        'y2tick': { mobile: '2rem', desktop: '2.2rem' },
        'legendtext': { mobile: '2rem', desktop: '2.1rem' }
    };

    function replaceFontSize(element, svgId) {
        const screenWidth = window.innerWidth;

        // 获取对应 SVG 的字体大小配置
        const config = FONT_SIZE_CONFIG[svgId] || { mobile: '1.5rem', desktop: '1.2rem' };

        // 根据屏幕宽度选择字体大小
        let fontSize = screenWidth <= 550 ? config.mobile : config.desktop;

        // 检查父级是否包含特殊 class 配置
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
        element.style.setProperty('font-family', '-apple-system', 'important'); // 设置 font-family
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

    function init() {
        // console.log('Initializing SVG font size replacement');

        const observer = new MutationObserver(() => {
            SVG_IDS.forEach(id => {
                const obj = document.getElementById(id);
                if (obj) {
                    // console.log(`Found object with ID ${id}:`, obj);

                    obj.onload = () => {
                        // console.log(`SVG object with ID ${id} loaded`);
                        processSvg(obj);
                    };

                    if (obj.contentDocument) {
                        // console.log(`SVG object with ID ${id} already loaded`);
                        processSvg(obj);
                    }

                    observer.disconnect();
                }
            });
        });

        observer.observe(document.body, { childList: true, subtree: true });
    }

    window.addEventListener('load', init);
})();