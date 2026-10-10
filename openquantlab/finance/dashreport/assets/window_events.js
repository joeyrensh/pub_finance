// assets/window_events.js
(function () {
    function dispatchClientEvent() {
        const store = document.getElementById("client-event");
        if (store) {
            store.click();
        }
    }

    function dispatchAfterLayout() {
        // 延迟一帧，等 viewport 真正更新
        requestAnimationFrame(() => {
            requestAnimationFrame(dispatchClientEvent);
        });
    }

    // 初始
    window.addEventListener("load", dispatchClientEvent);

    // resize（桌面 & 安卓 OK）
    window.addEventListener("resize", dispatchAfterLayout);

    // iOS 专用：屏幕旋转
    window.addEventListener("orientationchange", dispatchAfterLayout);

    // 更可靠的 viewport 监听（iOS 13+）
    if (window.visualViewport) {
        window.visualViewport.addEventListener("resize", dispatchAfterLayout);
    }

    // 主题切换
    const mql = window.matchMedia("(prefers-color-scheme: dark)");
    mql.addEventListener("change", dispatchClientEvent);
})();