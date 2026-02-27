// assets/window_events.js
(function() {
    // helper function: 触发 Dash callback
    function dispatchClientEvent() {
        const store = document.getElementById("client-event");
        if (store) {
            store.click();  // 模拟触发回调
        }
    }

    // 初始化触发一次
    window.addEventListener("load", dispatchClientEvent);

    // resize 监听
    window.addEventListener("resize", dispatchClientEvent);

    // dark / light theme 监听
    const mql = window.matchMedia("(prefers-color-scheme: dark)");
    mql.addEventListener("change", dispatchClientEvent);
})();