# Pub Finance

> 🌟 Free & Open-Source Quantitative Analysis Framework | Supporting A-Shares & US Stocks | Strategy Backtesting + Dynamic Visual Analytics

[![GitHub Stars](https://img.shields.io/github/stars/joeyrensh/pub_finance?style=flat-square)](https://github.com/joeyrensh/pub_finance)
[![Package Size](https://img.shields.io/github/languages/code-size/joeyrensh/pub_finance)](https://github.com/joeyrensh/pub_finance)
[![License](https://img.shields.io/github/license/joeyrensh/pub_finance?color=blue)](https://github.com/joeyrensh/pub_finance)

## 📌 Introduction
Pub Finance is a **full-lifecycle, open-source quantitative analysis framework** that provides an end-to-end closed loop spanning data pipelines, a backtesting engine, and interactive Web dashboards. Seamlessly supporting both the **A-Share** and **US Stock** markets, it is designed to help individual investors and quant enthusiasts rapidly validate, optimize, and deploy trading strategies.

---

## 🏗️ Architecture & Core Data Flow
Pub Finance features a lightweight, decoupled modular design. The core pipeline is structured as follows:

```text
 📊 Multi-Source Data           🧠 Backtesting Engine            🎨 Web Visualization
[ Yahoo Finance ] ── (US)  ──► [  Local Data Storage  ]       [  Dash Interactive Report  ]
                                (CSV / JSON Format)            ├── Performance Dashboard
[EastMoney/Sina ] ── (CN)  ──►           │                     ├── Interactive Candlestick
                                         ▼                     └── 🤖 LLM-Powered Stock Summary
                                [   Backtrader   ] ── (Perf) ──┘

## 界面预览
<p align="center">
  <img src="https://github.com/joeyrensh/pub_finance/raw/master/pub_finance/finance/Frontpage.png" height="320" alt="回测分析主界面">
  <img src="https://github.com/joeyrensh/pub_finance/raw/master/pub_finance/finance/Backtest.png" height="320" alt="买卖点K线分析">
  <img src="https://github.com/joeyrensh/pub_finance/raw/master/pub_finance/finance/Config.png" height="320" alt="自定义偏好">
</p>
<p align="center">
  <img src="https://github.com/joeyrensh/pub_finance/raw/master/pub_finance/finance/AI_Analysis.png" height="100" alt="AI智能摘要">
</p>
<p align="center">
  left：Candlestick with Buy/Sell Signals | Trade Log &nbsp;&nbsp; Right: Weight Configuration & Returns & Drawdowns
</p>

## Key Features
- 📥 Multi-Source Data Ingestion Seamlessly integrates with mainstream data providers (e.g., Yahoo Finance, EastMoney, Sina Finance) to automatically fetch market data.

- 💾 Lightweight Local Storage Uses localized CSV / JSON formats supporting incremental updates and data integrity validation.

- 🧠 Custom Strategy Backtesting Equipped with a highly flexible backtesting engine. Supports custom transaction logic for single assets as well as multi-asset portfolios.

- 📊 Comprehensive Performance Analytics Automatically computes quant metrics including returns, drawdowns, and risk ratios to intuitively evaluate your strategies.

📄 LLM-Powered Stock Reports Generates fundamental profiles (sector, market cap, financial health) via AI with one click (API token required).






# Pub Finance [中文版]

> 🌟 开源免费量化分析框架 | 支持 A股 / 美股 | 策略回测 + 动态可视化分析

[![GitHub Stars](https://img.shields.io/github/stars/joeyrensh/pub_finance?style=flat-square)](https://github.com/joeyrensh/pub_finance)
[![Package Size](https://img.shields.io/github/languages/code-size/joeyrensh/pub_finance)](https://github.com/joeyrensh/pub_finance)
[![License](https://img.shields.io/github/license/joeyrensh/pub_finance?color=blue)](https://github.com/joeyrensh/pub_finance)

## 📌 简介
Pub Finance 是一套**全流程开源量化分析框架**，提供从数据流、策略引擎到交互式 Web 报表的全链路闭环。框架原生支持 **A 股、美股** 两大市场，旨在帮助个人投资者与量化爱好者快速验证、优化并落地交易策略。

---

## 🏗️ 技术架构与核心数据流
Pub Finance 采用轻量低耦合的模块化设计，核心链路如下：

```text
 📊 多源数据流                 🧠 策略回测引擎               🎨 Web 可视化面板
[Yahoo Finance] ── (美股) ──► [数据本地存储]               [ Dash 交互报表 ]
                              (CSV / JSON)                 ├── 绩效联动看板
[东方财富/新浪] ── (A股)  ──►      │                       ├── 智能买卖K线
                                  ▼                        └── 🤖 AI 个股大模型摘要
                              [ Backtrader ] ── (绩效分析) ┘
```

## 界面预览
<p align="center">
  <img src="https://github.com/joeyrensh/pub_finance/raw/master/pub_finance/finance/Frontpage.png" height="320" alt="回测分析主界面">
  <img src="https://github.com/joeyrensh/pub_finance/raw/master/pub_finance/finance/Backtest.png" height="320" alt="买卖点K线分析">
  <img src="https://github.com/joeyrensh/pub_finance/raw/master/pub_finance/finance/Config.png" height="320" alt="自定义偏好">
</p>
<p align="center">
  <img src="https://github.com/joeyrensh/pub_finance/raw/master/pub_finance/finance/AI_Analysis.png" height="100" alt="AI智能摘要">
</p>
<p align="center">
  左侧：K线图 + 买卖信号标记 &nbsp;&nbsp; 右侧：回测配置 & 绩效指标面板
</p>

## 核心功能
- 📥 **多源数据采集**
  对接 Yahoo Finance、东方财富、新浪财经等主流数据源，自动拉取行情数据。

- 💾 **轻量化数据存储**
  采用 CSV / JSON 本地存储，支持增量更新与数据完整性校验。

- 🧠 **自定义策略回测**
  内置灵活回测引擎，支持自定义买卖逻辑，可单标的或多标的组合回测。

- 📊 **全方位绩效分析**
  自动计算收益、回撤、风险等量化指标，直观评估策略表现。

- 📄 **个股信息报告**
  AI一键生成个股行业、市值、财务等基础信息报告，需配置token。

## 绩效指标说明
| 指标缩写 | 指标名称 |
| :--- | :--- |
| ANN.R | 年化收益率 |
| CUM.R | 累计收益率 |
| MX.DD | 最大回撤 |
| D.RISK | 日间风险 |
| 30D/120D | 30日 / 120日滚动收益 |

## 可视化特点
- 收益曲线 + 回撤区域联动展示
- 标准 K 线图 + 成交量柱状图
- 红绿点位标记**买入/卖出**信号
- 年度绩效表格，多年数据横向对比
- AI一键生成基本面

## ⚡ Quick Start (三分钟极速上手)

### 1. 环境准备与依赖安装
克隆代码库并安装所需的量化基础依赖：
```bash
git clone [https://github.com/joeyrensh/pub_finance.git](https://github.com/joeyrensh/pub_finance.git)
cd pub_finance
pip install -r requirements.txt
```
### 2. 环境冷启动（下载历史数据）
运行历史数据下载脚本，一键拉取并初始化市场历史行情底表：
```bash
python -u finance/utility/history_data_download.py
```
### 3. 配置 Crontab 定时任务
为了保持本地数据的增量更新与策略的自动演算，请参照下方 自动化生产任务 (Crontab) 章节配置 Linux 定时任务。

### 4. 启动可视化交互服务
执行以下命令开启后台常驻服务，即可激活全功能交互式 Web 可视化面板，启动成功后，在浏览器访问 http://127.0.0.1:8050 即可开始分析。
```bash
nohup python -m finance.dashreport.dash_wsgi > dash_server.log 2>&1 &
```

## 项目目录结构
```text
pub_finance/
└── finance/
    ├── data/                # 数据存储目录
    │   ├── us_stockdetail.csv  # 美股持仓数据
    │   └── cn_stockdetail.csv  # A股持仓数据
    ├── utility/             # 工具脚本、代理池、数据采集工具
    ├── backtraderref/       # backtrader主策略
    ├── cncrawler/           # A股爬虫
    ├── uscralwer/           # 美股爬虫
    ├── proxy/               # 代理池维护工具
    ├── cnstockinfo/         # A股主数据存储
    ├── usstockinfo/         # 美股主数据存储
    ├── dashreport/          # 可视化工程
    ├── usstock_main.py      # 美股主程序
    ├── cnstock_main.py      # A股主程序
    ├── us.log               # 美股运行日志
    ├── cn.log               # A股运行日志
    └── README.md
```
## 服务部署（Linux 定时任务）
### 环境变量配置
```
PYSPARK_PYTHON=/home/ubuntu/miniconda3/bin/python
PYSPARK_DRIVER_PYTHON=/home/ubuntu/miniconda3/bin/python
```
### Crontab 定时任务
```
# 每日 07:00 执行美股策略
00 7 * * * cd /home/ubuntu/pub_finance/finance ; /home/ubuntu/miniconda3/bin/python -u /home/ubuntu/pub_finance/finance/usstock_main.py > /home/ubuntu/pub_finance/finance/us.log 2>&1

# 每日 15:30 执行 A 股策略
30 15 * * * cd /home/ubuntu/pub_finance/finance ; /home/ubuntu/miniconda3/bin/python -u /home/ubuntu/pub_finance/finance/cnstock_main.py > /home/ubuntu/pub_finance/finance/cn.log 2>&1

# 每日 07:30 执行中国大陆IP池维护
30 7 * * * cd /home/ubuntu/pub_finance/finance/proxy ; /home/ubuntu/miniconda3/bin/python -u /home/ubuntu/pub_finance/finance/proxy/fetch_cn_proxies.py --target 200 --workers 20 > /home/ubuntu/pub_finance/finance/proxy/cn_proxy.log 2>&1

# 每日 08:30 执行海外IP池维护
30 8 * * * cd /home/ubuntu/pub_finance/finance/proxy ; /home/ubuntu/miniconda3/bin/python -u /home/ubuntu/pub_finance/finance/proxy/fetch_overseas_proxies.py --target 100 --workers 20 > /home/ubuntu/pub_finance/finance/proxy/overseas_proxy.log 2>&1
```
## 参与贡献
欢迎大家提交 Issue 和 Pull Request，共同完善项目：
1. Fork 本仓库
2. 创建功能分支：git checkout -b feature/your-feature
3. 提交代码：git commit -m "feat: add new feature"
4. 推送分支：git push origin feature/your-feature
5. 提交 Pull Request
## 联系方式
- Email: haifengreal@qq.com
- Issues: https://github.com/joeyrensh/pub_finance/issues
<div align="center">

**⭐ 如果这个项目对你有帮助，请给一个 Star!**

[⬆ 返回顶部](#-量化分析框架)