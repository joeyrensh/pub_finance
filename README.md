# 📊 量化分析框架

> **开源免费** · 美股/A 股全市场数据分析 · 量化策略回测

---

## 🎯 框架概述

本量化分析框架是一个**开源免费**的全市场数据分析系统，支持 **美股** 和 **A 股** 市场的量化策略研究、回测与分析。

框架提供从数据获取、存储、策略分析到收益评估的完整解决方案，帮助投资者快速验证交易策略、分析买卖点、评估风险收益比。

---

## 📸 功能预览

### 回测分析界面

![回测分析](https://github.com/joeyrensh/pub_finance/finance/Qanalysis1.jpg)

*图 1: 回测分析主界面 - 支持 A 股/美股选择、股票代码输入、回测日期设置*


### 买卖点分析

![买卖点分析](https://github.com/joeyrensh/pub_finance/finance/Qanalysis2.jpg)

*图 2: 买卖点分析 - K 线图与买卖信号标记*

---

## 🚀 核心功能

| 模块 | 功能描述 |
|------|----------|
| 📥 **数据源获取** | 支持 Yahoo Finance、东方财富、新浪财经等多源数据接入 |
| 💾 **数据存储** | 本地 CSV/JSON 存储，支持增量更新与数据校验 |
| 📈 **量化策略** | 多策略回测框架，支持自定义买卖点逻辑 |
| 📊 **收益分析** | 年化收益、累计收益、最大回撤、风险指标等全面评估 |
| 📋 **个股报告** | 自动生成个股关键信息报告（行业、市值、财务指标等） |

---

## 📊 核心指标

框架提供以下核心分析指标：

| 指标 | 说明 |
|------|------|
| **ANN.R** | 年化收益率 (Annualized Return) |
| **CUM.R** | 累计收益率 (Cumulative Return) |
| **MX.DD** | 最大回撤 (Maximum Drawdown) |
| **D.RISK** | 日风险 (Daily Risk) |
| **30D/120D** | 30 日/120日滚动收益 |

---

## 🔧 技术架构

```
pub_finance/
├── finance/
│   ├── data/              # 数据存储目录
│   │   ├── us_stockdetail.csv   # 美股持仓
│   │   └── cn_stockdetail.csv   # A 股持仓
│   ├── utility/           # 工具脚本
│   │   ├── proxy/         # 代理 IP 池
│   │   └── *.py           # 数据获取脚本
│   ├── us.log             # 美股策略日志
│   ├── cn.log             # A 股策略日志
│   └── README.md          # 本文件
└── ...
```

---

## 📈 策略回测流程

1. **选择市场**: A 股 / 美股
2. **输入股票代码**: 支持多只股票组合回测
3. **设置回测日期**: 自定义回测时间范围
4. **执行回测**: 自动计算买卖点、收益率、风险指标
5. **生成报告**: 可视化图表 + 数据表格

---

## 🎨 界面特色

- **收益与回撤图**: 红色曲线为累计收益，绿色区域为回撤
- **K 线图**: 红绿蜡烛图 + 成交量柱状图
- **买卖信号**: 圆圈标记买入 (红)/卖出 (绿) 时机
- **数据表格**: 年度收益对比、风险指标一目了然

## 服务部署
PYSPARK_PYTHON = /home/ubuntu/miniconda3/bin/python
PYSPARK_DRIVER_PYTHON = /home/ubuntu/miniconda3/bin/python
00 7 * * * cd /home/ubuntu/pub_finance/finance ; /home/ubuntu/miniconda3/bin/python -u /home/ubuntu/pub_finance/finance/usstock_main.py > /home/ubuntu/pub_finance/finance/us.log 2>&1
30 15 * * * cd /home/ubuntu/pub_finance/finance ; /home/ubuntu/miniconda3/bin/python -u /home/ubuntu/pub_finance/finance/cnstock_main.py > /home/ubuntu/pub_finance/finance/cn.log 2>&1

## 🤝 贡献指南

欢迎提交 Issue 和 Pull Request！

1. Fork 本仓库
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

---

## 📬 联系方式

- 📧 Email: haifengreal@qq.com
- 💬 Issues: [GitHub Issues](https://github.com/joeyrensh/pub_finance/issues)

---

<div align="center">

**⭐ 如果这个项目对你有帮助，请给一个 Star!**

[⬆ 返回顶部](#-量化分析框架)

