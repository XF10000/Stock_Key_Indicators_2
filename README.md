# A股关键指标分析工具

一个本地化运行的命令行工具，用于获取、分析和可视化A股上市公司的关键财务指标，帮助进行基本面研究。

## 项目概述

### 核心功能

1. **数据获取与存储**：通过 `akshare` 自动获取全A股（约5000家公司）的历史财务报表数据，存入本地SQLite数据库
2. **指标计算**：根据预设公式计算四大核心财务指标
3. **对比分析**：生成指定股票的10年历史趋势，并与全市场中位数对比
4. **可视化输出**：生成HTML报告和Excel文件

### 四大核心指标

1. **回款能力**：应收账款周转率 vs. 毛利率
2. **再投资质量**：长期经营资产周转率
3. **产业链地位**：营运净资本 / 总资产
4. **真实盈利能力**：经营活动现金流净额 / 总资产

## 项目结构

```
Stock_key_indicators2/
├── data_provider/          # 数据访问层
│   ├── akshare_client.py   # API客户端
│   └── repository.py       # 数据库交互
├── analysis/               # 业务逻辑层
│   ├── calculator.py       # 指标计算
│   └── analyzer.py         # 统计分析
├── visualization/          # 可视化层
│   └── plotter.py          # 图表生成
├── utils/                  # 通用工具
│   ├── config_loader.py    # 配置加载
│   └── logger.py           # 日志记录
├── tests/                  # 测试
│   ├── unit/              # 单元测试
│   └── integration/       # 集成测试
├── notebooks/             # Jupyter notebooks
├── docs/                  # 项目文档
├── config/                # 配置文件
│   └── column_mapping.yaml # 列名映射
├── output/                # 输出目录
│   ├── html/             # HTML报告
│   ├── excel/            # Excel文件
│   └── reports/          # 数据质量报告
├── logs/                  # 日志文件
├── models.py              # 数据模型
├── data_updater.py        # 数据更新程序
├── main.py                # 主分析程序
├── orchestrator.py        # 流程调度器
├── config.yaml            # 主配置文件
├── requirements.txt       # 依赖列表
└── pyproject.toml         # 项目配置
```

## 环境设置

### 1. 克隆项目

```bash
git clone https://github.com/XF10000/Stock_Key_Indicators_2.git
cd Stock_key_indicators2
```

### 2. 创建虚拟环境

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

## 使用指南

### 第一步：初始化数据库（必须）

```bash
python data_updater.py
```

- **作用**：获取所有A股公司的历史财务数据并存入本地数据库
- **注意**：首次运行会非常耗时（可能数小时），但支持断点续传
- **测试运行**：`python data_updater.py --limit 10` （仅更新10只股票）

### 第二步：运行分析

```bash
python main.py --code <stock_code>
```

**示例**：
```bash
python main.py --code SH600519  # 分析贵州茅台
```

- **作用**：对指定股票执行分析，生成HTML报告和Excel文件
- **输出位置**：`output/html/` 和 `output/excel/`

## 技术栈

- **编程语言**：Python 3.10+
- **数据处理**：Pandas, NumPy
- **数据库**：SQLite + SQLAlchemy (ORM)
- **数据获取**：Akshare
- **数据可视化**：Matplotlib, Seaborn
- **测试框架**：Pytest
- **代码质量**：Black, Flake8, Mypy

## 开发阶段

项目采用4阶段迭代开发：

- [x] **阶段0**：项目脚手架与数据模型
- [ ] **阶段1**：核心数据管道原型验证
- [ ] **阶段2**：健壮的数据更新器
- [ ] **阶段3**：核心分析与计算引擎
- [ ] **阶段4**：主程序与可视化

## 测试

```bash
# 运行所有测试
pytest

# 运行特定测试
pytest tests/unit/test_calculator.py

# 生成覆盖率报告
pytest --cov=. --cov-report=html
```

## 代码质量

```bash
# 代码格式化
black .

# 代码检查
flake8 .

# 类型检查
mypy .
```

## 配置说明

主配置文件：`config.yaml`

- **database**：数据库路径
- **api**：API请求参数（延迟、重试次数等）
- **performance**：性能参数（内存限制、分块大小等）
- **output**：输出目录配置
- **logging**：日志配置

列名映射：`config/column_mapping.yaml`

- 用于标准化从API获取的财务报表列名
- 在原型验证阶段会根据实际数据补充完善

## 文档

详细文档位于 `docs/` 目录：

- [需求澄清报告](docs/需求澄清报告.md)
- [技术调研报告](docs/技术调研报告.md)
- [架构设计文档](docs/架构设计文档.md)
- [开发流程和阶段划分](docs/开发流程和阶段划分.md)
- [测试用例设计](docs/测试用例设计.md)
- [质量保证策略](docs/质量保证策略.md)
- [风险评估报告](docs/风险评估报告.md)

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！

## 联系方式

- GitHub: [XF10000](https://github.com/XF10000)
- 项目地址: https://github.com/XF10000/Stock_Key_Indicators_2
