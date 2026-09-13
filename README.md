# RoyaleAPI Downloader

RoyaleAPI 对局回放批量下载器。将玩家对局列表和回放事件合并为结构化 JSON，支持 SQLite 断点续传、跨玩家去重、固定赛季采集、历史高手玩家池持续采集和离线数据校验。

## 工作方式

```text
玩家种子 / 固定赛季名单
          ↓
列表页 → 卡组、等级、塔兵、对局时间和回放地址
          ↓
SQLite 持久队列 → 有界内存队列 → 并发下载回放
          ↓
解析与校验 → 原子写入 JSON → 持久索引
```

`session_curl` 模式采用 Patchright / Chromium 获取列表，curl_cffi 获取回放。每条代理绑定独立 Cookie，列表 profile 分开保存。也支持直接使用 `curl_cffi`、`patchright`、`ruyipage` 或外部 FlareSolverr 后端。

## 快速开始

建议 Python 3.12。核心代码使用 Python 3.11+ 的 `tomllib`；Windows 是原开发环境，跨平台离线测试由 GitHub Actions 执行。

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item config.example.toml config.toml
Copy-Item seeds.example.txt seeds.txt
```

Linux/macOS 激活命令为 `source .venv/bin/activate`，复制文件使用 `cp`。

先编辑 `seeds.txt` 和 `config.toml`。示例默认使用直接 HTTP 后端；网站要求浏览器校验或登录时，需要自行准备有效会话。示例不会附带可用账号、Cookie 或代理服务。

```powershell
# 仅核查配置，不下载
python -m crawler.main --config config.toml --seeds seeds.txt --dry-run
# 小批次试运行
python -m crawler.main --config config.toml --seeds seeds.txt --max-battles 100
# 只读查看状态
python -m crawler.main --config config.toml --status
```

Windows 也可使用 `START.cmd`，参数相同，例如 `START.cmd --config config.toml --status`。默认查找项目 `.venv`，否则使用 PATH 中的 Python。

选择 Patchright 或 SessionCurl 前，安装 Chromium：

```powershell
python -m patchright install chromium
```

SessionCurl 还需要代理与本地 Cookie 映射，见[配置与会话](docs/CONFIGURATION.md)。

## 固定赛季与新批次

- 新批次使用独立输出目录和数据库；可从旧库只读导入已完成对局 ID。
- 固定赛季需要用户准备带排名来源、准确赛季边界的名单文件；不会自动使用某个月的历史名单。
- 列表和回放按同场 `battle_tag` 去重；网站已过期的回放不能恢复。

见[批次与赛季](docs/CAMPAIGNS.md)。

## 持续采集与监测面板

`crawler.expert_continuous` 支持从指定时间持续回填历史对局并刷新新对局。可冻结已核验的玩家池，按实际出口共享速率预算，并在重启时恢复计数和未到期冷却。历史页与首页公平调度，避免回填任务长期等待。

列表浏览器有请求截止时间、内存保护和受限的初始化并发；回放客户端支持 Cookie 热加载。可选的内置会话维护最多同时恢复两个会话，需要人工登录或验证时会在面板提示。

见[持续采集运行指南](docs/CONTINUOUS.md)和[更新记录](CHANGELOG.md)。示例只配置一个本地代理入口，不包含可用节点或账号。

## 目录

| 路径 | 用途 |
| --- | --- |
| `crawler/main.py` | 命令行入口 |
| `crawler/crawler.py` | 列表、回放、重试和任务编排 |
| `crawler/client.py` | 网络及浏览器后端 |
| `crawler/queue.py` | SQLite 队列、事务与索引 outbox |
| `crawler/season.py` | 固定赛季名单导入与校验 |
| `crawler/expert_continuous.py`、`expert_pool.py` | 历史高手池、持续回填与新对局刷新 |
| `crawler/session_maintenance.py` | 有界会话恢复与真实回放检查 |
| `crawler/expert_dashboard.py` | 本机只读监测面板 |
| `crawler/test_*.py`、`crawler/selftest.py` | 无网络测试 |
| `data/` | 本地回放、数据库、浏览器会话；不提交 |

`production.py`、`authoritative_production.py` 和 lane 管理模块保留为高级运维工具，依赖使用者自己的 Mihomo、代理、账号和 native contract 配置。它们不是通用的一键服务，也未接入固定赛季启动命令。

## 验证与运行边界

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest crawler -q
python -m crawler.main --selftest
```

2026-09-12 发布检查：99 项离线测试通过。测试使用合成数据与模拟网络，不代表持续 24 小时或多日真实网络稳定性验收。

吞吐取决于站点响应、允许的请求速率、可用会话和每页新增对局数。每日 30 万场是采集目标，尚未完成连续 24 小时验收；瞬时速率或短窗口外推不能当作达标。详细说明见[架构与稳定性](docs/ARCHITECTURE.md)。

本仓库只发布源码、合成测试和示例配置，不含采集数据、登录信息、代理订阅、模型及原本地运维历史。数据采集及后续使用须遵守来源站点的访问规则。
