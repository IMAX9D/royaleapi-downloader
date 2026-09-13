# 仓库目录说明

本仓库把可复用下载模块、长期运行能力、离线测试与仍被调用的高级辅助模块放在同一个 Python 包中。文件多是因为这些职责分开实现；正常下载只需要运行一个采集入口。

## 根目录文件

| 文件 | 作用 |
| --- | --- |
| [README.md](../README.md) | 项目简介、快速开始和使用边界。 |
| [START.cmd](../START.cmd) | Windows 启动包装，查找 Python 并调用通用命令行入口。 |
| [requirements.txt](../requirements.txt) | 运行依赖。 |
| [requirements-dev.txt](../requirements-dev.txt) | 开发与测试依赖，包含运行依赖。 |
| [config.example.toml](../config.example.toml) | 通用配置示例；复制为本地 config.toml 后编辑。 |
| [settings.continuous.example.json](../settings.continuous.example.json) | 持续采集的配置示例；复制到自己的输出目录使用。 |
| [seeds.example.txt](../seeds.example.txt) | 玩家标签或 URL 的种子文件示例。 |
| [proxies.example.txt](../proxies.example.txt) | 代理入口格式示例，不含可用服务。 |
| [CHANGELOG.md](../CHANGELOG.md) | 已发布改动记录。 |
| [.gitignore](../.gitignore) | 排除本地配置、凭据、数据和临时产物。 |
| [.gitattributes](../.gitattributes) | Git 文本和换行约定。 |
| [.github/workflows/tests.yml](../.github/workflows/tests.yml) | GitHub 上的 Windows/Linux 离线测试、自检与配置检查。 |

## 源码与文档

| 路径 | 作用 |
| --- | --- |
| [crawler/README.md](../crawler/README.md) | 每个 Python 文件的分类、职责与阅读顺序。 |
| [ARCHITECTURE.md](ARCHITECTURE.md) | 浏览器、HTTP、队列、持久化和稳定性边界。 |
| [CONFIGURATION.md](CONFIGURATION.md) | 网络后端、代理和本地会话映射。 |
| [CAMPAIGNS.md](CAMPAIGNS.md) | 独立批次、历史名单、赛季时间和旧库排除。 |
| [CONTINUOUS.md](CONTINUOUS.md) | 持续采集的配置、启动、停止、续跑与监测。 |
| [REPOSITORY_MAP.md](REPOSITORY_MAP.md) | 本页：仓库文件导航。 |

## 运行后产生的本地文件

这些不是公开仓库的一部分。输出位置可由使用者配置。

| 位置或名称 | 内容 |
| --- | --- |
| `config.toml`、`settings.local.json` | 机器自己的运行参数。 |
| `data/auth_sessions/` | 登录会话、Cookie、浏览器 profile 与状态。 |
| 输出目录下的 `progress.sqlite3` | 持久任务、完成记录和排除标识。 |
| 输出目录下的 `expert-pool.sqlite3` | 持续模式的候选玩家、排名证据与成员。 |
| 回放文件、`index.jsonl` | 已保存的结构化数据和索引。 |
| `lanes/crawler-proxies.json`、日志 | 通道、队列、资源和运行状态。 |
| `mihomo-lanes.yaml`、私有订阅目录 | 使用者自己的代理配置与节点信息。 |

源码目录保持现有模块路径，方便已有命令和导入继续使用。已移除9个未被当前采集入口使用的旧文件；核心代码仍会导入部分高级校验和线路辅助函数，因此这些依赖继续保留。
