# crawler 文件导航

这里保留下载核心、持续采集、测试和仍有调用关系的高级辅助模块。旧启动器、独立守护程序、一次性迁移工具及旧桌面面板已清理。普通使用者只需配置并启动入口；其余模块由程序调用。

## 从哪里开始

| 你要做什么 | 先看哪里 |
| --- | --- |
| 下载指定玩家的回放 | [main.py](main.py)，启动方法见[项目首页](../README.md#快速开始) |
| 按赛季或历史排名名单筛选 | [season.py](season.py)、[批次与赛季](../docs/CAMPAIGNS.md) |
| 长期采集已核验玩家池 | [expert_continuous.py](expert_continuous.py)、[持续运行指南](../docs/CONTINUOUS.md) |
| 查看持续采集状态 | [expert_dashboard.py](expert_dashboard.py) |
| 理解下载流程 | 按 `main → crawler → client / parsers → queue / storage` 阅读 |
| 修改浏览器或会话处理 | [client.py](client.py)、[session_login.py](session_login.py)、[session_maintenance.py](session_maintenance.py) |

通过 `python -m crawler.main` 或 `python -m crawler.expert_continuous` 启动。下面标为“内部”的文件由入口导入，不需要逐个运行。

## 1. 下载核心

| 文件 | 作用 | 如何使用 |
| --- | --- | --- |
| [__init__.py](__init__.py) | Python 包标识与包说明。 | 内部 |
| [main.py](main.py) | 通用命令行入口，读取参数，选择普通、批次、赛季或高级采集模式。 | 普通下载入口 |
| [config.py](config.py) | 配置结构、TOML 加载、路径解析及参数校验。 | 修改配置行为时阅读 |
| [seeds.py](seeds.py) | 读取玩家种子文件，将标签转换为列表 URL。 | 内部 |
| [crawler.py](crawler.py) | 主流水线：列表发现、回放下载、筛选、重试与停止。 | 核心编排 |
| [client.py](client.py) | 网络后端：curl_cffi、Patchright、RuyiPage、SessionCurl 与 FlareSolverr。 | 浏览器和 HTTP 实现 |
| [parsers.py](parsers.py) | 解析列表、卡组、时间、历史分页及回放事件。 | 数据格式与解析 |
| [queue.py](queue.py) | SQLite 持久任务队列、去重、调度、重试与完成事务。 | 内部 |
| [queue_schema.py](queue_schema.py) | 队列表索引、调度优先级和事务计数的增量迁移。 | 内部；不是手动初始化脚本 |
| [storage.py](storage.py) | 原子保存回放、列表和索引。 | 内部 |
| [file_io.py](file_io.py) | 有界磁盘工作线程，避免文件写入阻塞网络任务。 | 内部 |
| [proxy_pool.py](proxy_pool.py) | 代理状态、健康检查、冷却和公平选择。 | 内部 |
| [ratelimit.py](ratelimit.py) | 异步令牌桶，控制请求节奏。 | 内部 |
| [resource_guard.py](resource_guard.py) | 检测可用内存，按暂停和恢复阈值保护采集。 | 内部 |
| [run_lock.py](run_lock.py) | 运行锁，阻止重复进程同时使用同一采集目录。 | 内部 |
| [curl_cffi_windows.py](curl_cffi_windows.py) | Windows 下 curl_cffi 异步选择器的兼容性处理。 | 内部 |

## 2. 玩家发现、批次与持续采集

| 文件 | 作用 | 何时需要 |
| --- | --- | --- |
| [campaign.py](campaign.py) | 构建独立批次和赛季输出目录，提供只读状态查询。 | 新建批次；通常由 main 调用 |
| [season.py](season.py) | 导入有排名来源和时间边界的名单，校验玩家资格。 | 固定赛季或历史高手名单 |
| [discovery.py](discovery.py) | 规范列表 URL、计算页面指纹、决定是否继续发现或回填。 | 内部 |
| [live_sources.py](live_sources.py) | 在进程内刷新榜单、种子和已有玩家的新对局。 | 启用动态来源时 |
| [source_store.py](source_store.py) | 持久保存来源的到期时间、重访和退避状态。 | 内部 |
| [refresh_seeds.py](refresh_seeds.py) | 从当前榜单生成带来源记录的玩家种子。 | 手动准备或更新种子 |
| [import_exclusions.py](import_exclusions.py) | 从已有清单导入对局 ID，防止重复下载。 | 复用旧库排除信息 |
| [expert_pool.py](expert_pool.py) | 保存历史排名证据、候选玩家和已核验成员，限制玩家池规模。 | 持续高手池模式内部 |
| [expert_continuous.py](expert_continuous.py) | 持续采集入口：已核验玩家池、历史回填、新对局刷新和冻结扩容。 | 长期采集入口 |

## 3. 会话、观测与诊断

| 文件 | 作用 | 说明 |
| --- | --- | --- |
| [session_login.py](session_login.py) | 打开登录浏览器，保存本地会话，并检查真实回放请求。 | 需要自己的浏览器环境与账号；可能需要人工操作 |
| [session_maintenance.py](session_maintenance.py) | 回放认证失败后，限制并发和超时地尝试恢复会话。 | 持续入口可选启用；不是额外的定时监控进程 |
| [cf_recover.py](cf_recover.py) | 浏览器验证的有界尝试和单通道恢复辅助。 | 不保证挑战通过；不能用尝试计数证明自动成功 |
| [list_observability.py](list_observability.py) | 列表页面缓存、请求延迟和通道统计。 | 内部 |
| [expert_dashboard.py](expert_dashboard.py) | 持续采集的本机只读网页面板。 | 这是持续运行指南使用的面板 |
| [capture.py](capture.py) | 交互式抓取网络请求，辅助确认页面与接口结构。 | 调试工具；输出可能含 Cookie，留在本地 |

“会话恢复成功”表示最终回放检查通过；即使用户手动完成验证，也会出现这个结果。因此不能仅凭 `ready` 或 `cf_attempts` 判断是否无人干预。

## 4. 高级数据校验

`authoritative` 是面向外部 native 回放管线的数据格式与准入校验模式，不表示数据获得 RoyaleAPI 官方认证。普通回放下载无需单独运行这些工具；部分校验代码也会被核心模块导入，因此不要直接删除文件。

| 文件 | 作用 | 前提 |
| --- | --- | --- |
| [authoritative.py](authoritative.py) | 离线升级回放 schema，并按外部 native contract 检查字段和准入条件。 | 了解目标格式与 contract |
| [authoritative_manifest.py](authoritative_manifest.py) | 只读整理旧数据升级需要的列表和回放依赖。 | 已有旧语料 |

## 5. 仍被调用的线路辅助模块

这两个模块仍有调用关系：`dynamic_lanes` 提供会话维护需要的回放地址选择、面板需要的进程检查，并依赖 `lane_manager`。因此暂时保留，动态线路管理仍默认关闭；普通使用者无需单独启动它们。

| 文件 | 作用 | 与默认流程的关系 |
| --- | --- | --- |
| [lane_manager.py](lane_manager.py) | 按预设端口管理 Mihomo 入口并分配出口。 | 需要本地 Mihomo 配置 |
| [dynamic_lanes.py](dynamic_lanes.py) | 原动态线路编排及共享辅助函数。 | 动态管理默认关闭；辅助函数仍被其他模块使用 |

## 6. 离线测试

测试使用合成数据、模拟网络或临时数据库，不参与生产下载。运行方式见[项目首页](../README.md#验证与运行边界)。

| 文件 | 覆盖内容 |
| --- | --- |
| [selftest.py](selftest.py) | 内置离线自检、模拟 Fetcher 和共享测试样例。 |
| [test_downloader_refactor.py](test_downloader_refactor.py) | 配置、存储、事务恢复与队列规模回归。 |
| [test_discovery.py](test_discovery.py) | 页面去重、历史导航、重叠窗口和公平调度。 |
| [test_live_sources.py](test_live_sources.py) | 来源刷新、重访、种子热加载和加速时间测试。 |
| [test_season.py](test_season.py) | 名单资格、赛季边界与历史回填。 |
| [test_expert_continuous.py](test_expert_continuous.py) | 高手池、持续筛选、历史游标和客户端恢复。 |
| [test_proxy_fairness.py](test_proxy_fairness.py) | 同出口会话公平轮转及共享限速。 |
| [test_cf_recovery.py](test_cf_recovery.py) | 验证辅助的页面限制与模拟浏览器行为。 |
| [test_session_login_flow.py](test_session_login_flow.py) | 模拟浏览器中的验证、登录和回放检查流程。 |
| [test_session_maintenance.py](test_session_maintenance.py) | 恢复后的状态发布、Cookie 更新与429冷却保留。 |
| [test_dynamic_lanes.py](test_dynamic_lanes.py) | 节点列表去重与同站回放样例选择。 |

根目录的配置、启动文件和文档用途见[仓库目录说明](../docs/REPOSITORY_MAP.md)。
