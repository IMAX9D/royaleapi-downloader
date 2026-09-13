# 持续采集运行指南

持续入口从已核验的历史高手名单启动，按指定开始时间回填历史并周期性刷新新对局。以下命令从仓库根目录执行，本地配置、会话和数据均留在被 Git 忽略的目录内。

## 准备

1. 按 README 安装依赖，并安装 Patchright Chromium。
2. 按[批次与赛季](CAMPAIGNS.md)生成带来源证据的名单。仓库不附带真实玩家池。
3. 复制 `config.example.toml` 为 `config.toml`，按[配置与会话](CONFIGURATION.md)设置 `backend="session_curl"`、有效代理入口及 `ruyi_auth_map_file`。先确认本地代理服务和会话可以下载真实回放。
4. 创建新输出目录并复制持续配置：

```powershell
New-Item -ItemType Directory -Force data/continuous
Copy-Item settings.continuous.example.json data/continuous/settings.local.json
```

编辑 `settings.local.json`，使 `output_dir` 与该目录一致。TOML 内的路径以 TOML 目录为基准；持续 JSON 的文件路径以启动工作目录为基准。建议始终从仓库根目录启动。

示例的 `start_timestamp=1785747600` 表示 2026-08-03 09:00 UTC，是特定历史批次的起点。更换批次时重新确定时间和名单，使用新的输出目录。

示例只有一个本地代理入口、4 个 worker、1 个列表 worker，回放上限为 0.15 请求/秒。`daily_target` 仅用于显示目标，不会自动提高速率，也不表示示例能达到目标。

`expand_player_pool=false` 冻结已有核验名单；`expert_target` 是人数上限，不会把不足的人数自动补齐。若要扩容，显式开启后再核查资格来源。

## 启动、状态与停止

```powershell
# 检查配置及名单，不联网下载
python -m crawler.expert_continuous --settings data/continuous/settings.local.json --dry-run
# 启动采集
python -m crawler.expert_continuous --settings data/continuous/settings.local.json
```

在另一个终端启动只读面板：

```powershell
python -m crawler.expert_dashboard --root data/continuous --port 19741
```

打开 <http://127.0.0.1:19741/>。面板仅监听本机，显示持久计数、心跳、队列、通道冷却和本次进程的会话恢复结果。瞬时落盘速度可能波动，评估持续吞吐应使用较长窗口内新增且保存成功的唯一对局数。

只读查询也可使用：

```powershell
python -m crawler.expert_continuous --settings data/continuous/settings.local.json --status
```

在采集终端按 Ctrl+C 正常停止，或创建输出目录下的 `STOP` 文件。确认进程退出后，需要继续时删除这个标记并使用相同设置重新启动。不要同时对同一数据库启动多个采集进程。

## 去重、速率与恢复

`exclude_databases` 可填已有进度数据库路径数组，`exclude_manifest` 可填已有排除清单路径；不需要时省略。导入完成对局标识用于去重，不会删除旧文件。

`rate_groups` 将本地代理入口映射为实际出口组；同一出口的多个会话必须共享组。worker 数量不是访问额度。列表与回放是两类请求，都要考虑站点允许的总量；遇到限速保留退避。

`auto_session_recovery=true` 可启用内置事件驱动会话维护，无须另外启动监测脚本。它依赖可用的 RuyiPage 浏览器环境、根目录 `config.toml` 和 `data/auth_sessions/<会话名>.json`。恢复任务重用已登录会话，最多并发两个；不足 6 GiB 可用内存时延后，不会持续打开所有浏览器。

内置维护需要本批次索引中已有可用回放地址。空白批次的首次登录、账号失效或仍未通过的人工验证需要自行处理。网站挑战自动恢复不保证成功，面板会提示需要人工操作的会话。

可选 `session_registry` 指向本地已验证会话登记文件；只有登记为 ready 的会话参与工作。它是高级运维输入，不能仅将状态改为 ready 来代替真实回放验证。默认示例省略它。

旧 `dynamic_lanes`、`expert_supervisor` 和 watchdog 不在默认流程中启用；它们依赖额外的 Mihomo 配置及端口约定。订阅 URL、节点密码、Cookie、账号信息和实际配置不得加入公开仓库。
