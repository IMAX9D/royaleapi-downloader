# 配置与会话

复制 `config.example.toml` 为 `config.toml`。相对文件路径以 TOML 所在目录为基准。未知配置项会直接报错；本地配置不提交 Git。

## 后端

| backend | 必需条件 |
| --- | --- |
| `curl_cffi` | 可访问的网络；如需会话可设置 `captured_file` |
| `patchright` | Patchright Chromium、可用持久 profile |
| `session_curl` | Patchright 列表浏览器、代理与独立 Cookie 映射 |
| `ruyipage` | RuyiPage 浏览器运行环境及登录态 |
| `flaresolverr` | 自行运行的 FlareSolverr 服务 |

SessionCurl 读取 `ruyi_auth_map_file`。在本地创建 `data/auth_sessions/active_map.json`，形式如下（这里没有真实凭据）：

```json
{
  "http://127.0.0.1:18080": "data/auth_sessions/lane01-profile"
}
```

映射值相对于启动工作目录；建议从仓库根目录启动，或填写绝对路径。该 profile 对应同目录的 `lane01.json`：

```json
{
  "cookies": [
    {"name": "COOKIE_NAME_FROM_YOUR_SESSION", "value": "YOUR_LOCAL_VALUE"}
  ]
}
```

Cookie 名和值必须来自自己的有效会话，上面的占位内容不能用于下载。将映射中的代理加入 `[proxy].proxies`，配置 `backend="session_curl"` 和 `ruyi_auth_map_file="data/auth_sessions/active_map.json"`。列表代理另由 `list_proxy_urls` 指定，通用 CLI 还包含直连列表通道；持续采集入口会按非空的 `list_proxy_urls` 筛选列表通道。

`session_login.py` 需要 RuyiPage 浏览器环境和仓库根目录的 `config.toml`。通过 `--replay-url` 指定当前可用的真实回放地址；未提供时尝试读取本地回放索引。登录状态保存在 `data/auth_sessions/`。持续采集可选的会话维护也使用此目录，详见[运行指南](CONTINUOUS.md)。

## 先小规模验证

检查代理服务存在、名单可读、输出磁盘足够，再用 `--max-battles 100` 运行小批次。不要把 workers 数当成目标请求速率；速率由令牌桶和全局回放限制共同控制。

示例内存阈值为低于 8 GiB 暂停新列表、高于 10 GiB 恢复。内存较小的机器应根据实际情况降低这两个值，并保持恢复阈值大于暂停阈值，否则可能始终无法开始列表工作。

`request_timeout` 用于 HTTP 请求；Patchright 页面内请求使用 AbortController，外层等待也有截止时间。浏览器首次初始化另有额外的时间预算。SessionCurl 会检测 Cookie 文件变化并热加载，浏览器 Cookie 与 HTTP 传输参数仍需保持一致。

高级 authoritative 模式需要外部工程提供冻结 native contract。仓库未附带机器专用的 `config.authoritative.toml` 或任何训练集。
