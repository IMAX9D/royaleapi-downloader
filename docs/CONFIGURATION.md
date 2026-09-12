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

Cookie 名和值必须来自自己的有效会话，上面的占位内容不能用于下载。将映射中的代理加入 `[proxy].proxies`，配置 `backend="session_curl"` 和 `ruyi_auth_map_file="data/auth_sessions/active_map.json"`。列表代理另由 `list_proxy_urls` 指定，代码还包含一个直连列表通道。

`session_login.py` 是原环境的辅助工具，需要既有 `data/index.jsonl` 回放示例与 RuyiPage 环境，不能当作空白安装的自动登录入口。

## 先小规模验证

检查代理服务存在、名单可读、输出磁盘足够，再用 `--max-battles 100` 运行小批次。不要把 workers 数当成目标请求速率；速率由令牌桶和全局回放限制共同控制。

示例内存阈值为低于 8 GiB 暂停新列表、高于 10 GiB 恢复。内存较小的机器应根据实际情况降低这两个值，并保持恢复阈值大于暂停阈值，否则可能始终无法开始列表工作。

`request_timeout` 当前用于 curl 等后端；它没有覆盖 Patchright 页面内 fetch 的整个调用，这是已记录的稳定性缺口。

高级 authoritative 模式需要外部工程提供冻结 native contract。仓库未附带机器专用的 `config.authoritative.toml` 或任何训练集。
