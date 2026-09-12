# 批次与赛季

## 新批次

```powershell
python -m crawler.main --config config.toml --campaign sample-202609 --since 2026-09-01 --max-battles 100 --seeds seeds.txt --no-live-sources --dry-run
```

删除 `--dry-run` 才会开始。新批次输出到 `output_dir/campaigns/<name>`，进度库单独存储。默认新批次启用动态来源，上面的例子用 `--no-live-sources` 限制为用户指定种子。

需要跳过以前已完成的对局时，加 `--exclude-db <存在的旧数据库路径>`，可重复。此操作读取旧库完成 ID，不导入其 pending 队列。旧库不在时不要传入该参数。

查询同一批次时保留选择参数，将 `--dry-run` 改为 `--status`。

## 固定赛季

固定赛季不接收外部 seeds。用户先准备公开排名页的本地 HTML，通过离线导入工具生成名单：

```powershell
python -m crawler.season --season 2026-08 --top 1000 --html work/leaderboard.html --start-utc 2026-08-03T09:00:00Z --end-utc 2026-09-07T09:00:00Z --boundary-source "User verified season schedule" --source-url https://royaleapi.com/players/leaderboard/season/2026-08 --output data/rosters/season-2026-08.json
```

以上日期仅演示历史批次参数，其他赛季应重新核对官方日程；不能直接用自然月边界。`--top` 必须与名单人数一致，名单不完整时会报错。

```powershell
python -m crawler.main --config config.toml --season 2026-08 --season-top 1000 --season-roster data/rosters/season-2026-08.json --dry-run
```

删除 `--dry-run` 开始，用 `--status` 查看。相同参数续跑；替换名单或时间条件时新建批次。`--historical-pool` 可导入有历史 Top 10000 结算证据的玩家，详见 `python -m crawler.season --help`。

历史游标以毫秒计，对局时间以秒计。名单模式限制同站、同玩家、严格递减的游标；不会向榜外玩家扩散。网站已经丢失的历史回放不在可恢复范围内。
