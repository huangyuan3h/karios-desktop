# Karios DB Backup + Migration — 2026-08-04

> **状态**：实施完成（2026-08-04），追踪在 [`optimization-checklist.md` OPT-061](../optimization-checklist.md)；落地摘要见 [`archive/2026-08-04-opt-061-db-backup-migrate.md`](../archive/2026-08-04-opt-061-db-backup-migrate.md)
>
> **背景**：2026-08-02 审查 todo §13 Longevity 时发现 OPT-053 已立"备份 3 副本策略"但**仓库里零脚本**——`换电脑也能跑`（OPT-056 已解决代码侧）还差**数据侧**。本文档拍板：
>   1. 备份触发与本地/iCloud 副本规则
>   2. 跨机迁移包（migrate tarball）的形态
>   3. 休眠场景下的兜底机制

---

## 0. 用户的"电脑就休眠"约束

场景：用户的 Mac 不会"长期开机"，而是**随用随醒**——可能白天工作、晚上睡眠、连续几天不用。
这意味着任何"夜间 cron"在睡眠期间**不会运行**，唤醒后也不会自动补跑错过的 job。

launchd 在 macOS 睡眠时的实际行为：

| 触发方式 | 睡眠期间 | 唤醒后 |
|----------|----------|--------|
| `StartCalendarInterval` (e.g. 03:00) | ❌ 不跑 | ❌ 不会自动补跑 |
| `Wake=true` | ❌ 不跑 | ✅ 如果 plist 本身因 wake 重新评估 |
| `RunAtLoad=true` | ❌ 不跑 | ✅ Login / `launchctl load` 时跑 |
| `WatchPaths` | ❌ 不跑 | ❌ |

launchd **没有** "系统唤醒后立即跑错过的 job" 这个能力——Apple 明确不支持。

要解决，**必须**让被调度的脚本自带"距上次备份多久了？"的检查：

- 上次备份 < 25h → 跳过（让 03:00 cron 安静）
- 上次备份 ≥ 25h → 立即备份一次（不管是谁触发的）

这条规则的 **触发器（trigger）** 不必严密——任何"用户活动"即可。叠加 3 个低成本 trigger：

1. **launchd 03:00 定时** — 机器在线场景的主路径
2. **launchd RunAtLoad=true** — 每次 login（早上开机 / 唤醒后解锁）触发
3. **~/.zshenv 一行 hook** — 每次开 shell 都触发（脚本内 25h 检查让日常 shell 开销几乎为零）

三者叠加 + last-age 检查，保证**最坏情况**（Mac 睡眠 N 天 → 用户某天醒来 → 打开 shell）→ 备份一次；**最好情况**（机器 7×24 不睡）→ 每天 03:00 一次。

---

## 1. 设计

### 1.1 三脚本分工

| 脚本 | 路径 | 触发 | 角色 |
|------|------|------|------|
| `db_backup.sh` | `services/data-sync-service/scripts/` | launchd / zshenv / 手动 | 增量备份（每天 / 上限 30d 本地 + 14d iCloud） |
| `db_restore.sh` | `services/data-sync-service/scripts/` | 手动 | 恢复到任意 Postgres 容器 + alembic upgrade + 校验 |
| `karios_migrate_export.sh` | `services/data-sync-service/scripts/` | 手动 | 打包完整 dump + env 模板 + standalone restore → tarball |

### 1.2 备份策略

| 维度 | 决策 | 理由 |
|------|------|------|
| 格式 | `pg_dump -Fc -Z 9 --no-owner --no-acl` | compressed custom format；体积压缩比 ~7x（1.7 GB → 245 MB）；parallel restore 支持 |
| 保留 | 本地 30 天 / iCloud 14 天 | iCloud 5 GB 免费档够存 ~20 份；本地给"恢复演练"用 |
| 异地副本 | `~/Library/Mobile Documents/com~apple~CloudDocs/Karios/backups/postgres/` | iCloud Drive 客户端自己处理同步，**Mac 睡眠时也在跑**（这是 §13 Longevity 在不上云方案下唯一能用的"异地"） |
| 校验 | 每次 dump 后跑 `pg_restore --list` | TOC parse 失败 → dump 标记 `.corrupt` + exit 非零 → launchd 留下错误日志 |
| 容器识别 | 自动找"image=postgres + ports 暴露 $PG_PORT"的那个 | 不硬编码（dev `postgres-db` vs compose `karios-postgres`）；可手动 `KARIOS_PG_CONTAINER=` 覆盖 |
| 跳过逻辑 | 上次 dump < 25h → exit 0 | cron 安静的代价：脚本本身的 stdio 输出仍是 "skip ..."，方便排错 |

### 1.3 last-age 阈值为什么是 25h

不是 24h 而是 25h。原因：

- 03:00 cron 如果正常跑了，下次凌晨 03:00 距上次是 24h（边界 case）
- 如果 cron 因 sleep 错过，RunAtLoad 触发 → 仍 < 24h → 跳过，等下一个 03:00
- 如果连续 2 次错过（machine off over a weekend），下次 login 是周一 10:00，距上次 49h+ → 强制 dump

25h 给凌晨 03:00 留 1h 冗余，不至于产生"差 1 秒也要重 dump"的副作用。

### 1.4 迁移包（migrate tarball）形态

```
karios-migrate-<ts>/
├── README.txt                    # 新 Mac 上 5 步快速上手
├── karios-<ts>.dump              # 完整 dump (~245 MB)
├── karios-<ts>.manifest.json     # pg version / table count / size
├── karios_restore.sh             # 拷贝自 db_restore.sh（无 repo 依赖）
├── env.template                  # .env 模板（敏感字段 __REDACTED__）
└── checksums.sha256              # tamper detection
```

打包后：~244 MB（gzip 后），可放进任何 USB / iCloud / 邮件附件（< 25 MB 限制外）。

**接收端流程**（文档化在 README.txt）：

```sh
# 1. 在新 Mac 起一个 Postgres 容器（任意 name / 任意 port）
docker run -d --name postgres-db -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin123 -e POSTGRES_DB=karios-desktop \
  -p 5432:5432 postgres:16-alpine

# 2. 解 tarball 到任意目录
tar -xzf karios-migrate-20260804-195239.tar.gz
cd karios-migrate-20260804-195239

# 3. 恢复（可选 --drop-existing 覆盖同名 db）
./karios_restore.sh karios-20260804-195239.dump

# 4. 复制 env 模板并填敏感字段
cp env.template /path/to/.env
# 编辑 .env：填 TU_SHARE_API_KEY / OPENAI_API_KEY / POSTGRES_PASSWORD 等

# 5. 启 stack
pnpm install
./scripts/docker-up.sh
```

### 1.5 不做异地副本到外置盘

用户没明确提到有外置盘；iCloud Drive 已在用且免费档够用。如果未来需要：

- `KARIOS_BACKUP_ICLOUD_DIR` 改成 `/Volumes/<外置盘>/Karios/backups/postgres`
- 但外置盘必须在 Mac 唤醒 + 挂载时才能同步 —— 睡眠场景下与本地副本等价

不预先实现，避免设计 YAGNI。

---

## 2. launchd plist 关键设计

```xml
<key>StartCalendarInterval</key>
<dict>
    <key>Hour</key><integer>3</integer>
    <key>Minute</key><integer>0</integer>
</dict>
<key>Wake</key><true/>
<key>RunAtLoad</key><true/>
<key>EnvironmentVariables</key>
<dict>
    <key>DATABASE_URL</key><string>postgresql://admin:admin123@localhost:5432/karios-desktop</string>
    <key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
</dict>
```

- **Wake=true** 在 macOS 上语义模糊：launchd 文档说"wake events are processed"，但实测 StartCalendarInterval 仍不会补跑。把它加上是**安全冗余**，不依赖它工作。
- **DATABASE_URL 写在 plist 里**（不进 shell 环境）—— 避免 launchd 启动时的 shell 上下文缺失。
- **LimitLoadToSessionType=Aqua**（launchd 默认）—— 只在 GUI session 跑，SSH session 不会意外触发。

---

## 3. 已知风险与缓解

| 风险 | 缓解 |
|------|------|
| iCloud 客户端被禁用 / 退出登录 | 脚本检测 `~/Library/Mobile Documents/com~apple~CloudDocs` 存在性，缺失则只保留本地 + log warn |
| Postgres 容器名变了（如 `karios-postgres` vs `postgres-db`） | 脚本按"image=postgres + port=$PG_PORT"自动检测；`KARIOS_PG_CONTAINER=` 可强制 |
| dump 进行中 Mac 突然关机 | 下次脚本启动时 last-age 检查 → 立即重 dump；旧的不完整文件被 `pg_dump` 覆盖，不留半成品 |
| iCloud 空间满 | dump 复制失败 → log warn + exit 0（**不**破坏本地 backup）；用户会看到 logs/db-backup.err.log 报错 |
| `.env` 被误删 → launchd 没有 DATABASE_URL | `install-db-backup-launchd.sh` 在每次 install / status 时打印当前 plist 里的 DATABASE_URL；如果 .env 改了，**重跑 install** 同步到 plist |
| 备份期间 Postgres 大量写入（cron 跑 03:30） | `pg_dump` 默认 `--single-transaction` 不是，但 -Fc 单线程 dump 对正在写入的 db 是安全的；dump 完后 manifest 校验会捕获一致性问题 |

---

## 4. 不做（避免 YAGNI）

- ❌ **加密 dump** — 数据已含个人交易记录，但 iCloud Drive 是 Apple E2E 加密的；用户本地有 pg_dump 备份的 copy 已是明文；再加一层加密徒增复杂度。
- ❌ **Wal archiving / PITR** — §13 Longevity 用户原话"保证系统长期有生命力"，不是"保证任意时刻可回滚到秒级"；24h 数据窗口足够。
- ❌ **自动上传到 S3 / 异地云** — §13 #1 Neon 副本暂缓（用户 review）；本地 iCloud 已满足"换电脑也能跑"。
- ❌ **Postgres BTRFS / ZFS snapshot** — 容器内文件系统是 docker volume，btrfs/zfs 不适用。
- ❌ **打包成 .dmg / .pkg** — tarball + restore.sh 已足够；新 Mac 上 git clone + `pnpm install` 即可起 UI。
- ❌ **备份 scheduler 自动检测** — 当前靠 launchd 03:00 + RunAtLoad + zshenv 已覆盖；再加 watchdog 是过度设计。

---

## 5. 验证（2026-08-04 实施时实测）

- [x] `bash db_backup.sh --dry-run` 显示正确路径
- [x] `bash db_backup.sh --force` 端到端 dump + verify + iCloud mirror OK（245 MB / 1m33s）
- [x] `bash db_backup.sh`（无 --force） 5s 后 skip（last backup 3s 前）
- [x] `bash db_restore.sh <dump> --drop-existing` drop + recreate + pg_restore 21s + alembic 1s + 表数校验通过
- [x] 新 Mac 模拟：`docker run postgres:16-alpine` 全新容器 → 解 tarball → `KARIOS_PG_CONTAINER=... ./karios_restore.sh ...` → 44 张表全恢复 + 抽样 daily 表 10.9M 行 + 00700.HK 2026-08-04 close 487.6 数据完整
- [x] launchd plist `plutil -lint` OK；`launchctl load -w` 加载 OK；RunAtLoad=true 触发跑一次正确读到 DATABASE_URL + skip 逻辑

---

## 6. 与 todo.md / 其他设计稿的衔接

- [todo §12 #18](../todo.md) 是本条目的源头
- [designs/db-direction-2026-08.md](./db-direction-2026-08.md) §3 的"备份 3 副本"具体落地（本地 + iCloud + 异地？见 §1.5：当前只 2 副本）
- [designs/karios-longevity-2026-08.md](./karios-longevity-2026-08.md) §3 "换电脑也能跑"的数据侧补完（与 §12 #7 Docker 的代码侧互补）
- [designs/mac-mini-deployment.md](./mac-mini-deployment.md) §5 实施时序应在 Mac mini 拿到当天跑 `install-db-backup-launchd.sh`
- §13 Longevity #1 Neon 副本仍是 🟡 暂缓，但 iCloud Drive 已能应对"换电脑也能恢复"场景