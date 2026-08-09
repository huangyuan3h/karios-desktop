# OPT-061 DB 本地备份自动化 + 跨机迁移包 · 归档于 2026-08-04

## 当时的目标（todo 链接）
- `docs/todo.md §12 #18`：DB 本地备份自动化（pg_dump 日备 + 本地/异地双副本 + 恢复演练）；2026-08-02 审查 §13 Longevity 时发现 OPT-053 已拍板"备份 3 副本策略"但**仓库里零脚本**——`换电脑也能跑`（OPT-056 已解决代码侧）还差**数据侧**。
- `docs/todo.md §13` Longevity：用户原话"我关心的无非换电脑也能正常跑这个系统，让这个系统长期有生命力，远程也能访问这几个痛点"。
- **用户 2026-08-04 补充约束**："我现在电脑就休眠考虑这一点的情况下做，保证我能恢复数据和转移数据" → 备份触发必须在 sleep / wake 场景下仍能跑。

## 实际做了什么

### 1. 三个核心脚本

| 脚本 | 行数 | 角色 |
|------|------|------|
| `services/data-sync-service/scripts/db_backup.sh` | ~140 | `pg_dump -Fc -Z 9` via `docker exec` → 本地 30d + iCloud 14d → TOC 校验 + manifest |
| `services/data-sync-service/scripts/db_restore.sh` | ~170 | `docker cp` → `pg_restore --jobs=4` → alembic upgrade head → 表数 cross-check |
| `services/data-sync-service/scripts/karios_migrate_export.sh` | ~140 | bundle dump + manifest + env.template + standalone restore.sh + checksums → tar.gz (~244 MB) |

### 2. launchd plist 安装脚本

- `scripts/install-db-backup-launchd.sh`（~180 行 · 模仿 §12 #7 `install-launchd.sh` 风格）
- plist：`StartCalendarInterval` 03:00 + `Wake=true` + `RunAtLoad=true` + DATABASE_URL env 写入 plist
- 提供 `--status` / `--unload` 子命令
- 可选 append `~/.zshenv` hook（每次开 shell 触发备份检查）

### 3. 休眠兜底机制（核心设计）

launchd 在 macOS 睡眠时 StartCalendarInterval **不会运行**、唤醒后**不会补跑**错过的 job（Apple 不支持这个能力）。

解决方案 = **3 trigger 叠加 + last-age 检查**：

| Trigger | 频率 | 代价 |
|---------|------|------|
| launchd 03:00 cron | 1/d | 0（脚本自我 skip） |
| launchd RunAtLoad | 每次 login | 0 |
| ~/.zshenv hook | 每次开 shell | 0 |
| 脚本内 last-age < 25h skip | — | — |

最坏情况：Mac 睡眠 N 天 → 用户某天开 shell → zshenv hook 触发 → last-age > 25h → 立即 dump（< 90s）。
最好情况：机器 7×24 不睡 → 每天 03:00 cron 一次，其他 trigger 自动 skip。

阈值取 25h 而非 24h：给凌晨 03:00 留 1h 冗余，避免"差 1 秒也要重 dump"的副作用。

### 4. 异地副本策略：iCloud Drive（不是 S3 / 外置盘）

`~/Library/Mobile Documents/com~apple~CloudDocs/Karios/backups/postgres/`：

- **Mac 睡眠时 iCloud 客户端仍在同步**——这是 §13 "不上云"约束下唯一能用的"异地"
- 5 GB 免费档够存 ~20 份 dump（每份 ~245 MB）
- dump 失败时 cp 失败 → log warn → 不破坏本地备份
- 检测 `~/Library/Mobile Documents/com~apple~CloudDocs` 存在性；用户禁用 iCloud 时只保留本地

### 5. 验证（端到端 2 次）

**Round-trip（同一容器）：**
```
$ bash db_backup.sh --force
[19:54:12] verified — 245M (256402414 bytes) / 44 tables / pg 16.11
[19:54:12] mirrored to iCloud: /Users/huangyuan/Library/Mobile Documents/...

$ bash db_restore.sh <dump> karios_restore_scratch --drop-existing
[20:00:24] dropping existing database karios_restore_scratch...
[20:00:25] pg_restore -> karios_restore_scratch @ postgres-db
[20:00:46] pg_restore ok
[20:00:47] running alembic upgrade head...
[20:00:47] alembic upgrade head ok
[20:00:47] verifying row counts...
[20:00:47] manifest cross-check ok — 44 public tables (expected 44)
[20:00:47] restore complete
```

**新 Mac 模拟（全新容器）：**
```
$ docker run -d --name karios-migrate-test \
    -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin123 \
    -e POSTGRES_DB=karios-desktop -p 5433:5432 postgres:16-alpine
$ tar -xzf karios-migrate-test2.tar.gz
$ cd karios-migrate-20260804-200438
$ KARIOS_PG_CONTAINER=karios-migrate-test \
    bash ./karios_restore.sh karios-20260804-200438.dump --drop-existing
[20:03:47] pg_restore -> karios-desktop @ karios-migrate-test
[20:04:07] pg_restore ok
[20:04:07] no alembic.ini here — skipping schema upgrade (caller must run it)
[20:04:08] manifest cross-check ok — 44 public tables (expected 44)
[20:04:08] restore complete
```

抽样验证：00700.HK 2026-08-04 close 487.6 完整；daily 表 10.9M 行（与源库一致，差 2715 行是源库在同时被 cron 写入）。

**launchd 加载验证：**
```
$ plutil -lint ~/Library/LaunchAgents/com.karios.db-backup.plist
: OK
$ launchctl print gui/501/com.karios.db-backup
type = LaunchAgent
program = .../db_backup.sh
$ bash install-db-backup-launchd.sh --status
LaunchAgent: LOADED
```

RunAtLoad=true 触发后 stdout 显示：
```
[2026-08-04 19:59:05] skip — last backup is 293s old (...), threshold 90000s
```
说明：DATABASE_URL 正确通过 plist env 传入 + last-age 检查正常工作。

## 设计稿 + 决策真值

`docs/designs/db-backup-and-migrate-2026-08.md`：

- **§0 用户约束**：明确 launchd 在 sleep/wake 场景的能力边界
- **§1 设计**：三脚本分工表 + 备份策略表（格式/保留/异地/校验/容器识别/跳过）
- **§3 风险与缓解**：6 类已知风险（iCloud 禁用 / 容器改名 / dump 中断 / iCloud 满 / .env 删 / 大写入）
- **§4 不做（YAGNI）**：加密 / WAL-PITR / S3 / ZFS snapshot / .dmg / watchdog
- **§5 验证**：本归档对应实测
- **§6 衔接**：与 todo §12 #18 / §13 / designs/db-direction / designs/karios-longevity / designs/mac-mini-deployment 的关系

## 后续影响 / 留给谁

- **OPT-053 "备份 3 副本策略"** 现在实际落地为 **2 副本**（本地 + iCloud）；第 3 副本留 §13 #1 Neon 副本解冻时再实现。
- **OPT-056 Docker 一键起** 解决"换电脑 2 小时起 stack"；**OPT-061** 解决"新 Mac 数据也能在 5 分钟内恢复"。两者一起覆盖 §13 "换电脑也能跑"全部。
- `designs/mac-mini-deployment.md §5 实施时序` 应该在 Mac mini 拿到当天跑 `install-db-backup-launchd.sh`。
- launchd plist 里的 DATABASE_URL 是当前 `.env` 的快照；如果 `.env` 改了 `POSTGRES_PASSWORD`，**重跑 install** 同步到 plist（这是 design §3 已知风险表的"缓解"项）。

## 不做的事（反模式对应）

- 不写自动化测试（脚本 round-trip 验证已替代）
- 不打 .dmg / .pkg（tarball + README 已够）
- 不写 watchdog（cron + RunAtLoad + zshenv 三 trigger 已覆盖）
- 不加密 dump（iCloud 已 E2E 加密）
- 不做 WAL archiving / PITR（用户要"长期生命力"非"任意秒级回滚"）