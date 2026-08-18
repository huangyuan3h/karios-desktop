# 回测探索池 D3/D6 + 回测文档整理 · 归档于 2026-08-15

## 当时的目标（todo 链接）
- docs/todo.md §8 探索池：D3 环境感知仓位（待做）、D6 利润护城河复核（低优先级）
- 用户要求：继续 D3/D6 实验；保证文档完整（回测成功+失败全记录，失败更重要——说明走过的路）；整理干净的回测文档文件夹

## 实际做了什么
1. **D3 环境感知仓位（2026-08-15 固化 ✅）**
   - 假设：仓位是纯杠杆旋钮 → 按入场日环境缩放
   - 引擎加 `BacktestConfig.env_position_scale`（uptrend/fan → scale 映射），两处入场点生效
   - 7 变体扫描（scripts/tip014_d3_env_position.py）：v4（uptrend 1.25× / fan 0.75×）最优
2. **D6 profit_trail 复核（2026-08-15 排除 ❌）**
   - A6（2026-08-12 防守攻击）曾拒收；在新基线（D2/D3/E2 固化后）重试 6 变体
   - 全部拒收：t10-4 全窗大劣化、唯一 t10-6 OOS2 +5.1 但 train -21.2/valid -8.5 违反铁律
   - 与 V7.0-03 同一根因：盈利后收紧回撤 = 截断右尾利润腿
3. **回测文档文件夹（新建 docs/backtests/）**
   - README.md（索引 + 三窗铁律纪律 + 基线档案 + 当前基线数字）
   - experiments-tip014.md（主链 5 项成功 + E1/HK/电风扇细分 3 条失败路径）
   - experiments-d-pool.md（D1-D8 全记录，D6 今日结果含变体表）
   - experiments-defensive.md（防守向攻击 23 项全拒收/中性 + 方法论遗产）
   - experiments-legacy.md（V6/V7 · OPT-105 · 红绿灯 · 熔断 · 长窗速查）
   - 文档中交叉引用（d-pool ↔ defensive ↔ legacy 的 A6/C1/D2 复核链）
4. 同步更新：todo.md（D6 状态 + backtests 引用）、strategy-params.md（版本历史加 D6 行）、docs/README.md（索引加 backtests）

## 验证 / 数据
- D3：三窗 OOS2 +24.6 / train +19.5 / valid +26.4，长窗 270.1→333.9（+64pt）；基线重固化
- D6：6 变体全拒收（见 tip014_d6_profit_trail.json），维持关闭
- 全量测试 3420 backend / 794 frontend 全绿
- 基线文件：walk_forward_baseline.json（D3 后重固化 · OOS2 117.2 / train 122.6 / valid 142.2）

## 后续影响 / 留给谁
- D8（港股情绪数据）仍在池中，需新数据源——唯一的剩余探索项
- D6 引擎能力保留（profit_trail 默认 0），未来若入场质量大幅变化可重试
- 回测文档文件夹为新增维护点：新实验必须记入 backtests/ 对应文件（成功+失败都要）
