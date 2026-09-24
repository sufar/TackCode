# pi-rs-code

**pi-rs 的桌面壳** —— 基于 [ZCode](https://github.com/zai-org/ZCode)（Apache-2.0）前端改造的
pi-rs 桌面应用。Zhipu/GLM 的商业化内容（登录、套餐、广告、分享、反馈、官方市场、自动更新）
已全部移除或中性化；Agent 后端由 ZCode 自家的 zcode-cli 换成了 **pi-rs**。

```
┌────────────────────────────── pi-rs-code (Electron) ───────────────────────────┐
│  Web/UI (React)                                                                │
│      │  @zcode/rpc (MessagePort)                                               │
│  zcode-host (utility process: services, provider registry, tasks-index sqlite) │
│      │  ZCode Protocol v4 (LF-JSONL stdio)                                     │
│  ┌───┴─────────────────────┐          ┌──────────────────┐                     │
│  │ packages/pi-agent (bridge)│ ──────▶  │ pi-rs --mode rpc │  (per session)     │
│  │  ZCode 协议子集 ⇄ pi rpc  │  JSONL   │  (用户自己的 pi-rs │                    │
│  └──────────────────────────┘          │   安装/凭据/会话)  │                   │
└─────────────────────────────────────────┴──────────────────┴───────────────────┘
```

## 工作原理

ZCode 桌面端通过一个 **agent 子进程**（stdio 上的 LF-JSONL "ZCode Protocol"）驱动 Agent，
并且官方支持用环境变量 `ZCODE_AGENT_SERVER_COMMAND` 指向任意二进制。
`packages/pi-agent` 就是这个边界上的替换实现：它对桌面端讲 ZCode Protocol v4 的一个子集
（sessions-index / workspace-config / conversation 三个 topic 的订阅、`v4/command`、
wire 帧、存储启动握手），对内为每个会话 spawn 一个 `pi-rs --mode rpc` 进程，把 pi-rs 的
流式事件翻译成 v4 的 rows/delta 投影。

因为这个接缝是协议级的，**上游 ZCode 的前端、服务、设置页全部原样保留**，pi-rs 免费获得：
会话列表/恢复、流式渲染（文本/思考/工具调用/终端）、中断、模型与思考级别选择、
文件树/编辑器/终端/Git 面板等完整工作区 UI。

## 已移除/中性化的 Zhipu 商业面

| 项 | 处置 | 位置 |
| --- | --- | --- |
| 官方服务端点（OAuth/套餐/分享/反馈/更新/远程目录/网关） | 默认指向 `127.0.0.1:9`（快速失败），env 仍可覆盖 | `packages/shared/src/zcodeEndpoint.ts` |
| 智谱 provider 模板 + Coding Plan 账号 + GLM 内置模型 | 从内置目录剔除（可重跑 `scripts/strip-zhipu-providers.mjs`） | `config/provider/zcode-builtin.json` |
| 远程内置目录下载 | 永不下载 | `packages/services/src/model-provider/zcodeBuiltinRemoteConfig.ts` |
| Z.ai/BigModel OAuth 登录 | 默认不注册（`PICODE_ENABLE_ZHIPU_OAUTH=1` 可恢复） | `packages/services/src/oauth/runtimeConfig.ts` |
| 官方插件市场 CDN | 清空默认市场 | `packages/shared/src/plugin-marketplaces.ts` |
| 桌面自动更新 / 强制更新 | 恒禁用（pi-rs 自更新走 `pi-rs update`） | `packages/desktop/src/main/autoUpdater.ts` |
| 帮助菜单社区/反馈工单入口；文档链接 | 移除；改指 pi-rs 仓库 | `packages/ui/src/WorkspaceHelpMenuButton.tsx` |
| Coding Plan 营销卡片/额度 widget/升级弹窗 | 数据源自 Coding Plan 账号，随账号剔除自然失效 | （设置页/输入区/侧栏） |
| 遥测/ARMS | 上游默认即关（无端点），未改动 | — |
| 品牌（名称/图标/i18n/启动页/关于） | pi-rs-code + pi-rs 钳子 logo | desktop identity / locales / icons |

`apps/zcode-cli`（Zhipu 自家 agent，含 GLM 网关改道代码）**不参与构建与分发**，
仅作为上游源码保留在仓库里；bridge 取代了它的位置。

## 开发

```bash
# 一次性：工具链（Node 24.14 + pnpm 10.33.2，corepack 自动取用）
.toolchains/node-v24.14.0-darwin-arm64/bin/node --version
corepack pnpm install

# 启动桌面 dev（需要 pi-rs 可执行文件；PI_AGENT_PI_BINARY 可显式指定）
PI_AGENT_PI_BINARY=/path/to/pi-rs scripts/dev-picode.sh
```

`dev-picode.sh` 会设置隔离的数据目录（`PICODE_HOME`，默认 `/tmp/picode-home`）、
Electron userData、`PI_RS_AGENT_DIR`（pi-rs 的会话/凭据目录），并通过
`piAgentDefaults` 让 host 自动使用 `packages/pi-agent` 作为 agent。

常用调试入口：

- `PI_AGENT_LOG_FILE`（默认 `$PICODE_HOME/pi-agent.log`）：bridge 日志
- `PI_AGENT_TRACE=1`：bridge 协议收发（预留）
- `PICODE_DEBUG_HOST=1`：host 启动检查点日志
- `packages/pi-agent/test/smoke.mjs`：不依赖桌面的协议冒烟
  `node test/smoke.mjs --prompt "Reply with exactly: PONG" --provider deepseek --model deepseek-chat`
- `packages/pi-agent/test/cdp.mjs`：CDP 驱动运行中的桌面 UI（pages/eval/text/click-text/type/key/shot）

## 模型与凭据

两条路，可混用：

1. **pi-rs 自有凭据**（推荐）：`pi-rs login --provider X` 或环境变量
   （`ANTHROPIC_API_KEY` 等，见 `pi-rs providers`）。bridge 启动时解析
   `pi-rs models`，把有凭据的 provider 的模型发布到聊天模型选择器。
2. **在 UI 里添加供应商**：设置 → 模型设置 → 添加供应商（deepseek/anthropic/openai/…
   模板已保留）。发送时 bridge 会通过 host 的 `interaction/requestProviderRuntimeHeaders`
   取回该 provider 的 API Key，写入 `~/.pi-rs/agent/auth.json`（与 `pi-rs login` 同一文件）
   供 pi-rs 使用。

模型 id 对齐注意：UI 侧模型 id 会原样传给 pi-rs 的 `set_model`。pi-rs 对内置 provider 的
未知模型 id 会合成 bare_model（用 provider 默认参数）兜底，所以 ZCode 模板自带的模型
（如 `deepseek-flash`）也能跑；想要精确的 contextWindow/maxTokens/思考档位，在设置页把
模型 id 改成 `pi-rs models` 里的值即可。ZCode 与 pi-rs 的 provider id 不一致时 bridge 内置
别名表（moonshot-kimi→moonshotai、qwen 百炼→qwen-token-plan(-cn)、xiaomi-mimo→xiaomi、
opencode-go/zen→opencode-go/opencode）。

## 当前限制（bridge v0.1）

- 权限确认交互未接通：pi-rs rpc 模式恒为 bypass（工具直接执行），
  ZCode 的权限弹窗不会出现。后续可走 `pi-rs serve` 的 permission 通道。
- 图片/附件上传未实现（`v4/attachment/*` 返回 method-not-found）。
- 工作流/动态工作流、Off-Peak 闲时、子代理下钻、文件回退预览：返回空或 method-not-found。
- 排队语义简化：会话 busy 时发送按 pi-rs 的 steer 处理（注入当前轮），不是 ZCode 的队列模型。
- 每会话一个 pi-rs 进程；暂无空闲回收（进程数 = 打开过的会话数，受 LRU 待实现约束）。
- Web/远程工作区形态未验证（bridge 只在 desktop local 形态测试过）。

## 与上游同步（升级策略）

上游 `zai-org/ZCode` 以**整包 squash 提交**发布（`feat: update vX.Y.Z`），因此升级方式是
**rebase 我们的补丁序列到新基线**，而不是 merge：

```bash
git fetch upstream
git switch pi-rs-code
git rebase --onto upstream/main <旧基线> pi-rs-code
# 解决冲突（我们的补丁都集中在少量文件；见下）
corepack pnpm install
node scripts/strip-zhipu-providers.mjs   # 上游新增/改动目录条目后重跑
corepack pnpm typecheck && corepack pnpm lint
node packages/pi-agent/test/smoke.mjs    # 协议冒烟（会验证 wire 帧与 seq 不变量）
```

补丁面（按冲突概率排序，全部带 `pi-rs-code` 注释锚点便于 grep）：

1. `packages/pi-agent/**` — 全新目录，零冲突。
2. `scripts/dev-picode.sh`、`scripts/strip-zhipu-providers.mjs` — 新增，零冲突。
3. `packages/shared/src/zcodeEndpoint.ts`、`plugin-marketplaces.ts` — 常量改动，小冲突面。
4. `packages/services/src/zcode-agent/zcodeAgentProcessManager.ts`（env 覆盖扩展）、
   `oauth/runtimeConfig.ts`、`model-provider/zcodeBuiltinRemoteConfig.ts` — 单点小改。
5. `packages/desktop/src/main/{piAgentDefaults.ts,index.ts}`、`autoUpdater.ts`、
   `scripts/dev.mjs`、`electron-builder.config.js` — 单点小改。
6. `packages/ui` 的 help 菜单/模板选择器 + 两个 locale（大批量 ZCode→pi-rs-code 文案）—
   i18n 冲突最多，建议冲突时取上游版本后重跑文案替换（sed）。
7. `config/provider/zcode-builtin.json` — 必然冲突，**直接取上游版本再跑
   `scripts/strip-zhipu-providers.mjs`**。

协议层预警：若上游改动 `packages/shared/src/zcode-protocol*/**`（v4 方法/rows/wire），
bridge 需要跟进——`test/smoke.mjs` 校验了关键不变量（ACK 形状、snapshot/delta 水位、
row 模型、wire 帧），先跑它再看 UI。

## 合规

- 上游 ZCode 为 Apache-2.0（见 `LICENSE`），本仓库保留其 `LICENSE`/`NOTICE.md`/
  `THIRD-PARTY-NOTICES.md`；修改遵循 Apache-2.0 第 4(b) 条以 `pi-rs-code` 注释与
  提交历史标注。"ZCode"、智谱、GLM 商标属于原厂商，此处仅作来源说明。
- pi-rs 侧同样为 Apache-2.0。
