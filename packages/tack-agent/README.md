# @tackcode/tack-agent

ZCode Protocol ⇄ pi-rs 桥接 agent。让 ZCode 桌面/Web 前端把 pi-rs 当作它的 agent 后端。

## 接入方式

ZCode host 通过 `ZCODE_AGENT_SERVER_COMMAND`（+ `ZCODE_AGENT_SERVER_ARGS_JSON`）把 agent
替换为任意可执行文件；本包就是那个可执行文件。在 TackCode fork 里由
`desktop/src/main/tackAgentDefaults.ts` 自动装配（打包态 = `resources/tack-agent`）。

```bash
ZCODE_AGENT_SERVER_COMMAND=/path/to/node \
ZCODE_AGENT_SERVER_ARGS_JSON='[".../packages/tack-agent/bin/tack-agent.mjs"]' \
ZCODE_AGENT_SERVER_STORAGE_PREPARATION_ENTRY=".../packages/tack-agent/bin/tack-agent.mjs" \
TACK_AGENT_STORAGE_STARTUP=1 \
ZCode
```

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `TACK_AGENT_PI_BINARY` | `pi-rs`（PATH） | 要驱动的 pi-rs 可执行文件 |
| `PI_RS_AGENT_DIR` | `~/.pi-rs/agent` | pi-rs 数据目录（会话/凭据/设置） |
| `TACK_AGENT_STORAGE_STARTUP` | — | `1` = 启动时上报 agent 存储就绪帧（桌面启动门要求） |
| `TACK_AGENT_LOG_FILE` | — | bridge 诊断日志（stdout 是协议通道，勿混用） |

## 协议覆盖（v0.1）

- 订阅：`v4/conversation/subscribe|resync|unsubscribe`，topic =
  `sessions-index/<ws>`、`workspace-config/<ws>`、`conversation/<sessionId>`
- 命令：`v4/command` = createSession / sendText / stop / compact / renameSession /
  deleteSession / switchModelConfig / switchCollaborationMode / setFollowupMode；
  其余返回 ACK failed(`unsupported.command`)
- MCP：createSession.payload.mcpServers 随会话创建注入 pi-rs（`set_mcp_servers`，
  bridge 磁盘留档 tack-mcp-servers.json：冷恢复/bridge 重启重放、fork 换键）；
  `mcp/list` 经共享 probe pi-rs 进程真实连接并回写状态快照（mode=status 只读不连接）
- 帧：`v4/conversation/frame`（snapshot + deltas），wire 封套 complete/fragment（crc32），
  deliveryKind = initial/online/recovery
- 查询：`v4/commands/query`、`v4/conversation/rowsRange`、`v4/conversation/usage`、
  `v4/conversation/plans`、`v4/conversation/workflowRuns(-Events)`（后两者为空实现）
- 旧方法：`workspace/readPresentation`、`provider/updateAccountConfig`（回执 revision）、
  `workspace/updateInteractionPreferences|updateModelIoPreferences|updateOffPeakToolPolicy|
  `updateDynamicWorkflowPolicy`（回显结果）、`session/setModel|setThoughtLevel|setMode`、
  `provider/testModelConnectivity`、`workspace/hooks/trustGrant`
- 存储启动握手：`--prepare-storage` 单发模式（startup/storagePath → storagePathReady →
  storageState → storagePrepared）+ 启动就绪上报
- 反向请求：`interaction/requestProviderRuntimeHeaders`（按需向 host 取 API Key →
  写入 pi-rs `auth.json`）、`interaction/requestPermission`（权限弹窗，声明式 options 回映）
- 权限交互：`resolveInteraction` 命令结算（先到先得），映射到 pi 的
  allow/allowAlways/deny；协作模式 build/edit/plan/yolo → pi `set_mode` 的
  ask/acceptEdits/plan/bypass
- 其余方法：`-32601 method not found`（host 对可选能力会降级）

## pi-rs 侧映射

- 每会话一个 `pi-rs --mode rpc` 子进程；冷恢复 = spawn + `switch_session` + `get_messages`
  重建 rows（新 logEpoch，host 自动 resync）
- MCP：host 下发的 mcpServers（ZCode 协议 DTO）翻译成 mcp.json 条目形（env/headers
  数组转 map；oauth scope 字符串拆 scopes；clientSecret/isolation/timeoutMs 无 pi
  对应字段，丢弃）；pi-rs 侧按会话合并进连接池（同名覆盖文件配置），连接池与
  prompt 共享（fingerprint 失效自动重建，死连接剔除）
- pi 事件 → v4：text/thinking → assistantText/reasoning 行（row.delta 流式）；
  toolcall_* → toolCall 行（inputText 流式）；tool_execution_* → 行状态/输出；
  turn_end → turnHeader 终态；message_update error → control.lastError
- 模型目录：`pi-rs models` 解析出有凭据 provider 的模型，经 workspace-config 发布
- 会话列表：扫描 `<agentDir>/sessions/--<cwd>--/*.jsonl`（标题 = 自定义名或首条用户消息）

## 测试

```bash
node test/smoke.mjs                 # 协议冒烟（无 LLM，含 mcp/list + createSession.mcpServers）
node --test test/mcp.test.mjs       # MCP 翻译层单测
node test/smoke.mjs --prompt "Reply with exactly: PONG" --provider deepseek --model deepseek-chat
node test/cdp.mjs pages|eval|text|click-text|type|key|shot   # 驱动运行中的桌面 UI
```
