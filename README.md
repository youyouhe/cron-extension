# cron-extension

Agent 会话内定时任务插件：让 agent 在对话中建立定时任务，到点后自动驱动当前会话执行。

单文件扩展，无构建步骤，无第三方依赖。分别提供 opencode 和 Oh My Pi (omp) 两个版本。

## 仓库结构

```
cron-extension/
├── cron-opencode.ts    # opencode 版本
├── cron-omp.ts         # Oh My Pi (omp) 版本
└── README.md
```

## 功能

- **三个 LLM 工具**：`cron_add`（新建）、`cron_list`（查看）、`cron_remove`（删除）
- **`/cron` 命令**：手动列出当前会话的全部任务
- **三种调度**：固定间隔（`every_seconds`）、每天定点（`daily_at`，"HH:MM"，本机时区）、一次性延时（`once_in_seconds`）
- **到点执行**：任务以用户消息注入会话 -- 会话空闲时立即开新轮次执行；忙碌时按任务的 `on_busy` 策略分流：`queue`（默认）以 `followUp` 排队不打断当前工作，`cancel` 取消本次触发
- **会话持久化**：任务写入会话文件（`appendEntry`），重启 / 切分支后自动恢复；错过的任务补跑一次，不堆积
- **安全约束**：最小间隔 5 秒（与触发粒度一致）、单会话最多 32 个任务、定时器走 `ctx.setInterval` 托管通道（回调抛错只记日志，不会拖垮会话进程）
- **条件门控**：可选 `condition` 参数 `"__TOKEN__ # <agent_id> # <endpoint>"`，到点先 `get_messages` 判 `count>0` 才注入、count=0 静默跳过本次不进 LLM
- **Token 支持**：可选 `tokenFile` 明文 token 文件路径，读首行作为 `condition` 中的 `__TOKEN__` 占位

## 安装

### opencode

```bash
git clone https://github.com/youyouhe/cron-extension.git
cp cron-extension/cron-opencode.ts ~/.config/opencode/cron.ts
```

重启 opencode 会话生效。

### Oh My Pi (omp)

```bash
git clone https://github.com/youyouhe/cron-extension.git
cd cron-extension

# 全局安装：所有 omp 会话加载
ln -s "$(pwd)/cron-omp.ts" ~/.omp/agent/extensions/cron.ts

# 或项目级安装
mkdir -p /path/to/your/project/.omp/extensions
cp cron-omp.ts /path/to/your/project/.omp/extensions/
```

重启 omp 会话生效。

加载成功后输入 `/cron` 应显示「当前没有定时任务。」；删除文件或软链即卸载。

## 使用

直接用自然语言对话，模型会调用工具建立任务：

- 「每 5 分钟检查一次 `git status`，有未提交变更就提醒我」
- 「每天 09:30 总结一下昨天的提交」
- 「10 分钟后提醒我重新部署」

### 工具参数（cron_add）

| 参数 | 类型 | 说明 |
|---|---|---|
| `name` | string | 任务名（<=80 字符） |
| `prompt` | string | 到点后注入会话的指令（<=4000 字符） |
| `every_seconds` | number | 固定间隔秒数，>=5 |
| `daily_at` | string | 每天定点，`"HH:MM"` 24 小时制，本机时区 |
| `once_in_seconds` | number | 一次性延时秒数，>=5 |
| `on_busy` | string | 会话忙碌时策略：`queue` 排队（默认）/ `cancel` 取消本次 |
| `condition` | string | 可选，`"__TOKEN__ # <agent_id> # <endpoint>"` 三段，到点先 get_messages 判 count>0 才注入 |
| `tokenFile` | string | 可选，明文 token 文件路径（读首行），注入 condition 的 `__TOKEN__` 占位 |

三个调度字段**必填其一，且只能填一个**。

### 语义细节

- 触发粒度 5 秒：实际触发最多晚一个 tick
- 周期任务到期后从当前时刻顺延；daily 任务错过则下次定点触发
- 忙碌时 `cancel`：本次不执行、记 warn 日志 -- 一次性任务即被移除，周期任务照常顺延到下一期；空闲时该类任务正常直发
- 任务的生命周期跟随会话：新会话从零开始，恢复的会话带回原任务
- 条件门控：`count<=0` 时跳过本次触发，不进 LLM，也不受 busy 状态影响
- tokenFile：用于 condition 表达式中的 `__TOKEN__` 占位，读取文件首行内容