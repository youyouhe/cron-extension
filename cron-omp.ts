// 会话内定时任务插件：通过 cron_add / cron_list / cron_remove 工具（或让模型代为调用）
// 建立定时任务；到点后任务以用户消息注入会话 —— 空闲时立即开新轮次执行，
// 忙碌时以 followUp 排队，不打断当前工作。
// 持久化：任务随 appendEntry 写进会话文件，重启 / 切分支后自动恢复；
//         恢复时已过期的 nextAt 会在首个 tick 补跑一次（错过的任务跑一次，不堆积）。
// 定时器：必须用 ctx.setInterval（托管）—— 回调抛错只记日志，不会拖垮整个会话，
//         且会话关闭时自动清理。raw setInterval 的未捕获异常会以 uncaughtException
//         终结整个进程，禁止使用。
// 手动查看：/cron；删除本文件即卸载（需重启会话生效）。

// ---- 与扩展运行时的结构化边界：只声明本扩展实际用到的方法 ----
interface CronUi {
  notify?: (text: string, level?: string) => void;
}

interface CronCtx {
  ui?: CronUi;
  isIdle?: () => boolean;
  setInterval?: (fn: () => void, ms: number) => unknown;
  clearTimer?: (handle: unknown) => void;
  sessionManager?: { getBranch?: () => unknown[] };
}

type ZodNode = { optional: () => ZodNode };
interface CronPi {
  zod: {
    string: () => ZodNode;
    number: () => ZodNode;
    object: (shape: Record<string, ZodNode>) => unknown;
  };
  exec?: (command: string, args: string[], opts?: { cwd?: string }) => Promise<{ stdout: string; stderr: string; code: number }>;
  sendUserMessage: (
    content: string,
    options?: { deliverAs?: "followUp" | "steer" | "nextTurn" | "aside" },
  ) => Promise<unknown> | unknown;
  appendEntry: (customType: string, data: unknown) => unknown;
  registerTool: (tool: Record<string, unknown>) => unknown;
  registerCommand?: (
    name: string,
    command: { description: string; handler: (args: unknown, ctx: CronCtx) => Promise<void> },
  ) => unknown;
  on: (event: string, handler: (event: unknown, ctx: CronCtx) => Promise<void>) => unknown;
  logger?: { warn?: (msg: string) => void; error?: (msg: string) => void; info?: (msg: string) => void };
}

// ---- 数据模型 ----
type JobKind = "every" | "daily" | "once";

interface CronJob {
  id: string;
  name: string;
  prompt: string;
  kind: JobKind;
  everySeconds?: number; // kind=every
  at?: string; // kind=daily，"HH:MM" 本机时区
  nextAt: number; // epoch ms
  createdAt: number;
  onBusy?: "queue" | "cancel"; // 会话忙碌时：queue 排队（默认）/ cancel 取消本次触发
  condition?: string; // "__TOKEN__ # <agent_id> # <endpoint>"：到点先 get_messages 判 count>0，空则跳过本次不进 LLM
  tokenFile?: string; // 明文 token 文件路径，读首行
}

const ENTRY_TYPE = "local.cron.jobs.v1";
const TICK_MS = 5_000; // 到点检查粒度；实际触发最多晚一个 tick
const MIN_SECONDS = 5; // 允许的最小间隔；与 tick 粒度一致，再小会被 5s tick 吞掉且只烧 token
const MAX_JOBS = 32;
const MAX_PROMPT = 4_000;
const MAX_NAME = 80;

// "HH:MM" -> 下一次触达时刻（本机时区）；已过今日该点则顺延一天。
export function nextDailyAt(at: string, from: number = Date.now()): number {
  const m = /^(\d{1,2}):(\d{2})$/.exec(at.trim());
  const h = m ? Number(m[1]) : NaN;
  const min = m ? Number(m[2]) : NaN;
  if (!m || !(h >= 0 && h < 24) || !(min >= 0 && min < 60)) {
    throw new Error(`daily_at 需为 "HH:MM"（24 小时制），收到：${at}`);
  }
  const d = new Date(from);
  d.setHours(h, min, 0, 0);
  if (d.getTime() <= from) d.setDate(d.getDate() + 1);
  return d.getTime();
}

// 动态键读取：`in` 收窄对非常量键不生效，此处收口为唯一出口（字段逐一经 asStr/asNum 校验）。
function field(raw: unknown, key: string): unknown {
  return raw && typeof raw === "object" ? (raw as Record<string, unknown>)[key] : undefined;
}

function asStr(v: unknown): string {
  return typeof v === "string" ? v : "";
}

function asNum(v: unknown): number | undefined {
  return typeof v === "number" && Number.isFinite(v) ? v : undefined;
}

function validateSchedule(raw: unknown): { kind: JobKind; everySeconds?: number; at?: string; nextAt: number } {
  const every = asNum(field(raw, "every_seconds"));
  const once = asNum(field(raw, "once_in_seconds"));
  const daily = asStr(field(raw, "daily_at"));
  const given = [every, daily, once].filter((v) => v !== undefined && v !== "");
  if (given.length !== 1) {
    throw new Error("every_seconds / daily_at / once_in_seconds 三选一，必填其一");
  }
  if (every !== undefined) {
    if (every < MIN_SECONDS) throw new Error(`every_seconds 最小 ${MIN_SECONDS} 秒`);
    return { kind: "every", everySeconds: every, nextAt: Date.now() + every * 1000 };
  }
  if (once !== undefined) {
    if (once < MIN_SECONDS) throw new Error(`once_in_seconds 最小 ${MIN_SECONDS} 秒`);
    return { kind: "once", nextAt: Date.now() + once * 1000 };
  }
  // 前两个分支未返回 ⇒ 唯一提供的调度字段是 daily_at
  return { kind: "daily", at: daily, nextAt: nextDailyAt(daily) };
}

function formatJob(j: CronJob): string {
  const schedule =
    j.kind === "every"
      ? `每 ${j.everySeconds}s`
      : j.kind === "daily"
        ? `每天 ${j.at}`
        : "一次性";
  const at = new Date(j.nextAt).toLocaleString();
  return `- [${j.id}] ${j.name}（${schedule}，${j.onBusy === "cancel" ? "忙时取消" : "忙时排队"}，下次 ${at}）：${j.prompt}`;
}

export default function cron(pi: CronPi) {
  // 状态全部放在工厂闭包内：每个会话各自加载扩展实例，互不串扰。
  let jobs = new Map<string, CronJob>();
  let ctxRef: CronCtx | null = null;
  let timer: unknown = null;
  let idSeq = 0;

  function persist(): void {
    pi.appendEntry(ENTRY_TYPE, { version: 1, jobs: [...jobs.values()] });
  }

  function restore(ctx: CronCtx): void {
    const branch = ctx.sessionManager?.getBranch?.() ?? [];
    let latest: { version: number; jobs: CronJob[] } | undefined;
    for (const entry of branch) {
      const e = entry as { type?: string; customType?: string; data?: unknown };
      if (e.type === "custom" && e.customType === ENTRY_TYPE) {
        latest = e.data as { version: number; jobs: CronJob[] };
      }
    }
    jobs = new Map((latest?.jobs ?? []).filter((j) => j && j.id && typeof j.prompt === "string").map((j) => [j.id, j]));
  }

  // MCP 条件门：condition 形如 "__TOKEN__ # <agent_id> # <endpoint>"。
  // 到点先用 curl 走 streamable-http 调 harbor get_messages，判 count>0 才放行注入；
  // count=0 / 解析失败 / 缺依赖 → fail-closed 跳过本次（不进 LLM），只顺延 nextAt。
  // 返回 {go:boolean, detail?:string}；go=false 时本轮回整体跳过。
  async function conditionGate(job: CronJob): Promise<{ go: boolean; detail?: string }> {
    const spec = (job.condition ?? "").trim();
    if (!spec) return { go: true }; // 无 condition：老行为，无条件注入
    if (!pi.exec) return { go: false, detail: "pi.exec 不可用，跳过本次" };
    // condition 模板形如：
    //   __TOKEN__ placeholder + " # " + agent_id + " # " + endpoint
    // 按 " # " 三段切分，极简而明确
    const parts = spec.split(" # ");
    if (parts.length !== 3) {
      return { go: false, detail: `condition 需为 "__TOKEN__ # <agent_id> # <endpoint>"，收到 ${spec}` };
    }
    const [_tokenPh, agentId, endpoint] = parts.map((p) => p.trim());
    let token = "";
    try {
      const tok = await pi.exec("head", ["-n1", job.tokenFile ?? ""]);
      if (tok.code !== 0) return { go: false, detail: `读 token 失败: ${tok.stderr.trim()}` };
      token = tok.stdout.trim();
    } catch (e) {
      return { go: false, detail: `读 token 文件失败: ${String(e)}` };
    }
    if (!token) return { go: false, detail: "token 为空" };

    // 1) 建立 MCP streamable-http 会话拿到 mcp-session-id
    const initRes = await pi.exec("curl", [
      "-s", "-D", "-", "-o", "/dev/null", endpoint,
      "-H", "Content-Type: application/json",
      "-H", "Accept: application/json, text/event-stream",
      "-d", JSON.stringify({
        jsonrpc: "2.0", id: 1, method: "initialize",
        params: { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "omp-cron", version: "1" } },
      }),
    ]);
    const sidMatch = /mcp-session-id:\s*(\S+)/i.exec(initRes.stdout);
    if (!sidMatch) return { go: false, detail: "initialize 未返回 mcp-session-id" };
    const mcpSessionId = sidMatch[1];

    // 2) notifications/initialized（可选，多数 server 不强求）
    await pi.exec("curl", ["-s", "-o", "/dev/null", endpoint,
      "-H", "Content-Type: application/json",
      "-H", "Accept: application/json, text/event-stream",
      "-H", `mcp-session-id: ${mcpSessionId}`,
      "-d", JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }),
    ]);

    // 3) tools/call get_messages：判 count>0 决定是否唤醒
    const call = await pi.exec("curl", ["-s", endpoint,
      "-H", "Content-Type: application/json",
      "-H", "Accept: application/json, text/event-stream",
      "-H", `mcp-session-id: ${mcpSessionId}`,
      "-d", JSON.stringify({
        jsonrpc: "2.0", id: 2, method: "tools/call",
        params: { name: "get_messages", arguments: { agent_id: agentId, token, unread_only: true, limit: 20 } },
      }),
    ]);
    // 从 SSE data: 行取最后一个 data: 的 JSON
    const dataLines = call.stdout.split("\n").filter((l) => l.startsWith("data:"));
    if (dataLines.length === 0) return { go: false, detail: "tools/call 无 data 行" };
    const payload = JSON.parse(dataLines[dataLines.length - 1].slice(5).trim());
    const contentArr = payload?.result?.content as Array<{ text?: string }> | undefined;
    const text = (contentArr ?? []).map((c) => c.text ?? "").join("");
    const inner = JSON.parse(text); // {messages:[...], count:N}
    const count = Number(inner.count);
    pi.logger?.info?.(`cron 任务 ${job.id} condition count=${count}`);
    return { go: count > 0, detail: `count=${count}` };
  }

  // 到点触发：一次性任务移除，周期任务从当前时刻顺延（错过只补跑一次）。
  // 忙碌时按 on_busy 分流：queue=followUp 排队（默认）；cancel=取消本次（一次性任务移除、周期任务照常顺延）。
  async function fireDue(): Promise<void> {
    const now = Date.now();
    for (const job of [...jobs.values()]) {
      if (job.nextAt > now) continue;
      if (job.kind === "once") jobs.delete(job.id);
      else
        job.nextAt =
          job.kind === "daily" ? nextDailyAt(job.at as string, now) : now + (job.everySeconds as number) * 1000;
      persist();
      const busy = !ctxRef?.isIdle?.();
      // 条件门放最前：count<=0 直接静默跳过，不进 LLM，也不受 busy 影响
      const gate = await conditionGate(job);
      if (!gate.go) {
        pi.logger?.warn?.(`cron 任务 ${job.id} 条件未命中，跳过本次（${gate.detail ?? ""}，下次 ${new Date(job.nextAt).toLocaleString()}）`);
        continue;
      }
      if (busy && job.onBusy === "cancel") {
        pi.logger?.warn?.(`cron 任务 ${job.id} 忙时取消本次触发（下次 ${new Date(job.nextAt).toLocaleString()}）`);
        continue;
      }
      const text = `⏰ 定时任务 [${job.name}] 触发，请执行：\n${job.prompt}`;
      try {
        if (!busy) await pi.sendUserMessage(text);
        else await pi.sendUserMessage(text, { deliverAs: "followUp" });
      } catch (e) {
        pi.logger?.error?.(`cron 任务 ${job.id} 注入失败: ${String(e).slice(0, 200)}`);
      }
    }
  }

  const z = pi.zod;
  const textContent = (text: string) => ({ content: [{ type: "text" as const, text }] });

  pi.registerTool({
    name: "cron_add",
    label: "新建定时任务",
    description:
      `在当前会话建立定时任务，到点后把 prompt 作为用户消息注入会话驱动执行。` +
      `every_seconds / daily_at（"HH:MM"，本机时区）/ once_in_seconds 三选一；` +
      `最小间隔 ${MIN_SECONDS} 秒，任务随会话持久化。` +
      `on_busy：会话忙碌时策略，queue=排队等空闲（默认），cancel=取消本次触发。` +
      `condition（可选）："__TOKEN__ # <agent_id> # <endpoint>" 三段，到点先 get_messages 判 count>0 才注入、count=0 静默跳过本次不进 LLM；` +
      `tokenFile（可选）：明文 token 文件路径（读首行），注入 condition 的 __TOKEN__ 占位。`,
    parameters: z.object({
      name: z.string(),
      prompt: z.string(),
      every_seconds: z.number().optional(),
      daily_at: z.string().optional(),
      once_in_seconds: z.number().optional(),
      on_busy: z.string().optional(),
      condition: z.string().optional(),
      tokenFile: z.string().optional(),
    }),
    async execute(_id: string, params: unknown) {
      try {
        if (jobs.size >= MAX_JOBS) return textContent(`定时任务已达上限（${MAX_JOBS}），请先 cron_remove。`);
        const name = asStr(field(params, "name")).trim().slice(0, MAX_NAME);
        const prompt = asStr(field(params, "prompt")).trim().slice(0, MAX_PROMPT);
        if (!name || !prompt) return textContent("name 和 prompt 均必填且不能为空。");
        const onBusy = asStr(field(params, "on_busy"));
        if (onBusy !== "" && onBusy !== "queue" && onBusy !== "cancel") {
          return textContent(`on_busy 仅支持 "queue"（排队）或 "cancel"（取消本次），收到：${onBusy}`);
        }
        const condition = asStr(field(params, "condition")).trim();
        const tokenFile = asStr(field(params, "tokenFile")).trim();
        const sched = validateSchedule(params);
        const job: CronJob = {
          id: `${Date.now().toString(36)}-${(idSeq++).toString(36)}`,
          name,
          prompt,
          kind: sched.kind,
          everySeconds: sched.everySeconds,
          at: sched.at,
          nextAt: sched.nextAt,
          onBusy: onBusy === "" ? undefined : onBusy === "cancel" ? "cancel" : "queue",
          condition: condition || undefined,
          tokenFile: tokenFile || undefined,
          createdAt: Date.now(),
        };
        jobs.set(job.id, job);
        persist();
        return { ...textContent(`已建立定时任务：\n${formatJob(job)}`), details: { job } };
      } catch (e) {
        return textContent(`建立失败：${e instanceof Error ? e.message : String(e)}`);
      }
    },
  });

  pi.registerTool({
    name: "cron_list",
    label: "列出定时任务",
    description: "列出当前会话的全部定时任务（含下次触发时间）。",
    parameters: z.object({}),
    async execute() {
      const list = [...jobs.values()].sort((a, b) => a.nextAt - b.nextAt);
      return textContent(list.length ? list.map(formatJob).join("\n") : "当前没有定时任务。");
    },
  });

  pi.registerTool({
    name: "cron_remove",
    label: "删除定时任务",
    description: "按 id 删除一个定时任务。",
    parameters: z.object({ id: z.string() }),
    async execute(_id: string, params: unknown) {
      const id = asStr(field(params, "id"));
      const gone = jobs.get(id);
      if (!id || !gone) return textContent(`未找到定时任务 ${id}，可用 cron_list 查询。`);
      jobs.delete(id);
      persist();
      return textContent(`已删除：${gone.name}（${id}）`);
    },
  });

  pi.registerCommand?.("cron", {
    description: "查看当前会话的定时任务",
    handler: async (_args: unknown, ctx: CronCtx) => {
      const list = [...jobs.values()].sort((a, b) => a.nextAt - b.nextAt);
      ctx.ui?.notify?.(list.length ? list.map(formatJob).join("\n") : "当前没有定时任务。", "info");
    },
  });

  // 加载时机即注册期；定时器只能在运行期（session_start 之后）启动。
  pi.on("session_start", async (_event: unknown, ctx: CronCtx) => {
    ctxRef = ctx;
    restore(ctx); // 上次运行留下的任务在此复活；过期的 nextAt 首个 tick 补跑一次
    timer = ctx.setInterval?.(() => {
      void fireDue();
    }, TICK_MS);
  });

  // 切分支 / 切树视图后以当前分支为准重建任务表。
  pi.on("session_branch", async (_event: unknown, ctx: CronCtx) => {
    restore(ctx);
  });
  pi.on("session_tree", async (_event: unknown, ctx: CronCtx) => {
    restore(ctx);
  });

  pi.on("session_shutdown", async () => {
    // 托管定时器本会随会话自动清理；这里显式清一次并释放引用。
    if (timer !== null) ctxRef?.clearTimer?.(timer);
    timer = null;
    ctxRef = null;
  });
}
