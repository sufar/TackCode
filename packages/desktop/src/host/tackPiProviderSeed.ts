// TackCode: seed the model-provider registry with providers that pi-rs can
// already serve (pi-rs login / env credentials / local runtimes). Seeded
// entries are normal personal providers with a placeholder apiKey marker —
// the actual call is made by pi-rs with its own credential store, the
// placeholder only satisfies this side's "configured" availability checks.
import { spawnSync } from "node:child_process";
import { IProviderSettingsService } from "@zcode/services";

interface TackPiProviderSeedLogger {
  info(...args: unknown[]): void;
  warn(...args: unknown[]): void;
}

interface PiCatalogProvider {
  id: string;
  name: string;
  hasAuth: boolean;
  models: { id: string; name: string }[];
}

/** pi-rs provider id -> ZCode builtin template id (seed uses template defaults). */
const PI_TO_ZCODE_TEMPLATE: Readonly<Record<string, string>> = {
  deepseek: "deepseek",
  anthropic: "anthropic",
  openai: "openai",
  moonshotai: "moonshot-kimi",
  minimax: "minimax",
  xai: "xai",
  openrouter: "openrouter",
  "qwen-token-plan-cn": "qwen-alibaba-model-studio-cn",
  "qwen-token-plan": "qwen-alibaba-model-studio-intl",
  xiaomi: "xiaomi-mimo",
  "opencode-go": "opencode-go-chat",
  opencode: "opencode-zen-chat",
};

const MAX_MODELS_PER_PROVIDER = 48;

function parsePiModelsOutput(text: string): PiCatalogProvider[] {
  const providers: PiCatalogProvider[] = [];
  let current: PiCatalogProvider | null = null;
  for (const line of text.split("\n")) {
    if (line.trim().length === 0) continue;
    const modelMatch = /^ {4}(\S+)\/(\S+)\t(.*)$/.exec(line);
    if (modelMatch && current) {
      current.models.push({ id: modelMatch[2]!, name: modelMatch[3]!.trim() || modelMatch[2]! });
      continue;
    }
    const providerMatch = /^(✓| ) (\S+) — (.+)$/.exec(line);
    if (providerMatch) {
      current = {
        id: providerMatch[2]!,
        name: providerMatch[3]!.trim(),
        hasAuth: providerMatch[1] === "✓",
        models: [],
      };
      providers.push(current);
    }
  }
  return providers;
}

function listPiProvidersWithAuth(logger: TackPiProviderSeedLogger): PiCatalogProvider[] {
  const binary = process.env.TACK_AGENT_PI_BINARY?.trim() || "pi-rs";
  try {
    const result = spawnSync(binary, ["models"], {
      encoding: "utf8",
      timeout: 60_000,
      maxBuffer: 32 * 1024 * 1024,
    });
    if (result.status !== 0) {
      logger.warn(
        `[tack-seed] pi-rs models failed: ${String(result.stderr || result.stdout).slice(0, 200)}`,
      );
      return [];
    }
    return parsePiModelsOutput(result.stdout).filter((provider) => provider.hasAuth);
  } catch (error) {
    logger.warn(`[tack-seed] pi-rs models unavailable: ${(error as Error).message}`);
    return [];
  }
}

/**
 * Idempotent: providers already present (any source, id match) are skipped;
 * models missing from an existing templated provider are NOT backfilled —
 * user edits always win.
 */
export async function seedTackPiProviders(
  services: { getOptional<T>(token: unknown): T | undefined },
  logger: TackPiProviderSeedLogger,
): Promise<void> {
  const settings = services.getOptional<IProviderSettingsService>(IProviderSettingsService);
  if (!settings) return;
  const readyProviders = listPiProvidersWithAuth(logger);
  if (readyProviders.length === 0) return;
  const view = await settings.getView();
  const existing = new Set(view.providers.map((provider) => provider.providerId));
  let seeded = 0;
  for (const pi of readyProviders) {
    const templateId = PI_TO_ZCODE_TEMPLATE[pi.id];
    // 无匹配模板的 provider（codebuddy、本地 ollama、自定义 models.json 等）
    // 模板 id 才与 bridge 的 provider 映射对齐，v1 只播种模板对齐的。
    if (!templateId || existing.has(templateId)) continue;
    const modelIds = pi.models.map((model) => model.id).slice(0, MAX_MODELS_PER_PROVIDER);
    if (modelIds.length === 0) continue;
    try {
      const { providerId } = await settings.createPersonalProvider({
        templateId,
        providerName: `pi-rs · ${pi.name}`,
        initialConfig: {
          access: { type: "api-key", apiKey: "pi-rs-managed" },
          personalModelIds: modelIds,
          modelOrder: modelIds,
        },
      });
      existing.add(providerId);
      seeded += 1;
      logger.info(
        `[tack-seed] seeded pi-rs provider ${pi.id} -> ${providerId} (${modelIds.length} models)`,
      );
    } catch (error) {
      logger.warn(`[tack-seed] seed ${pi.id} failed: ${(error as Error).message}`);
    }
  }
  if (seeded > 0) {
    await settings.refresh("tack-pi-seed").catch(() => {});
  }
}
