#!/usr/bin/env node
// TackCode: strip Zhipu (GLM) commercial entries from the builtin provider
// catalog. Idempotent — safe to re-run after merging upstream releases.
//
// Removes:
//   - the 4 Zhipu provider templates (zai-api, zai-standard-api, bigmodel-api,
//     bigmodel-standard-api) — OAuth/Coding-Plan login surfaces
//   - all account:* providerRules (zai/bigmodel coding plans & idle plans)
//   - builtinProviderModelRules bound to removed account providers
// Keeps:
//   - neutral templates (deepseek, anthropic, openai, kimi, minimax, qwen,
//     xiaomi, xai, openrouter, opencode-*) and generic model rule regexes,
//     including third-party-hosted glm-* models under non-Zhipu templates.
import fs from "node:fs";

const file = process.argv[2] ?? "config/provider/zcode-builtin.json";
const j = JSON.parse(fs.readFileSync(file, "utf8"));

const ZHIPU_TEMPLATES = new Set([
  "zai-api",
  "zai-standard-api",
  "bigmodel-api",
  "bigmodel-standard-api",
]);

const rules = j.config.providerConfigRules;
const before = {
  templates: rules.templateRules.length,
  providers: rules.providerRules.length,
  builtinModels: j.config.modelConfigRules.builtinProviderModelRules.length,
  templateModels: j.config.modelConfigRules.templateModelRules.length,
};

rules.templateRules = rules.templateRules.filter((t) => !ZHIPU_TEMPLATES.has(t.templateId));
rules.providerRules = rules.providerRules.filter(
  (p) => !String(p.providerId).startsWith("account:"),
);
const modelRules = j.config.modelConfigRules;
modelRules.builtinProviderModelRules = modelRules.builtinProviderModelRules.filter(
  (r) => !String(r.providerId).startsWith("account:"),
);
modelRules.templateModelRules = modelRules.templateModelRules.filter(
  (r) => !ZHIPU_TEMPLATES.has(r.templateId),
);

j.revision = (j.revision ?? 0) + 1;
fs.writeFileSync(file, `${JSON.stringify(j, null, 2)}\n`);

console.log("stripped:", {
  templates: `${before.templates} -> ${rules.templateRules.length}`,
  providers: `${before.providers} -> ${rules.providerRules.length}`,
  builtinModels: `${before.builtinModels} -> ${modelRules.builtinProviderModelRules.length}`,
  templateModels: `${before.templateModels} -> ${modelRules.templateModelRules.length}`,
});
