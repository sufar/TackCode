// pi-rs skill discovery (JS mirror of crates/pi-app/src/skills.rs load_skills):
// project <cwd>/.pi/skills and .agents/skills (cwd + ancestors to git root),
// user <agentDir>/skills and ~/.agents/skills. First hit wins on name
// collision; identical real files are deduped.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

function parseFrontmatter(content) {
  const match = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?/.exec(content);
  if (!match) return { frontmatter: {}, body: content };
  const frontmatter = {};
  for (const line of match[1].split(/\r?\n/)) {
    const kv = /^([A-Za-z_][\w-]*)\s*:\s*(.*)$/.exec(line);
    if (kv) frontmatter[kv[1]] = kv[2].trim().replace(/^["']|["']$/g, "");
  }
  return { frontmatter, body: content.slice(match[0].length) };
}

function loadSkillFile(filePath, declared) {
  let raw;
  try {
    raw = fs.readFileSync(filePath, "utf8");
  } catch {
    return null;
  }
  const { frontmatter } = parseFrontmatter(raw);
  const description = typeof frontmatter.description === "string" ? frontmatter.description : "";
  if (!declared && description.trim().length === 0) return null;
  const baseDir = path.dirname(filePath);
  const name =
    (typeof frontmatter.name === "string" && frontmatter.name) || path.basename(baseDir);
  return { name, description, filePath, baseDir };
}

function loadSkillsFromDir(dir, includeRootFiles) {
  const skills = [];
  let entries;
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return skills;
  }
  if (entries.some((e) => e.isFile() && e.name === "SKILL.md")) {
    const skill = loadSkillFile(path.join(dir, "SKILL.md"), true);
    if (skill) skills.push(skill);
    return skills;
  }
  for (const entry of entries) {
    if (entry.name.startsWith(".") || entry.name === "node_modules") continue;
    const entryPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      skills.push(...loadSkillsFromDir(entryPath, false));
    } else if (entry.isFile() && includeRootFiles && entry.name.endsWith(".md")) {
      const skill = loadSkillFile(entryPath, false);
      if (skill) skills.push(skill);
    }
  }
  return skills;
}

function findGitRoot(cwd) {
  let current = cwd;
  for (;;) {
    if (fs.existsSync(path.join(current, ".git"))) return current;
    const parent = path.dirname(current);
    if (parent === current) return null;
    current = parent;
  }
}

/**
 * Discover pi-rs skills for a workspace.
 * Returns [{name, description, path, scope: "workspace" | "user"}].
 */
export function loadSkills({ cwd, agentDir }) {
  const dirs = [];
  const home = os.homedir();
  const gitRoot = findGitRoot(cwd);
  const stopAt = gitRoot ?? path.parse(cwd).root;
  for (let dir = cwd; ; dir = path.dirname(dir)) {
    dirs.push({ dir: path.join(dir, ".agents", "skills"), scope: "workspace" });
    if (dir === stopAt || dir === home) break;
  }
  dirs.unshift({ dir: path.join(cwd, ".pi", "skills"), scope: "workspace" });
  dirs.push({ dir: path.join(agentDir, "skills"), scope: "user" });
  dirs.push({ dir: path.join(home, ".agents", "skills"), scope: "user" });

  const seenFiles = new Set();
  const seenNames = new Set();
  const out = [];
  for (const { dir, scope } of dirs) {
    for (const skill of loadSkillsFromDir(dir, true)) {
      let canonical;
      try {
        canonical = fs.realpathSync(skill.filePath);
      } catch {
        canonical = skill.filePath;
      }
      if (seenFiles.has(canonical)) continue;
      seenFiles.add(canonical);
      if (seenNames.has(skill.name)) continue;
      seenNames.add(skill.name);
      out.push({ name: skill.name, description: skill.description, path: skill.filePath, scope });
    }
  }
  return out;
}
