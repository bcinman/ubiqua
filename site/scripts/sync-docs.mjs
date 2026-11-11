// Builds the site's pages from the repository's own docs, so docs/ stays the one place to edit them.
//
// - docs/*.md become src/content/docs/*.md: the H1 becomes the title, README.md becomes the home
//   page, and the README's numbered "Guides" list orders the sidebar.
// - Links written for GitHub become site links: `views.md#change-a-view` is `/views/#change-a-view`,
//   and `../examples/bank` points at the file on GitHub.
// - The crates' rustdoc (`cargo doc`) is copied to public/rustdoc.
//
// Both outputs are generated, and ignored by git. astro.config.ts runs this on every dev start and
// build, and again when a file in docs/ changes. `node scripts/sync-docs.mjs` runs it alone.

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const site = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
export const repo = path.resolve(site, "..");
export const docsDir = path.join(repo, "docs");
const pagesDir = path.join(site, "src/content/docs");
const rustdocDir = path.join(site, "public/rustdoc");
const github = "https://github.com/bcinman/ubiqua";

/** The page a docs file becomes: README.md is the home page. */
const slugOf = (file) => (file === "README.md" ? "index" : file.replace(/\.md$/, ""));
const urlOf = (file) => (file === "README.md" ? "/" : `/${slugOf(file)}/`);

/** Writes every page from docs/, and removes pages whose file is gone. */
export function syncDocs() {
  const files = fs.readdirSync(docsDir).filter((f) => f.endsWith(".md"));
  const readme = fs.readFileSync(path.join(docsDir, "README.md"), "utf8");
  // "1. [Getting started](getting-started.md): ..." lines, in order.
  const guides = [...readme.matchAll(/^\d+\.\s+\[[^\]]*\]\(([^)#]+\.md)\)/gm)].map((m) => m[1]);

  fs.mkdirSync(pagesDir, { recursive: true });
  const written = new Set();
  for (const file of files) {
    const source = fs.readFileSync(path.join(docsDir, file), "utf8");
    const order = file === "README.md" ? 0 : guides.indexOf(file) + 1 || guides.length + 1;
    const page = toPage(file, source, order);
    const out = path.join(pagesDir, `${slugOf(file)}.md`);
    // Unchanged pages keep their mtime, so the dev server reloads only what changed.
    if (!fs.existsSync(out) || fs.readFileSync(out, "utf8") !== page) fs.writeFileSync(out, page);
    written.add(path.basename(out));
  }
  for (const stale of fs.readdirSync(pagesDir)) {
    if (!written.has(stale)) fs.rmSync(path.join(pagesDir, stale));
  }
  return files.length;
}

function toPage(file, source, order) {
  const lines = source.split("\n");
  let title = slugOf(file);
  let fence = null;
  const body = [];
  for (const line of lines) {
    const fenceMatch = /^\s*(`{3,}|~{3,})(\S*)/.exec(line);
    if (fenceMatch) {
      if (fence === null) {
        fence = fenceMatch[1];
        // ```diagram is plain text drawn with box characters.
        body.push(fenceMatch[2] === "diagram" ? line.replace("diagram", "text") : line);
      } else {
        if (fenceMatch[1].startsWith(fence[0]) && fenceMatch[1].length >= fence.length) fence = null;
        body.push(line);
      }
      continue;
    }
    if (fence !== null) {
      body.push(line);
      continue;
    }
    // The first H1 is the page title, which the layout already shows.
    const h1 = /^#\s+(.+?)\s*$/.exec(line);
    if (h1 && title === slugOf(file)) {
      title = h1[1];
      continue;
    }
    body.push(line.replace(/\]\(([^)\s]+)\)/g, (_, target) => `](${link(target)})`));
  }
  while (body[0] === "") body.shift();

  const frontmatter = ["---", `title: ${JSON.stringify(title)}`, "sidebar:"];
  if (file === "README.md") frontmatter.push("  label: Overview");
  frontmatter.push(`  order: ${order}`);
  const updated = lastCommitDate(`docs/${file}`);
  if (updated) frontmatter.push(`lastUpdated: ${updated}`);
  frontmatter.push("---", "");
  return `${frontmatter.join("\n")}\n${body.join("\n")}`;
}

/** A link as GitHub reads it in docs/, as the site serves it. */
function link(target) {
  if (/^[a-z][a-z0-9+.-]*:/i.test(target) || target.startsWith("#") || target.startsWith("/")) return target;
  const [file, hash = ""] = target.split(/(?=#)/);
  if (!file.includes("/") && file.endsWith(".md")) return urlOf(file) + hash;
  // Anything else in the repository: its page on GitHub.
  const inRepo = path.relative(repo, path.resolve(docsDir, file));
  if (inRepo.startsWith("..")) return target;
  const kind = fs.existsSync(path.join(repo, inRepo)) && fs.statSync(path.join(repo, inRepo)).isDirectory() ? "tree" : "blob";
  return `${github}/${kind}/main/${inRepo.split(path.sep).join("/")}${hash}`;
}

/** When the file was last committed, or nothing for an uncommitted file or no git. */
function lastCommitDate(file) {
  try {
    return execFileSync("git", ["log", "-1", "--format=%aI", "--", file], { cwd: repo, encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

/** The workspace's crates, as rustdoc names their folders. */
export function crates() {
  return fs
    .readdirSync(path.join(repo, "crates"))
    .filter((name) => fs.existsSync(path.join(repo, "crates", name, "Cargo.toml")))
    .sort((a, b) => (a === "domain" ? -1 : b === "domain" ? 1 : a.localeCompare(b)))
    .map((name) => ({ name, dir: name.replaceAll("-", "_") }));
}

/** Runs `cargo doc` for the workspace's crates and copies the result to public/rustdoc. */
export function syncRustdoc() {
  try {
    execFileSync("cargo", ["doc", "--workspace", "--no-deps", "--locked", "--quiet"], { cwd: repo, stdio: "inherit" });
  } catch (error) {
    console.warn(`[sync-docs] cargo doc failed, so the API reference is missing: ${error.message}`);
    return false;
  }
  fs.rmSync(rustdocDir, { recursive: true, force: true });
  fs.cpSync(path.join(repo, "target/doc"), rustdocDir, { recursive: true });
  return true;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  console.log(`[sync-docs] ${syncDocs()} pages from docs/`);
  if (!process.argv.includes("--no-rustdoc") && syncRustdoc()) console.log("[sync-docs] rustdoc in public/rustdoc");
}
