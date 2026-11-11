import { defineConfig } from "astro/config";
import tailwindcss from "@tailwindcss/vite";
import nimbus, {
  defineConfig as defineNimbusConfig,
} from "@cloudflare/nimbus-docs";
import { tableScroll } from "@cloudflare/nimbus-docs/markdown";
import type { AstroIntegration } from "astro";
import fs from "node:fs";
import { crates, docsDir, syncDocs, syncRustdoc } from "./scripts/sync-docs.mjs";

// The pages come from the repository's docs/; see scripts/sync-docs.mjs.
const repoDocs: AstroIntegration = {
  name: "repo-docs",
  hooks: {
    "astro:config:setup": ({ command, logger }) => {
      logger.info(`${syncDocs()} pages from docs/`);
      if (command === "dev" || command === "build") syncRustdoc();
      if (command === "dev") {
        // A config change restarts the server and runs this again; keep one watcher.
        const shared = globalThis as { repoDocsWatcher?: fs.FSWatcher };
        shared.repoDocsWatcher?.close();
        shared.repoDocsWatcher = fs.watch(docsDir, () => {
          try {
            syncDocs();
          } catch (error) {
            logger.warn(`docs/ not synced: ${error}`);
          }
        });
      }
    },
  },
};

const nimbusConfig = defineNimbusConfig({
  // CHANGE_ME: the production origin, once the site is deployed (no trailing slash). Drives
  // canonical URLs, absolute OG image URLs, robots.txt, the sitemap and the links in /llms.txt.
  site: "https://example.com",
  title: "domain",
  description: "Business logic as commands, aggregates and events, in Rust. A port of Alchemy Domain.",
  locale: "en",
  github: "https://github.com/bcinman/ubiqua",
  socialImageAlt: "domain documentation preview",
  // The guides, in the order docs/README.md lists them, then each crate's rustdoc.
  sidebar: {
    items: [
      "index",
      { autogenerate: { collection: "docs" } },
      {
        label: "API reference",
        items: crates().map(({ name, dir }) => ({ label: name, link: `/rustdoc/${dir}/index.html` })),
      },
    ],
  },
});

export default defineConfig({
  // The dev toolbar covers the bottom of the page in the preview.
  devToolbar: { enabled: false },
  // nimbus:adapter
  output: "static",
  // Tailwind v4 via its Vite plugin (the integration Astro recommends for
  // Tailwind v4 — replaces the PostCSS plugin, which doesn't build under
  // Astro 7's Vite 8 bundler).
  vite: {
    plugins: [tailwindcss()],
    // Thousands of generated rustdoc files; rewriting them shouldn't reload the dev server.
    server: { watch: { ignored: ["**/public/rustdoc/**"] } },
  },
  // Hover-prefetch link targets so full-page navigations feel instant without
  // a client-side router.
  prefetch: {
    prefetchAll: true,
    defaultStrategy: "hover",
  },
  integrations: [
    repoDocs,
    nimbus(nimbusConfig, {
      // Authoring rules are opt-in by design — your repo, your taste. The
      // two below are the load-bearing pair: frontmatter has to validate
      // against the content schema for the page to render properly, and
      // broken internal links are 404s for your readers. Add the others
      // (heading hierarchy, code-block language, style, etc.) when you're
      // ready to enforce them — see `nimbus-docs lint --help`.
      rules: {
        "nimbus/frontmatter-shape": "error",
        "nimbus/internal-link": "error",
      },
      markdown: {
        // Wrap wide tables so they scroll instead of overflowing the page
        // (styled by `.nb-table-scroll` in src/styles/prose.css).
        hastPlugins: [tableScroll()],
        // `## Install {#install}` gives a heading a fixed id. A heading that
        // ends in other `{…}` text, such as `{account_id}` in a .md file, is
        // read as attributes too; remove this to keep it as text.
        features: { headingAttributes: true },
      },
    }),
  ],
});
