#!/usr/bin/env node
// tack-agent: ZCode host <-> pi-rs bridge entry point.
// The host spawns this executable via ZCODE_AGENT_SERVER_COMMAND; any trailing
// args (app-server --stdio --surface desktop) are accepted and ignored.
import { main } from "../src/main.mjs";

main(process.argv.slice(2)).catch((error) => {
  console.error(`[tack-agent] fatal: ${error?.stack ?? error}`);
  process.exit(1);
});
