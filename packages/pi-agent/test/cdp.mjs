// Minimal CDP driver for the running ZCode Dev instance (port 9229).
// Usage: node test/cdp.mjs <command> [args]
//   pages                          list renderer targets
//   eval <expression>              evaluate JS in the first page
//   text                           dump document.body.innerText (first 3000 chars)
//   click-text <text>              click the first visible element containing text
//   shot <file>                    capture a PNG screenshot
import fs from "node:fs";

const command = process.argv[2];
const arg = process.argv.slice(3).join(" ");
const DEBUG_PORT = process.env.CDP_PORT ?? 9229;

async function getTargets() {
  const response = await fetch(`http://127.0.0.1:${DEBUG_PORT}/json`);
  return response.json();
}

function connect(url) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    const pending = new Map();
    let nextId = 1;
    ws.onmessage = (event) => {
      const message = JSON.parse(event.data);
      if (message.id && pending.has(message.id)) {
        const { resolve: res, reject: rej } = pending.get(message.id);
        pending.delete(message.id);
        if (message.error) rej(new Error(message.error.message));
        else res(message.result);
      }
    };
    ws.onopen = () =>
      resolve({
        send(method, params = {}) {
          const id = nextId++;
          ws.send(JSON.stringify({ id, method, params }));
          return new Promise((res, rej) => pending.set(id, { resolve: res, reject: rej }));
        },
        close: () => ws.close(),
      });
    ws.onerror = (error) => reject(error);
  });
}

async function evaluate(client, expression) {
  const result = await client.send("Runtime.evaluate", {
    expression,
    returnByValue: true,
    awaitPromise: true,
  });
  if (result.exceptionDetails) {
    throw new Error(
      result.exceptionDetails.exception?.description ??
        result.exceptionDetails.text ??
        "evaluation failed",
    );
  }
  return result.result?.value;
}

const targets = await getTargets();
const pages = targets.filter((t) => t.type === "page");

if (command === "pages") {
  for (const t of targets) console.log(`${t.type}\t${t.url}\t${t.title}`);
  process.exit(0);
}

const page = pages.find((p) => !p.url.startsWith("devtools://")) ?? pages[0];
if (!page) {
  console.error("no renderer page found");
  process.exit(1);
}
const client = await connect(page.webSocketDebuggerUrl);

if (command === "eval") {
  console.log(JSON.stringify(await evaluate(client, arg), null, 2)?.slice(0, 5000));
} else if (command === "text") {
  const text = await evaluate(client, "document.body?.innerText ?? ''");
  console.log(String(text).slice(0, 3000));
} else if (command === "click-text") {
  const found = await evaluate(
    client,
    `(() => {
      const needle = ${JSON.stringify(arg)};
      const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
      const candidates = [];
      while (walker.nextNode()) {
        const el = walker.currentNode;
        if (el.children.length > 4) continue;
        const text = (el.innerText ?? "").trim();
        if (!text.includes(needle)) continue;
        const rect = el.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) continue;
        candidates.push({ el, area: rect.width * rect.height, text: text.slice(0, 40) });
      }
      candidates.sort((a, b) => a.area - b.area);
      const hit = candidates[0];
      if (!hit) return null;
      hit.el.scrollIntoView({ block: "center" });
      const rect = hit.el.getBoundingClientRect();
      const x = rect.left + rect.width / 2;
      const y = rect.top + rect.height / 2;
      const opts = { bubbles: true, cancelable: true, composed: true, clientX: x, clientY: y, button: 0, buttons: 1, pointerId: 1, pointerType: "mouse", isPrimary: true };
      hit.el.dispatchEvent(new PointerEvent("pointerdown", opts));
      hit.el.dispatchEvent(new MouseEvent("mousedown", opts));
      hit.el.dispatchEvent(new PointerEvent("pointerup", { ...opts, buttons: 0 }));
      hit.el.dispatchEvent(new MouseEvent("mouseup", { ...opts, buttons: 0 }));
      hit.el.dispatchEvent(new MouseEvent("click", { ...opts, buttons: 0 }));
      return hit.text;
    })()`,
  );
  console.log(found === null ? "NOT FOUND" : `clicked: ${found}`);
} else if (command === "shot") {
  const shot = await client.send("Page.captureScreenshot", { format: "png" });
  fs.writeFileSync(arg, Buffer.from(shot.data, "base64"));
  console.log(`saved ${arg}`);
} else if (command === "type") {
  // type into the focused element via insertText
  await client.send("Input.insertText", { text: arg });
  console.log("typed");
} else if (command === "key") {
  const keyMap = { enter: { key: "Enter", code: "Enter", keyCode: 13 }, escape: { key: "Escape", code: "Escape", keyCode: 27 } };
  const k = keyMap[arg] ?? keyMap.enter;
  await client.send("Input.dispatchKeyEvent", { type: "keyDown", key: k.key, code: k.code, windowsVirtualKeyCode: k.keyCode });
  await client.send("Input.dispatchKeyEvent", { type: "keyUp", key: k.key, code: k.code, windowsVirtualKeyCode: k.keyCode });
  console.log(`key: ${arg}`);
} else {
  console.error("unknown command");
  process.exit(1);
}
client.close();
process.exit(0);
