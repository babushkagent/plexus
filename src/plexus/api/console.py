"""Operator console: one dependency-free HTML page over the public REST API.

The console is a *client* of the same routes any SDK calls. It has no privileged path,
embeds no data, sends no telemetry, and stores no credential on the server: everything
on screen was fetched with the operator's own key through the ordinary auth, RBAC and
tenant-scoping pipeline. One way to reach the control plane means one way to audit it.

Two properties are load-bearing and tested:
1. The page is safe to serve publicly because it is inert -- a shell plus a manifest of
   routes. No tenant data, no secret, no config value crosses the wire unauthenticated.
2. `connect-src 'self'` plus DOM-only rendering (never innerHTML with data) means a
   hostile API response cannot exfiltrate the credential held in this page.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..errors import NotFound
from .messages import Request, Response

if TYPE_CHECKING:  # pragma: no cover - imported for typing only, avoids a cycle
    from .server import App

CONSOLE_PATHS = ("/console", "/console/")
CHAT_PATH = "/v1/chat/completions"

CSP = (
    "default-src 'none'; "
    "script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
    "connect-src 'self'; img-src 'self' data:; "
    "base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)


@dataclass(frozen=True, slots=True)
class Call:
    """One API call the console can make, with a body template where one is needed."""

    label: str
    method: str
    path: str
    body: dict[str, Any] | None = None
    note: str = ""
    live: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "method": self.method,
            "path": self.path,
            "body": self.body,
            "note": self.note,
            "live": self.live,
        }


@dataclass(frozen=True, slots=True)
class View:
    """A console tab. `kind=chat` renders the streaming playground instead of calls."""

    id: str
    label: str
    calls: tuple[Call, ...] = ()
    kind: str = "calls"
    hint: str = ""
    platform: bool = False
    stream_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "kind": self.kind,
            "hint": self.hint,
            "platform": self.platform,
            "stream_path": self.stream_path,
            "calls": [call.to_dict() for call in self.calls],
        }


VIEWS: tuple[View, ...] = (
    View(
        id="overview",
        label="Overview",
        hint="Who you are, what you have spent, and whether the plane is healthy.",
        calls=(
            Call("Tenant", "GET", "/v1/tenant", live=True),
            Call("Usage", "GET", "/v1/usage?days=30", live=True),
            Call("Liveness", "GET", "/healthz", live=True),
            Call("Readiness", "GET", "/readyz", live=True),
            Call(
                "Rename / set plan",
                "PATCH",
                "/v1/tenant",
                body={"name": "acme", "settings": {"contact": "ops@acme.dev"}},
                note="Plan and budget changes are platform-scoped.",
            ),
        ),
    ),
    View(
        id="playground",
        label="Playground",
        kind="chat",
        hint="Streams /v1/chat/completions as server-sent events through the gateway.",
        stream_path=CHAT_PATH,
        calls=(Call("Allowed models", "GET", "/v1/models"),),
    ),
    View(
        id="models",
        label="Models",
        hint="Registry, lineage and gated promotion. A model cannot deploy unsigned.",
        calls=(
            Call("List versions", "GET", "/v1/models?limit=50", live=True),
            Call("Get version", "GET", "/v1/models/{version_id}"),
            Call("Lineage", "GET", "/v1/models/{version_id}/lineage"),
            Call(
                "Register",
                "POST",
                "/v1/models",
                body={
                    "name": "sentiment",
                    "version": "1.0.0",
                    "digest": "sha256:0000000000000000",
                    "kind": "weights",
                    "size_bytes": 1024,
                    "metadata": {"train_auc": 0.91},
                },
            ),
            Call(
                "Record eval",
                "POST",
                "/v1/models/{version_id}/eval",
                body={"passed": True, "metrics": {"accuracy": 0.93, "cost_usd": 0.12}},
            ),
            Call("Sign", "POST", "/v1/models/{version_id}/sign", body={"signature": "sig-ed25519-..."}),
            Call("Promote", "POST", "/v1/models/{version_id}/promote", body={"stage": "staging"}),
            Call("Artifacts", "GET", "/v1/artifacts"),
            Call(
                "Attach artifact",
                "POST",
                "/v1/artifacts",
                body={"digest": "sha256:0000000000000000", "kind": "dataset", "uri": "s3://bucket/d.csv"},
            ),
        ),
    ),
    View(
        id="deployments",
        label="Deployments",
        hint="Traffic is a percentage split across versions; rollback is one call.",
        calls=(
            Call("List", "GET", "/v1/deployments", live=True),
            Call("Get", "GET", "/v1/deployments/{deployment_id}"),
            Call(
                "Create / update",
                "POST",
                "/v1/deployments",
                body={"name": "sentiment", "model_version_id": "mv_...", "min_replicas": 1, "max_replicas": 5},
            ),
            Call("Scale", "POST", "/v1/deployments/{deployment_id}/scale", body={"replicas": 3}),
            Call("Shift traffic", "POST", "/v1/deployments/{deployment_id}/traffic", body={"traffic_percent": 25}),
            Call("Rollback", "POST", "/v1/deployments/{deployment_id}/rollback", body={"model_version_id": "mv_..."}),
        ),
    ),
    View(
        id="runs",
        label="Runs",
        calls=(
            Call("List", "GET", "/v1/runs?limit=50", live=True),
            Call("Get", "GET", "/v1/runs/{run_id}"),
            Call("Start", "POST", "/v1/runs", body={"kind": "batch.inference", "metrics": {"items": 1000}}),
            Call("Finish", "POST", "/v1/runs/{run_id}/finish", body={"status": "succeeded", "metrics": {"f1": 0.88}}),
        ),
    ),
    View(
        id="queue",
        label="Queue",
        hint="Durable tasks with leases, retries and a dead-letter queue you can drain.",
        calls=(
            Call("Enqueue", "POST", "/v1/tasks", body={"type": "index.build", "payload": {"shard": 1}, "delay_s": 0}),
            Call("Get task", "GET", "/v1/tasks/{task_id}"),
            Call("Cancel", "POST", "/v1/tasks/{task_id}/cancel"),
            Call("Dead letters", "GET", "/v1/dead-letters?limit=50", live=True),
            Call("Requeue", "POST", "/v1/dead-letters/{task_id}/requeue"),
        ),
    ),
    View(
        id="access",
        label="Access",
        hint="Keys are stored as digests: the secret is shown exactly once, here.",
        calls=(
            Call("List keys", "GET", "/v1/keys"),
            Call(
                "Create key",
                "POST",
                "/v1/keys",
                body={"roles": ["ml_engineer"], "label": "ci", "plan": "standard"},
                note="Copy the secret from the response now; it is not retrievable.",
            ),
            Call("Revoke key", "DELETE", "/v1/keys/{key_id}"),
            Call("Mint token", "POST", "/v1/tokens", body={"roles": ["viewer"], "ttl_s": 900}),
            Call("Members", "GET", "/v1/members"),
            Call("Grant member", "PUT", "/v1/members/{subject}", body={"role": "viewer"}),
            Call("Revoke member", "DELETE", "/v1/members/{subject}", body={"role": "viewer"}),
        ),
    ),
    View(
        id="audit",
        label="Audit & usage",
        calls=(
            Call("Audit log", "GET", "/v1/audit?limit=50", live=True),
            Call("Usage (30d)", "GET", "/v1/usage?days=30", live=True),
        ),
    ),
    View(
        id="platform",
        label="Platform",
        platform=True,
        hint="Requires a platform-scoped token (plexus token --platform). Tenant keys are refused.",
        calls=(
            Call("Health", "GET", "/v1/platform/health", live=True),
            Call("Queue depth", "GET", "/v1/platform/queue", live=True),
            Call("Autoscaler decision", "GET", "/v1/platform/scaling", live=True),
            Call("Tenants", "GET", "/v1/platform/tenants", live=True),
            Call("Create tenant", "POST", "/v1/platform/tenants", body={"name": "acme", "plan": "standard"}),
            Call("Get tenant", "GET", "/v1/platform/tenants/{tenant_id}"),
            Call(
                "Update tenant",
                "PATCH",
                "/v1/platform/tenants/{tenant_id}",
                body={"plan": "enterprise", "status": "active", "rps_limit": 100, "monthly_budget_usd": 5000},
            ),
            Call("Tenant keys", "GET", "/v1/platform/tenants/{tenant_id}/keys"),
            Call("Issue tenant key", "POST", "/v1/platform/tenants/{tenant_id}/keys", body={"roles": ["owner"], "label": "ops"}),
            Call("Revoke tenant key", "DELETE", "/v1/platform/tenants/{tenant_id}/keys/{key_id}"),
            Call("Tenant members", "GET", "/v1/platform/tenants/{tenant_id}/members"),
            Call("Grant tenant member", "PUT", "/v1/platform/tenants/{tenant_id}/members/{subject}", body={"role": "admin"}),
            Call("Revoke tenant member", "DELETE", "/v1/platform/tenants/{tenant_id}/members/{subject}", body={"role": "admin"}),
        ),
    ),
)


def manifest(app: App) -> dict[str, Any]:
    """What the console can do -- derived from code, never from a hand-written copy."""
    from .. import __version__

    return {
        "service": app.settings.service_name,
        "version": __version__,
        "default_model": app.settings.default_model,
        "views": [view.to_dict() for view in VIEWS],
    }


def render_page(app: App) -> str:
    payload = json.dumps(manifest(app), separators=(",", ":")).replace("<", "\\u003c")
    return (
        _PAGE.replace("/*STYLE*/", _STYLE)
        .replace("/*SCRIPT*/", _SCRIPT)
        .replace('"__MANIFEST__"', payload)
        .replace("/*SERVICE*/", _escape(app.settings.service_name))
        .replace("/*VERSION*/", _escape(manifest(app)["version"]))
    )


def _escape(value: str) -> str:
    return (value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))[:64]


def console_index(app: App, request: Request) -> Response:
    """Serve the console shell. Public by design: it carries no data and no secret."""
    if not app.settings.console_enabled:
        raise NotFound("console is disabled", details={"setting": "PLEXUS_CONSOLE_ENABLED"})
    return Response(
        status=200,
        raw=render_page(app),
        content_type="text/html; charset=utf-8",
        headers={
            "Content-Security-Policy": CSP,
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "Referrer-Policy": "no-referrer",
            "Cache-Control": "no-store",
        },
    )


_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="referrer" content="no-referrer">
<link rel="icon" href="data:,">
<title>Plexus Console</title>
<style>/*STYLE*/</style>
</head>
<body>
<header>
  <div class="brand">Plexus<span> /*SERVICE*/ /*VERSION*/</span></div>
  <input id="base" placeholder="API origin" spellcheck="false">
  <input id="key" type="password" placeholder="x-api-key or Bearer JWT" spellcheck="false" autocomplete="off">
  <button id="save" title="Keep this credential in sessionStorage for this tab only">Save</button>
  <button id="forget">Forget</button>
  <label class="switch"><input type="checkbox" id="live"> live 5s</label>
  <div id="who" class="hint"></div>
</header>
<main>
  <nav id="tabs"></nav>
  <section id="panel"></section>
</main>
<footer><div id="log" class="log"></div></footer>
<script id="manifest" type="application/json">"__MANIFEST__"</script>
<script>/*SCRIPT*/</script>
</body>
</html>
"""

_STYLE = """
:root{--bg:#0a0e13;--panel:#111823;--panel2:#0d141d;--line:#1e2937;--fg:#e6edf3;
--dim:#8b98a9;--accent:#58a6ff;--ok:#3fb950;--warn:#d29922;--err:#f85149;
--mono:ui-monospace,SFMono-Regular,Menlo,monospace}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.55 system-ui,-apple-system,"Segoe UI",sans-serif}
header{position:sticky;top:0;z-index:3;display:flex;flex-wrap:wrap;gap:8px;align-items:center;
padding:10px 16px;background:#0d141d;border-bottom:1px solid var(--line)}
.brand{font-weight:700;letter-spacing:.4px}
.brand span{color:var(--dim);font-weight:400;font-family:var(--mono);font-size:12px}
input,textarea,button{background:var(--panel);color:var(--fg);border:1px solid var(--line);
border-radius:6px;padding:7px 9px;font:inherit}
input:focus,textarea:focus,button:focus{outline:1px solid var(--accent)}
#base{width:250px}#key{width:300px}
button{cursor:pointer;background:#1b2735}
button:hover{border-color:var(--accent)}
.switch{color:var(--dim);font-size:12px;display:flex;gap:5px;align-items:center}
.hint{color:var(--dim);font-size:12px;margin-left:auto;font-family:var(--mono)}
main{display:grid;grid-template-columns:190px 1fr;min-height:calc(100vh - 110px)}
nav{border-right:1px solid var(--line);padding:12px 8px;background:#0c1219}
nav button{display:block;width:100%;text-align:left;border:0;background:transparent;
padding:7px 10px;border-radius:6px;color:var(--dim);margin-bottom:2px}
nav button.on{background:#1b2735;color:var(--fg)}
nav .tag{float:right;color:#6b4f1d;background:#3a2f16;border-radius:4px;font-size:10px;padding:1px 4px}
section{padding:16px;overflow:auto}
.viewhint{color:var(--dim);font-size:12.5px;margin:0 0 14px}
.card{border:1px solid var(--line);border-radius:8px;background:var(--panel2);margin-bottom:12px;overflow:hidden}
.card>header{position:static;display:flex;gap:8px;align-items:center;padding:9px 12px;
background:transparent;border-bottom:1px solid var(--line);flex-wrap:wrap}
.verb{font:600 11px/1 var(--mono);padding:4px 6px;border-radius:4px;background:#1f2f45;color:var(--accent)}
.verb.POST{background:#173423;color:var(--ok)}
.verb.DELETE{background:#3a1d20;color:var(--err)}
.verb.PUT,.verb.PATCH{background:#3a2f16;color:var(--warn)}
.lbl{font-weight:600}
code.path{font-family:var(--mono);font-size:12px;color:var(--dim)}
.note{color:var(--dim);font-size:11.5px;width:100%}
.body{padding:10px 12px;display:flex;flex-direction:column;gap:8px}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.row input{font-family:var(--mono);font-size:12px;min-width:150px}
textarea{width:100%;font-family:var(--mono);font-size:12px;min-height:96px}
table{border-collapse:collapse;width:100%;font-size:12.5px}
th,td{border-bottom:1px solid var(--line);padding:6px 8px;text-align:left;vertical-align:top;font-family:var(--mono)}
th{color:var(--dim);font-weight:600;background:#0f1621;position:sticky;top:0}
pre{margin:0;padding:10px;background:#0b1119;border-radius:6px;overflow:auto;
font-family:var(--mono);font-size:12px;max-height:420px;white-space:pre-wrap}
.status{font-family:var(--mono);font-size:12px;margin-left:auto}
.ok{color:var(--ok)}.warn{color:var(--warn)}.err{color:var(--err)}
.log{font-family:var(--mono);font-size:11.5px;color:var(--dim);padding:8px 16px;
border-top:1px solid var(--line);max-height:150px;overflow:auto;white-space:pre-wrap}
.chatbox{border:1px solid var(--line);border-radius:8px;background:var(--panel2);padding:12px;margin-bottom:12px}
.msg{white-space:pre-wrap;padding:8px 10px;border-radius:8px;margin-bottom:8px;background:#131c27}
.msg.user{background:#1b2a3d}
footer{border-top:1px solid var(--line);background:#0c1219}
@media(max-width:900px){main{grid-template-columns:1fr}nav{display:flex;overflow:auto;border-right:0}}
"""

_SCRIPT = r"""
"use strict";
const M = JSON.parse(document.getElementById("manifest").textContent);
const store = window.sessionStorage;
const state = {
  base: (store.getItem("px.base") || location.origin).replace(/\/+$/, ""),
  key: store.getItem("px.key") || "",
  params: JSON.parse(store.getItem("px.params") || "{}"),
  live: false, timer: null, view: null
};
const $ = (id) => document.getElementById(id);
const el = (tag, cls, text) => {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
};
const isObject = (v) => v !== null && typeof v === "object" && !Array.isArray(v);

function authHeaders() {
  const h = { accept: "application/json" };
  const key = state.key.trim();
  if (!key) return h;
  if (key.split(".").length === 3) h.authorization = "Bearer " + key;
  else h["x-api-key"] = key;
  return h;
}

function fill(path, params) {
  return path.replace(/\{(\w+)\}/g, (_m, name) => encodeURIComponent(params[name] || ""));
}

async function send(call, params, bodyText) {
  const path = fill(call.path, params);
  const init = { method: call.method, headers: authHeaders() };
  if (call.body !== null && typeof call.body === "object") {
    init.headers["content-type"] = "application/json";
    init.body = bodyText;
  }
  const t0 = performance.now();
  let res;
  try {
    res = await fetch(state.base + path, init);
  } catch (err) {
    return { status: 0, ms: Math.round(performance.now() - t0), method: call.method,
             path: path, text: String(err), payload: null };
  }
  const text = await res.text();
  let payload = null;
  try { payload = text ? JSON.parse(text) : null; } catch (_e) { payload = null; }
  return { status: res.status, ms: Math.round(performance.now() - t0), method: call.method,
           path: path, text: text, payload: payload,
           requestId: res.headers.get("x-request-id") };
}

function cls(status) { return status >= 500 || status === 0 ? "err" : status >= 400 ? "warn" : "ok"; }

function note(meta, message) {
  const line = new Date().toISOString().slice(11, 19) + "  " + meta.method + " " + meta.path +
    "  " + meta.status + "  " + meta.ms + "ms" + (meta.requestId ? "  " + meta.requestId : "") +
    (message ? "  " + message : "");
  const log = $("log");
  log.textContent = line + "\n" + log.textContent.split("\n").slice(0, 200).join("\n");
}

function cell(v) {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return v;
}

function rowsOf(payload) {
  if (Array.isArray(payload)) return payload.filter(isObject);
  if (isObject(payload) && Array.isArray(payload.items)) return payload.items.filter(isObject);
  return null;
}

function tableOf(rows) {
  const keys = [];
  rows.slice(0, 25).forEach((row) => Object.keys(row).forEach((k) => {
    if (keys.length < 9 && !keys.includes(k)) keys.push(k);
  }));
  const table = el("table");
  const head = document.createElement("tr");
  keys.forEach((k) => head.appendChild(el("th", null, k)));
  table.appendChild(head);
  rows.slice(0, 200).forEach((row) => {
    const tr = document.createElement("tr");
    keys.forEach((k) => tr.appendChild(el("td", null, cell(row[k]))));
    table.appendChild(tr);
  });
  return table;
}

function render(meta, target) {
  target.textContent = "";
  const bar = el("div", "row");
  bar.appendChild(el("span", "status " + cls(meta.status), meta.status + (meta.requestId ? "  " + meta.requestId : "  " + meta.ms + "ms")));
  if (meta.payload !== null || meta.text) bar.appendChild(copyButton(curlOf(meta)));
  target.appendChild(bar);
  const rows = meta.payload === null ? null : rowsOf(meta.payload);
  if (rows && rows.length) target.appendChild(tableOf(rows));
  else if (meta.text) target.appendChild(el("pre", null, meta.payload !== null ? JSON.stringify(meta.payload, null, 2) : meta.text));
  else target.appendChild(el("pre", null, "(empty body)"));
}

function copyButton(text) {
  const button = el("button", null, "curl");
  button.onclick = () => {
    if (navigator.clipboard) navigator.clipboard.writeText(text);
    button.textContent = "copied";
    setTimeout(() => { button.textContent = "curl"; }, 1200);
  };
  return button;
}

function curlOf(meta) {
  const key = state.key.trim() || "<api-key>";
  const auth = key.split(".").length === 3 ? '-H "authorization: Bearer ' + key + '"' : '-H "x-api-key: ' + key + '"';
  const parts = ["curl -sS -X " + meta.method + ' "' + state.base + meta.path + '"', auth];
  if (meta.bodyText) {
    const compact = String(meta.bodyText).replace(/\s+/g, " ").replace(/'/g, "");
    parts.push("-H 'content-type: application/json' -d '" + compact + "'");
  }
  return parts.join(" ");
}

function paramsOf(call) {
  const found = [];
  call.path.replace(/\{(\w+)\}/g, (_m, name) => { found.push(name); return ""; });
  return found;
}

function callCard(call) {
  const card = el("div", "card");
  const head = document.createElement("header");
  head.appendChild(el("span", "verb " + call.method, call.method));
  head.appendChild(el("span", "lbl", call.label));
  head.appendChild(el("code", "path", call.path));
  if (call.note) head.appendChild(el("div", "note", call.note));
  card.appendChild(head);

  const body = el("div", "body");
  const row = el("div", "row");
  const inputs = {};
  paramsOf(call).forEach((name) => {
    const input = el("input");
    input.placeholder = name;
    input.value = state.params[name] || "";
    input.onchange = () => { state.params[name] = input.value; store.setItem("px.params", JSON.stringify(state.params)); };
    row.appendChild(input);
    inputs[name] = input;
  });
  const run = el("button", null, "Run");
  row.appendChild(run);
  const out = el("div", "body");
  let editor = null;
  if (call.body !== null && typeof call.body === "object") {
    editor = el("textarea");
    editor.value = JSON.stringify(call.body, null, 2);
    body.appendChild(editor);
  }
  body.appendChild(row);
  card.appendChild(body);
  card.appendChild(out);
  run.onclick = async () => {
    const params = {};
    Object.keys(inputs).forEach((name) => { params[name] = inputs[name].value.trim(); });
    run.textContent = "...";
    const meta = await send(call, params, editor ? editor.value : "");
    run.textContent = "Run";
    note(meta);
    render(meta, out);
    if (call.path === "/v1/tenant" && meta.status === 200 && meta.payload) {
      $("who").textContent = (meta.payload.name || "") + "  " + (meta.payload.id || "");
    }
  };
  return card;
}

function renderChat(view) {
  const wrap = el("div");
  const box = el("div", "chatbox");
  const transcript = el("div");
  const input = el("textarea");
  input.placeholder = "Ask the gateway. Model routing, retries, budget and metering happen server side.";
  input.style.minHeight = "70px";
  const row = el("div", "row");
  const model = el("input");
  model.placeholder = "model";
  model.value = M.default_model || "";
  model.style.flex = "0 0 180px";
  const temp = el("input");
  temp.placeholder = "temperature";
  temp.value = "0";
  temp.style.flex = "0 0 110px";
  const streaming = el("input");
  streaming.type = "checkbox";
  streaming.checked = true;
  const sendButton = el("button", null, "Send");
  row.appendChild(model);
  row.appendChild(temp);
  row.appendChild(el("span", "hint", "stream"));
  row.appendChild(streaming);
  row.appendChild(sendButton);
  box.appendChild(transcript);
  box.appendChild(input);
  box.appendChild(row);
  wrap.appendChild(box);
  view.calls.forEach((call) => wrap.appendChild(callCard(call)));

  sendButton.onclick = async () => {
    const prompt = input.value.trim();
    if (!prompt) return;
    transcript.appendChild(el("div", "msg user", prompt));
    input.value = "";
    const answer = el("div", "msg assistant", "");
    transcript.appendChild(answer);
    const call = { label: "chat", method: "POST", path: view.stream_path, body: {} };
    const payload = {
      model: model.value.trim() || undefined,
      temperature: Number(temp.value || 0),
      stream: !!streaming.checked,
      messages: [{ role: "user", content: prompt }]
    };
    if (payload.model === undefined) delete payload.model;
    const bodyText = JSON.stringify(payload);
    sendButton.textContent = "...";
    try {
      const res = await fetch(state.base + call.path, {
        method: "POST",
        headers: Object.assign({ "content-type": "application/json" }, authHeaders()),
        body: bodyText
      });
      if (!streaming.checked || !res.body) {
        const data = await res.json();
        answer.textContent = (data.choices && data.choices[0] && data.choices[0].message
          ? data.choices[0].message.content : JSON.stringify(data, null, 2));
        note({ method: "POST", path: call.path, status: res.status, ms: 0, requestId: res.headers.get("x-request-id") });
      } else {
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        for (;;) {
          const chunk = await reader.read();
          if (chunk.done) break;
          buffer += decoder.decode(chunk.value, { stream: true });
          const parts = buffer.split("\n\n");
          buffer = parts.pop() || "";
          parts.forEach((frame) => {
            const line = frame.split("\n").find((l) => l.indexOf("data:") === 0);
            if (!line) return;
            const data = line.slice(5).trim();
            if (data === "[DONE]") return;
            try {
              const event = JSON.parse(data);
              const delta = event.choices && event.choices[0] && event.choices[0].delta;
              if (delta && delta.content) answer.textContent += delta.content;
            } catch (_e) { /* keep streaming on a partial frame */ }
          });
        }
        note({ method: "POST", path: call.path, status: res.status, ms: 0, requestId: res.headers.get("x-request-id") });
      }
    } catch (err) {
      answer.textContent = "request failed: " + err;
    }
    sendButton.textContent = "Send";
    transcript.scrollTop = transcript.scrollHeight;
  };
  return wrap;
}

function selectView(view) {
  state.view = view;
  const panel = $("panel");
  panel.textContent = "";
  Array.prototype.forEach.call($("tabs").children, (node) => node.classList.remove("on"));
  const tab = $("tab-" + view.id);
  if (tab) tab.classList.add("on");
  if (view.hint) panel.appendChild(el("p", "viewhint", view.hint));
  if (view.kind === "chat") {
    panel.appendChild(renderChat(view));
    return;
  }
  view.calls.forEach((call) => panel.appendChild(callCard(call)));
  refresh();
}

async function refresh() {
  const view = state.view;
  if (!view || !state.key.trim()) return;
  const cards = $("panel").querySelectorAll(".card");
  let index = 0;
  for (const call of view.calls) {
    const card = cards[index++];
    if (!card || call.body !== null) continue;
    const out = card.lastElementChild;
    const meta = await send(call, state.params, "");
    note(meta);
    render(meta, out);
  }
}

function tick() {
  if (state.timer) clearInterval(state.timer);
  state.timer = state.live ? setInterval(() => { if (!document.hidden) refresh(); }, 5000) : null;
}

function boot() {
  $("base").value = state.base;
  $("key").value = state.key;
  $("save").onclick = () => {
    state.base = $("base").value.trim().replace(/\/+$/, "");
    state.key = $("key").value;
    store.setItem("px.base", state.base);
    store.setItem("px.key", state.key);
    $("who").textContent = state.key ? "credential held in this tab" : "no credential";
    refresh();
  };
  $("forget").onclick = () => {
    store.removeItem("px.key");
    $("key").value = "";
    state.key = "";
    $("who").textContent = "credential forgotten";
  };
  $("live").onchange = () => { state.live = $("live").checked; tick(); };
  const tabs = $("tabs");
  M.views.forEach((view) => {
    const button = el("button", null);
    button.id = "tab-" + view.id;
    button.appendChild(document.createTextNode(view.label));
    if (view.platform) button.appendChild(el("span", "tag", "platform"));
    button.onclick = () => selectView(view);
    tabs.appendChild(button);
  });
  $("who").textContent = state.key ? "credential held in this tab" : "paste an api key to begin";
  selectView(M.views[0]);
}

boot();
"""
