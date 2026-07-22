import { el, mount } from "../ui.js";
import { api } from "../api.js";
import { state, ENGINE_LABEL } from "../state.js";
import { adminSubnav, pageHead } from "./admin-common.js";

export function renderSystem() {
  mount(el("div", { class: "page" },
    adminSubnav("system"),
    pageHead("System", "Health & isolation",
      "Diagnostics a multi-tenant, multi-engine app actually needs — each runs against whichever engine is live right now."),
    el("div", { class: "grid-2" },
      checkCard(
        "Tenant isolation under load",
        "Fire 32 parallel requests split across both stores and assert no request ever surfaces the other tenant's rows. A single leaked row fails the check.",
        "Run 32-way proof",
        async () => {
          const r = await api.isolationProof();
          return r.isolationHeld
            ? { ok: true, text: `PASS · ${r.parallelRequests} parallel (${r.tenantARuns} A / ${r.tenantBRuns} B) · 0 cross-tenant bleed · ${r.elapsedMs} ms on ${ENGINE_LABEL[state.activeEngine]}` }
            : { ok: false, text: "FAIL · cross-tenant bleed detected" };
        }),
      checkCard(
        "Cross-tenant write guard",
        "Attempt to update and delete a row owned by the other store. The generated write path must affect zero rows.",
        "Run write guard",
        async () => {
          const r = await api.crossTenantProof();
          return { ok: r.passed, text: r.passed ? "PASS · cross-tenant update + delete each affected 0 rows" : "FAIL · a cross-tenant write got through" };
        }),
    )));
}

function checkCard(title, desc, label, fn) {
  const out = el("div", { class: "check-out" });
  const btn = el("button", { class: "btn btn-primary" }, label);
  btn.addEventListener("click", async () => {
    btn.disabled = true;
    out.replaceChildren(el("div", { class: "log-line running" }, el("span", { class: "spinner" }), "Running…"));
    try {
      const { ok, text } = await fn();
      out.replaceChildren(el("div", { class: "log-line " + (ok ? "ok" : "err") }, text));
    } catch (e) {
      out.replaceChildren(el("div", { class: "log-line err" }, "✕ " + e.message));
    } finally { btn.disabled = false; }
  });
  return el("section", { class: "card" },
    el("div", { class: "card-head" }, el("h2", {}, title)),
    el("p", { class: "muted" }, desc),
    el("div", { class: "check-actions" }, btn),
    out);
}
