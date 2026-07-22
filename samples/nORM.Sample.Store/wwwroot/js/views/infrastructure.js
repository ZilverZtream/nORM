import { el, mount, loading } from "../ui.js";
import { api } from "../api.js";
import { state, ENGINE_LABEL } from "../state.js";
import { adminSubnav, pageHead } from "./admin-common.js";

export async function renderInfrastructure() {
  const view = el("div", { class: "page" }, adminSubnav("infra"), loading("Reading engine configuration"));
  mount(view);
  try {
    const data = await api.providers();
    view.replaceChildren(
      adminSubnav("infra"),
      pageHead("Infrastructure", "Database engine",
        "Point an engine at a connection string, test it, then make it live. nORM re-homes the running app — migrating your data across, provider to provider — and every screen re-tints to whatever you're now on."),
      el("div", { class: "engines" }, data.providers.map(engineCard)),
      el("div", { class: "swap-log", id: "swapLog" }),
    );
  } catch (e) {
    view.replaceChildren(adminSubnav("infra"), el("div", { class: "empty" }, "Couldn't load: " + e.message));
  }
}

function engineCard(p) {
  const kind = p.provider;
  const isSqlite = kind === "sqlite";
  // Placeholder (not value): the stored connection string is redacted, so echoing it back would
  // overwrite the real password. Blank input = keep the configured connection; type to replace it.
  const csInput = isSqlite ? null : el("input", { class: "mono", spellcheck: "false", placeholder: p.redactedConnectionString || "Server=…;Database=…;User Id=…" });
  const result = el("span", { class: "test-result mono" });

  const testBtn = el("button", { class: "btn btn-sm" }, "Test");
  testBtn.addEventListener("click", async () => {
    testBtn.disabled = true; result.textContent = "…"; result.className = "test-result mono";
    try {
      const r = await api.testProvider(kind, csInput ? csInput.value : null);
      result.textContent = r.ok ? `✓ ${r.serverVersion}` : `✕ ${r.error}`;
      result.className = "test-result mono " + (r.ok ? "pill-ok" : "pill-danger");
    } catch (e) { result.textContent = "✕ " + e.message; result.className = "test-result mono pill-danger"; }
    finally { testBtn.disabled = false; }
  });

  const activateBtn = el("button", { class: "btn btn-primary btn-sm" }, p.active ? "Live" : "Make live");
  activateBtn.disabled = p.active;
  activateBtn.addEventListener("click", () => activate(kind, csInput, activateBtn));

  return el("article", { class: "engine-card" + (p.active ? " is-active" : ""), dataset: { engine: kind } },
    el("div", { class: "engine-card-head" },
      el("span", { class: "engine-badge" }, el("span", { class: "engine-swatch" }), ENGINE_LABEL[kind] ?? kind),
      p.active ? el("span", { class: "chip accent" }, "live now")
        : p.configured ? el("span", { class: "chip" }, "configured") : el("span", { class: "chip" }, "needs connection")),
    isSqlite
      ? el("p", { class: "muted mono engine-cs" }, "bundled local file — always available")
      : el("label", {}, "Connection string", csInput),
    el("div", { class: "engine-actions" }, testBtn, result, el("div", { class: "spacer" }), activateBtn),
  );
}

async function activate(kind, csInput, btn) {
  const log = document.getElementById("swapLog");
  const from = state.activeEngine;
  const line = el("div", { class: "log-line running" },
    el("span", { class: "spinner" }),
    `Switching ${ENGINE_LABEL[from]} → ${ENGINE_LABEL[kind]} — migrating your data…`);
  log.prepend(line);
  btn.disabled = true;
  try {
    const r = await api.activate(kind, csInput ? csInput.value : null);
    if (!r.ready) throw new Error(r.error || "activation failed");
    line.className = "log-line ok";
    line.replaceChildren(`✓ ${ENGINE_LABEL[from]} → ${ENGINE_LABEL[kind]} — moved ${r.rowsMoved} rows · ${r.productCount} products live on ${ENGINE_LABEL[kind]}`);
    dispatchEvent(new CustomEvent("engine-changed", { detail: kind })); // app shell re-tints
    renderInfrastructure(); // reflect the new active engine
  } catch (e) {
    line.className = "log-line err";
    line.replaceChildren(`✕ couldn't switch to ${ENGINE_LABEL[kind]}: ${e.message}`);
    btn.disabled = false;
  }
}
