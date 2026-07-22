import { el, money, mount, loading, toast } from "../ui.js";
import { api } from "../api.js";
import { adminSubnav, pageHead } from "./admin-common.js";

let productId = null;
let lastTag = null;

export async function renderHistory() {
  const view = el("div", { class: "page" }, adminSubnav("history"), loading("Loading"));
  mount(view);
  try {
    const cat = await api.catalog(1, 50);
    if (!productId || !cat.items.some((p) => p.id === productId)) productId = cat.items[0]?.id ?? null;
    render(view, cat.items);
    await refreshTimeline();
  } catch (e) {
    view.replaceChildren(adminSubnav("history"), el("div", { class: "empty" }, "Couldn't load: " + e.message));
  }
}

function render(view, products) {
  const select = el("select", { onChange: (e) => { productId = Number(e.target.value); refreshTimeline(); } },
    products.map((p) => el("option", { value: p.id, selected: p.id === productId }, p.name)));
  const price = el("input", { type: "number", step: "0.01", value: "17.50" });

  view.replaceChildren(
    adminSubnav("history"),
    pageHead("Audit trail", "Version history",
      "nORM keeps a provider-neutral history of every product change. Tag a moment, edit the price, then read the product as-of the tag, diff the versions, or restore it."),
    el("div", { class: "card" },
      el("div", { class: "temporal-bar" },
        el("label", {}, "Product", select),
        el("label", {}, "New price", price),
        el("div", { class: "btn-row" },
          action("Tag now", "btn-secondary", async () => {
            const name = "v-" + Date.now().toString(36); await api.createTag(name); lastTag = name;
            toast(`Tagged “${name}”`); await refreshTimeline();
          }),
          action("Update price", "btn-primary", async () => {
            await api.updatePrice(productId, Number(price.value)); toast("Price updated"); await refreshTimeline();
          }),
          action("Read as-of tag", "btn-secondary", async () => {
            if (!lastTag) return toast("Create a tag first", "danger");
            const r = await api.asOf(productId, lastTag); toast(`As of “${lastTag}”: ${money(r.price)}`);
          }),
          action("Diff", "btn-secondary", showDiff),
          action("Restore latest tag", "btn-secondary", async () => {
            if (!lastTag) return toast("Create a tag first", "danger");
            const r = await api.restore(productId, lastTag); toast(r.restored ? "Restored" : "Nothing to restore"); await refreshTimeline();
          })))),
    el("div", { class: "timeline", id: "timeline" }),
    el("pre", { class: "out", id: "diffOut", style: "display:none" }),
  );
}

function action(label, cls, fn) {
  const b = el("button", { class: "btn btn-sm " + cls }, label);
  b.addEventListener("click", async () => {
    b.disabled = true;
    try { await fn(); } catch (e) { toast(e.message, "danger"); } finally { b.disabled = false; }
  });
  return b;
}

async function refreshTimeline() {
  const tl = document.getElementById("timeline");
  if (!tl || !productId) return;
  tl.replaceChildren(el("div", { class: "loading" }, el("span", { class: "spinner" }), "Reading history…"));
  try {
    const hist = await api.history(productId);
    tl.replaceChildren(...(hist.length
      ? hist.map(historyRow)
      : [el("div", { class: "empty" }, "No history yet — update the price to record a version.")]));
  } catch (e) { tl.replaceChildren(el("div", { class: "empty" }, e.message)); }
}

function historyRow(h) {
  return el("div", { class: "tl-row" },
    el("span", { class: "chip " + (h.operation === "Delete" ? "pill-danger" : "accent") }, h.operation),
    el("span", { class: "tl-name" }, h.name),
    el("span", { class: "mono" }, money(h.price)),
    el("span", { class: "tl-time mono muted" }, `${fmt(h.validFrom)} → ${h.validTo ? fmt(h.validTo) : "now"}`));
}

async function showDiff() {
  const out = document.getElementById("diffOut");
  const diffs = await api.diff(productId);
  out.style.display = "block";
  out.textContent = diffs.length
    ? diffs.map((d) => `${fmt(d.from)} → ${fmt(d.to)}  [${d.operation}]\n` +
        d.changes.map((c) => `   ${c.propertyName}: ${c.previousValue} → ${c.currentValue}`).join("\n")).join("\n\n")
    : "No changes recorded yet.";
}

function fmt(iso) { try { return new Date(iso).toLocaleString(); } catch { return iso; } }
