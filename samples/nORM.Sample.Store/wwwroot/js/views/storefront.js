import { el, money, mount, loading, toast } from "../ui.js";
import { api } from "../api.js";
import { state, tenantOf, ENGINE_LABEL } from "../state.js";

let page = 1;

export async function renderStorefront() {
  const tenant = tenantOf(state.me.tenantId);
  const view = el("div", { class: "page" }, loading("Loading catalog"));
  mount(view);
  try {
    const data = await api.catalog(page, 6);
    view.replaceChildren(
      el("section", { class: "store-hero" },
        el("div", {},
          el("p", { class: "kicker" }, "Now serving"),
          el("h1", {}, tenant.name),
          el("p", { class: "lede" }, tenant.tagline)),
        el("div", {},
          el("span", { class: "served" }, el("span", { class: "dot" }), `served from ${ENGINE_LABEL[state.activeEngine] ?? state.activeEngine}`)),
      ),
      el("div", { class: "catalog" }, data.items.map(productCard)),
      pager(data),
    );
  } catch (e) {
    view.replaceChildren(el("div", { class: "empty" }, "Couldn't load the catalog: " + e.message));
  }
}

function productCard(p) {
  return el("article", { class: "product" },
    el("div", { class: "thumb" }, (p.name?.[0] || "•").toUpperCase()),
    el("div", { class: "body" },
      el("span", { class: "sku mono" }, p.sku),
      el("span", { class: "name" }, p.name),
      el("div", { class: "foot" },
        el("span", { class: "price" }, money(p.price)),
        el("button", { class: "btn btn-primary btn-sm", onClick: () => toast(`Added ${p.name} to cart`) }, "Add")),
    ),
  );
}

function pager(data) {
  const row = el("div", { class: "pager" });
  if (data.totalPages <= 1) return row;
  row.append(
    el("button", { class: "btn btn-sm", disabled: page <= 1, onClick: () => { page = Math.max(1, page - 1); renderStorefront(); } }, "‹ Prev"),
    el("span", { class: "chip mono" }, `page ${data.page} of ${data.totalPages}`),
    el("button", { class: "btn btn-sm", disabled: page >= data.totalPages, onClick: () => { page = Math.min(data.totalPages, page + 1); renderStorefront(); } }, "Next ›"),
  );
  return row;
}
