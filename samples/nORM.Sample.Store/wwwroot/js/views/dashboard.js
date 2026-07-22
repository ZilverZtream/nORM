import { el, money, mount, loading, toast } from "../ui.js";
import { api } from "../api.js";
import { state, tenantOf, ENGINE_LABEL } from "../state.js";

export async function renderDashboard() {
  const view = el("div", { class: "page" }, adminSubnav("dashboard"), loading("Loading dashboard"));
  mount(view);
  try {
    const d = await api.dashboard();
    const tenant = tenantOf(state.me.tenantId);
    view.replaceChildren(
      adminSubnav("dashboard"),
      el("div", { class: "page-head" },
        el("p", { class: "kicker" }, `${tenant.name} · admin`),
        el("h1", {}, "Dashboard")),
      el("div", { class: "stat-grid" },
        stat("Active products", d.metrics.activeProducts),
        stat("Open orders", d.metrics.openOrders),
        stat("Revenue", money(d.metrics.revenue)),
        stat("Audit events", d.metrics.eventCount)),
      el("div", { class: "grid-2" },
        el("section", { class: "card span" },
          el("div", { class: "card-head" }, el("h2", {}, "Recent orders"), el("span", { class: "chip mono" }, "Include + split query")),
          ordersTable(d.orders)),
        el("section", { class: "card" },
          el("div", { class: "card-head" }, el("h2", {}, "Running on")),
          el("p", { class: "muted" }, "The live database engine. Same app, same schema — swap it from Infrastructure and this whole screen re-tints and keeps working, with your data carried across."),
          el("div", { class: "engine-line" },
            el("span", { class: "dot" }),
            el("span", { class: "mono", style: "font-size:1rem;color:var(--accent);font-weight:600" }, ENGINE_LABEL[state.activeEngine] ?? state.activeEngine))),
        el("section", { class: "card" },
          el("div", { class: "card-head" }, el("h2", {}, "Tenant boundary")),
          el("p", { class: "muted" }, `Every read and write on this screen is scoped to ${tenant.name}. nORM injects this predicate on the generated path:`),
          el("pre", { class: "out" }, d.tenantBoundary.predicateSql || "(tenant predicate applied)"))),
    );
  } catch (e) {
    view.replaceChildren(adminSubnav("dashboard"), el("div", { class: "empty" }, "Couldn't load: " + e.message));
  }
}

function adminSubnav(active) {
  const item = (label, key) =>
    el("a", {
      class: active === key ? "active" : "",
      onClick: () => { if (key !== "dashboard") toast("Building this screen next"); },
    }, label);
  return el("nav", { class: "subnav" },
    item("Dashboard", "dashboard"),
    item("Products", "products"),
    item("Orders", "orders"),
    item("History", "history"),
    item("Infrastructure", "infra"),
    item("System", "system"));
}

function stat(label, value) {
  return el("div", { class: "stat" }, el("div", { class: "label" }, label), el("div", { class: "value" }, value));
}

function ordersTable(orders) {
  if (!orders?.length) return el("div", { class: "empty" }, "No orders yet.");
  const rows = orders.map((o) =>
    el("tr", {},
      el("td", { class: "mono" }, "#" + o.id),
      el("td", {}, o.status),
      el("td", {}, o.itemCount + " items"),
      el("td", {}, money(o.total))));
  return el("table", { class: "table" },
    el("thead", {}, el("tr", {}, el("th", {}, "Order"), el("th", {}, "Status"), el("th", {}, "Items"), el("th", {}, "Total"))),
    el("tbody", {}, rows));
}
