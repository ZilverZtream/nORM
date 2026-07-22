import { el, money, mount, loading } from "../ui.js";
import { api } from "../api.js";
import { adminSubnav, pageHead } from "./admin-common.js";

export async function renderOrders() {
  const view = el("div", { class: "page" }, adminSubnav("orders"), loading("Loading orders"));
  mount(view);
  try {
    const orders = await api.orders();
    view.replaceChildren(
      adminSubnav("orders"),
      pageHead("Fulfilment", "Orders",
        "Each order and its line items are eagerly loaded with Include + AsSplitQuery, scoped to this store."),
      orders.length
        ? el("div", { class: "orders-list" }, orders.map(orderCard))
        : el("div", { class: "empty" }, "No orders yet."),
    );
  } catch (e) {
    view.replaceChildren(adminSubnav("orders"), el("div", { class: "empty" }, "Couldn't load: " + e.message));
  }
}

function orderCard(o) {
  const lineRows = o.lines.length
    ? o.lines.map((l) =>
        el("tr", {},
          el("td", { class: "mono" }, "#" + l.id),
          el("td", { class: "mono" }, "product " + l.productId),
          el("td", {}, String(l.quantity)),
          el("td", {}, money(l.unitPrice)),
          el("td", {}, money(l.quantity * l.unitPrice))))
    : [el("tr", {}, el("td", { colspan: "5", class: "muted" }, "No line items"))];

  return el("section", { class: "card order-card" },
    el("div", { class: "order-head" },
      el("div", {},
        el("span", { class: "mono order-id" }, "#" + o.id),
        el("span", { class: "chip " + ((o.status || "").toLowerCase() === "open" ? "accent" : "") }, o.status)),
      el("span", { class: "order-total" }, money(o.total))),
    el("table", { class: "table" },
      el("thead", {}, el("tr", {},
        el("th", {}, "Line"), el("th", {}, "Product"), el("th", {}, "Qty"), el("th", {}, "Unit"), el("th", {}, "Subtotal"))),
      el("tbody", {}, lineRows)),
  );
}
