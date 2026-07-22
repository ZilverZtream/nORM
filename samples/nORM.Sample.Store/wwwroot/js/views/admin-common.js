import { el, mount } from "../ui.js";
import * as router from "../router.js";

const ITEMS = [
  ["Dashboard", "dashboard", "/admin"],
  ["Products", "products", "/admin/products"],
  ["Orders", "orders", "/admin/orders"],
  ["History", "history", "/admin/history"],
  ["Infrastructure", "infra", "/admin/infrastructure"],
  ["System", "system", "/admin/system"],
];

export function adminSubnav(active) {
  return el("nav", { class: "subnav" },
    ITEMS.map(([label, key, path]) =>
      el("a", { class: active === key ? "active" : "", onClick: () => router.navigate(path) }, label)));
}

export function pageHead(kicker, title, lede) {
  return el("div", { class: "page-head" },
    el("p", { class: "kicker" }, kicker),
    el("h1", {}, title),
    lede ? el("p", { class: "lede" }, lede) : null);
}

export function renderPlaceholder(active, title) {
  mount(el("div", { class: "page" }, adminSubnav(active), pageHead("Admin", title),
    el("div", { class: "card" }, el("p", { class: "muted" }, "This screen is coming next."))));
}
