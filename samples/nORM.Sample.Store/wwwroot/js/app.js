import { api } from "./api.js";
import * as router from "./router.js";
import { el, toast } from "./ui.js";
import { state, tenantOf, otherTenant, setActiveEngine, ENGINE_LABEL } from "./state.js";
import { renderLogin } from "./views/login.js";
import { renderStorefront } from "./views/storefront.js";
import { renderDashboard } from "./views/dashboard.js";
import { renderInfrastructure } from "./views/infrastructure.js";
import { renderProducts } from "./views/products.js";
import { renderSystem } from "./views/system.js";
import { renderPlaceholder } from "./views/admin-common.js";

const root = document.getElementById("root");
let routerStarted = false;
const savedTheme = localStorage.getItem("norm.theme");
if (savedTheme) document.documentElement.dataset.theme = savedTheme;

boot();

async function boot() {
  try {
    const me = await api.me();
    if (me.authenticated) return enter(me);
  } catch { /* fall through to login */ }
  showLogin();
}

function showLogin() {
  root.replaceChildren(renderLogin(async () => enter(await api.me())));
}

function enter(me) {
  state.me = me;
  setActiveEngine(me.provider);
  renderShell();
  if (!routerStarted) {
    router.route("/store", () => renderStorefront());
    router.route("/admin", () => renderDashboard());
    router.route("/admin/infrastructure", () => renderInfrastructure());
    router.route("/admin/products", () => renderProducts());
    router.route("/admin/system", () => renderSystem());
    router.route("/admin/orders", () => renderPlaceholder("orders", "Orders"));
    router.route("/admin/history", () => renderPlaceholder("history", "Version history"));
    router.start(onRoute);
    routerStarted = true;
  }
  router.navigate(location.hash.slice(1) || "/store");
}

function onRoute(path) {
  document.querySelectorAll(".nav-link").forEach((l) => {
    const seg = l.dataset.route;
    l.classList.toggle("active", path === seg || (seg !== "/store" && path.startsWith(seg)));
  });
}

function renderShell() {
  const tenant = tenantOf(state.me.tenantId);
  const bar = el("header", { class: "appbar" },
    el("div", { class: "brand wordmark" }, el("span", { html: "n<b>ORM</b>" }), el("small", {}, "Store")),
    el("nav", { class: "app-nav" },
      navLink("Storefront", "/store"),
      navLink("Admin", "/admin"),
    ),
    el("div", { class: "spacer" }),
    el("div", { class: "right" },
      el("button", { class: "tenant-switch", title: "Switch store", onClick: switchTenant },
        el("span", { class: "avatar", style: `background:${tenant.color}` }, tenant.avatar),
        el("span", {}, tenant.name)),
      el("button", { class: "engine-pill", title: "Live database engine — open Infrastructure", onClick: () => router.navigate("/admin") },
        el("span", { class: "dot" }),
        el("span", { class: "engine-name", id: "engineName" }, state.activeEngine)),
      el("button", { class: "btn icon-btn btn-ghost", title: "Toggle theme", onClick: toggleTheme }, "◐"),
      el("button", { class: "btn btn-ghost btn-sm", onClick: signOut }, "Sign out"),
    ),
  );
  root.replaceChildren(bar, el("main", { id: "view" }));
}

function navLink(label, route) {
  return el("button", { class: "nav-link", dataset: { route }, onClick: () => router.navigate(route) }, label);
}

async function switchTenant() {
  const next = otherTenant(state.me.tenantId);
  await api.login(next.key);
  state.me = await api.me();
  renderShell();
  router.navigate(location.hash.slice(1) || "/store");
  toast(`Now viewing ${next.name}`);
}

function toggleTheme() {
  const dark = document.documentElement.dataset.theme !== "dark";
  document.documentElement.dataset.theme = dark ? "dark" : "light";
  localStorage.setItem("norm.theme", dark ? "dark" : "light");
}

async function signOut() {
  await api.logout();
  location.hash = "";
  showLogin();
}

// Let the engine swap (from the Infrastructure screen) re-tint the whole shell.
addEventListener("engine-changed", (e) => {
  setActiveEngine(e.detail);
  const n = document.getElementById("engineName");
  if (n) n.textContent = e.detail;
  toast(`Live engine is now ${ENGINE_LABEL[e.detail] ?? e.detail}`);
});
