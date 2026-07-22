import { el, toast } from "../ui.js";
import { api } from "../api.js";
import { TENANTS } from "../state.js";

export function renderLogin(onLoggedIn) {
  let selected = "tenant-a";

  const cards = Object.values(TENANTS).map((t) =>
    el("button", {
      type: "button",
      class: "tenant-card" + (t.key === selected ? " active" : ""),
      dataset: { key: t.key },
      onClick: () => {
        selected = t.key;
        document.querySelectorAll(".tenant-card").forEach((c) => c.classList.toggle("active", c.dataset.key === selected));
      },
    },
      el("span", { class: "avatar", style: `background:${t.color}` }, t.avatar),
      el("span", {}, el("b", {}, t.name), el("small", {}, t.key === "tenant-a" ? "Tenant A" : "Tenant B")),
    ));

  const signIn = el("button", { class: "btn btn-primary btn-block" }, "Enter store");
  const form = el("form", { class: "auth-form" },
    el("p", { class: "kicker" }, "Choose a store"),
    el("h2", {}, "Sign in"),
    el("p", { class: "muted" }, "Each tenant is an isolated store. Everything you do runs behind that tenant's boundary."),
    el("div", { class: "tenant-cards" }, cards),
    signIn,
  );
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    signIn.disabled = true;
    try { await api.login(selected); await onLoggedIn(); }
    catch (err) { toast(err.message, "danger"); signIn.disabled = false; }
  });

  return el("div", { class: "auth" },
    el("div", { class: "auth-pitch" },
      el("p", { class: "kicker" }, "Provider-mobile commerce"),
      el("h1", { html: "One app.<br>Any database.<br><em>Swapped live.</em>" }),
      el("p", { class: "lede" }, "The same code runs on SQLite, SQL Server, PostgreSQL and MySQL. Change the engine while the app is running — every screen keeps working, tenant isolation intact, and your data comes with it."),
      el("div", { class: "auth-engines" },
        el("span", { class: "auth-engine", dataset: { e: "sqlite" } }, "SQLite"),
        el("span", { class: "auth-engine", dataset: { e: "sqlserver" } }, "SQL Server"),
        el("span", { class: "auth-engine", dataset: { e: "postgres" } }, "PostgreSQL"),
        el("span", { class: "auth-engine", dataset: { e: "mysql" } }, "MySQL"),
      ),
    ),
    form,
  );
}
