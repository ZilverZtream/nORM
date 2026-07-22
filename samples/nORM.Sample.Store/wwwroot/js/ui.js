// Small DOM helpers — a hyperscript-style builder so views read like markup without a framework.

export function el(tag, attrs = {}, ...kids) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) continue;
    if (k === "class") n.className = v;
    else if (k === "html") n.innerHTML = v;
    else if (k === "dataset") Object.assign(n.dataset, v);
    else if (k.startsWith("on") && typeof v === "function") n.addEventListener(k.slice(2).toLowerCase(), v);
    else if (v === true) n.setAttribute(k, "");
    else n.setAttribute(k, v);
  }
  for (const kid of kids.flat(Infinity)) {
    if (kid == null || kid === false) continue;
    n.append(kid.nodeType ? kid : document.createTextNode(String(kid)));
  }
  return n;
}

export const money = (n) => "$" + Number(n ?? 0).toFixed(2);
export const escapeHtml = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

let toastTimer;
export function toast(message, kind = "") {
  const t = document.getElementById("toast");
  t.textContent = message;
  t.className = `toast show ${kind}`;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (t.className = "toast"), 2800);
}

export function mount(node) {
  const view = document.getElementById("view");
  view.replaceChildren(node);
  view.scrollTo?.(0, 0);
  window.scrollTo(0, 0);
}

export function loading(label = "Loading") {
  return el("div", { class: "loading" }, el("span", { class: "spinner" }), label + "…");
}
