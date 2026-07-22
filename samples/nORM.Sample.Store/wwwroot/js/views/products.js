import { el, money, mount, loading, toast } from "../ui.js";
import { api } from "../api.js";
import { adminSubnav, pageHead } from "./admin-common.js";

export async function renderProducts() {
  const view = el("div", { class: "page" }, adminSubnav("products"), loading("Loading products"));
  mount(view);
  try {
    const data = await api.catalog(1, 50);
    view.replaceChildren(
      adminSubnav("products"),
      pageHead("Catalog", "Products",
        "Rename writes through the tracked change tracker (only the changed column is updated); Set price uses the set-based ExecuteUpdate path. Both stay inside this store's tenant boundary."),
      el("div", { class: "prod-admin" }, data.items.map(productRow)),
    );
  } catch (e) {
    view.replaceChildren(adminSubnav("products"), el("div", { class: "empty" }, "Couldn't load: " + e.message));
  }
}

function productRow(p) {
  const nameInput = el("input", { value: p.name });
  const priceInput = el("input", { type: "number", step: "0.01", value: Number(p.price).toFixed(2) });

  const rename = el("button", { class: "btn btn-sm" }, "Rename");
  rename.addEventListener("click", async () => {
    rename.disabled = true;
    try { await api.rename(p.id, nameInput.value); toast(`Renamed to “${nameInput.value}”`); }
    catch (e) { toast(e.message, "danger"); } finally { rename.disabled = false; }
  });

  const setPrice = el("button", { class: "btn btn-sm btn-primary" }, "Set price");
  setPrice.addEventListener("click", async () => {
    setPrice.disabled = true;
    try { await api.updatePrice(p.id, Number(priceInput.value)); toast(`Price set to ${money(priceInput.value)}`); }
    catch (e) { toast(e.message, "danger"); } finally { setPrice.disabled = false; }
  });

  return el("div", { class: "prod-row card" },
    el("div", { class: "prod-thumb" }, (p.name?.[0] || "•").toUpperCase()),
    el("div", { class: "prod-fields" },
      el("span", { class: "sku mono" }, p.sku),
      el("div", { class: "prod-edit" }, nameInput, rename)),
    el("div", { class: "prod-price-edit" }, el("span", { class: "mono muted" }, "$"), priceInput, setPrice),
  );
}
