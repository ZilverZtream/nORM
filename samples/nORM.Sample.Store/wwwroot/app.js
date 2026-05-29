let selectedTenant = "tenant-a";
let currentTag = null;

const loginView = document.querySelector("#login");
const appView = document.querySelector("#app");
const temporalResult = document.querySelector("#temporalResult");
const evidenceResult = document.querySelector("#evidenceResult");
const tenantProofResult = document.querySelector("#tenantProofResult");

document.querySelectorAll(".tenant-button").forEach(button => {
  button.addEventListener("click", () => {
    selectedTenant = button.dataset.tenant;
    document.querySelectorAll(".tenant-button").forEach(x => x.classList.remove("active"));
    button.classList.add("active");
  });
});

document.querySelector("#loginButton").addEventListener("click", async () => {
  await postJson("/api/login", { tenant: selectedTenant });
  loginView.classList.add("hidden");
  appView.classList.remove("hidden");
  await loadDashboard();
});

document.querySelector("#logoutButton").addEventListener("click", async () => {
  await fetch("/api/logout", { method: "POST" });
  appView.classList.add("hidden");
  loginView.classList.remove("hidden");
});

document.querySelector("#refreshButton").addEventListener("click", loadDashboard);

document.querySelector("#tagButton").addEventListener("click", async () => {
  currentTag = `ui-${Date.now()}`;
  const result = await postJson("/api/temporal/tags", { name: currentTag });
  setResult(temporalResult, `Created tag ${result.name}`, "pass");
});

document.querySelector("#updatePriceButton").addEventListener("click", async () => {
  const id = Number(document.querySelector("#productIdInput").value);
  const price = Number(document.querySelector("#priceInput").value);
  const result = await postJson(`/api/products/${id}/price`, { price });
  setResult(temporalResult, `Updated rows: ${result.updated}`, result.updated === 1 ? "pass" : "fail");
  await loadDashboard();
});

document.querySelector("#asOfButton").addEventListener("click", async () => {
  const id = Number(document.querySelector("#productIdInput").value);
  if (!currentTag) {
    setResult(temporalResult, "Create a tag before reading historical state.", "fail");
    return;
  }

  const response = await fetch(`/api/temporal/products/${id}?tag=${encodeURIComponent(currentTag)}`);
  if (!response.ok) {
    setResult(temporalResult, `No product ${id} visible as of ${currentTag}`, "fail");
    return;
  }

  setResult(temporalResult, JSON.stringify(await response.json(), null, 2), "pass");
});

document.querySelector("#historyButton").addEventListener("click", async () => {
  const id = Number(document.querySelector("#productIdInput").value);
  await loadHistory(id);
});

document.querySelector("#diffButton").addEventListener("click", async () => {
  const id = Number(document.querySelector("#productIdInput").value);
  const response = await fetch(`/api/temporal/products/${id}/diff`);
  if (!response.ok) {
    setResult(temporalResult, `Diff read failed for product ${id}.`, "fail");
    return;
  }

  const rows = await response.json();
  setResult(temporalResult, JSON.stringify(rows, null, 2), rows.length > 0 ? "pass" : "fail");
});

document.querySelector("#restoreButton").addEventListener("click", async () => {
  const id = Number(document.querySelector("#productIdInput").value);
  if (!currentTag) {
    setResult(temporalResult, "Create a tag before restoring historical state.", "fail");
    return;
  }

  const result = await postJson(`/api/temporal/products/${id}/restore`, { tag: currentTag });
  setResult(temporalResult, JSON.stringify(result, null, 2), result.restored === 1 ? "pass" : "fail");
  await loadDashboard();
  await loadHistory(id);
});

document.querySelector("#pruneButton").addEventListener("click", async () => {
  const id = Number(document.querySelector("#productIdInput").value);
  const result = await postJson(`/api/temporal/products/${id}/history/prune`, {});
  setResult(temporalResult, JSON.stringify(result, null, 2), result.pruned > 0 ? "pass" : "fail");
  await loadHistory(id);
});

document.querySelector("#tenantProofButton").addEventListener("click", async () => {
  setResult(tenantProofResult, "Running cross-tenant write proof...", "");
  const result = await postJson("/api/tenant-boundary/prove", {});
  setResult(tenantProofResult, JSON.stringify(result, null, 2), result.passed ? "pass" : "fail");
});

document.querySelector("#verifyButton").addEventListener("click", async () => {
  setResult(evidenceResult, "Running provider scenario...", "");
  const result = await postJson("/api/admin/verify", {});
  setResult(evidenceResult, JSON.stringify(result, null, 2), result.success ? "pass" : "fail");
  await loadDashboard();
});

document.querySelector("#bulkButton").addEventListener("click", async () => {
  const result = await postJson("/api/events/bulk", {});
  setResult(evidenceResult, JSON.stringify(result, null, 2), result.inserted > 0 ? "pass" : "fail");
  await loadDashboard();
});

async function loadDashboard() {
  const meResponse = await fetch("/api/me");
  if (!meResponse.ok) return;
  const me = await meResponse.json();
  document.querySelector("#providerBadge").textContent = `Provider: ${me.provider}`;
  document.querySelector("#tenantBadge").textContent = me.tenantName;

  const data = await (await fetch("/api/dashboard")).json();
  renderTenantBoundary(data.tenantBoundary);
  renderMetrics(data.metrics);
  renderProducts(data.products);
  renderOrders(data.orders, data.totals);
  renderEvents(data.recentEvents);
}

function renderTenantBoundary(boundary) {
  setResult(document.querySelector("#tenantDiagnostic"), JSON.stringify({
    table: boundary.tableName,
    tenantColumn: boundary.tenantColumnName,
    tenantIdType: boundary.tenantIdType,
    predicate: boundary.predicateSql
  }, null, 2), boundary.isTenantScoped ? "pass" : "fail");
}

function renderMetrics(metrics) {
  document.querySelector("#metricProducts").textContent = metrics.activeProducts;
  document.querySelector("#metricOrders").textContent = metrics.openOrders;
  document.querySelector("#metricRevenue").textContent = formatMoney(metrics.revenue);
  document.querySelector("#metricEvents").textContent = metrics.eventCount;
}

function renderProducts(products) {
  const host = document.querySelector("#products");
  host.innerHTML = "";
  for (const product of products) {
    const card = document.createElement("article");
    card.className = "product-card";
    card.innerHTML = `
      <div>
        <strong>${escapeHtml(product.name)}</strong>
        <span class="muted">${escapeHtml(product.sku)}</span>
      </div>
      <span class="price">${formatMoney(product.price)}</span>
    `;
    host.appendChild(card);
  }
}

function renderOrders(orders, totals) {
  const host = document.querySelector("#orders");
  host.innerHTML = "";
  document.querySelector("#orderSummary").textContent =
    totals.map(x => `${x.status}: ${formatMoney(x.total)}`).join(" | ");
  for (const order of orders) {
    const row = document.createElement("article");
    row.className = "order-row";
    row.innerHTML = `
      <div>
        <strong>Order ${order.id}</strong>
        <span class="muted">${escapeHtml(order.status)} - ${order.lineCount} lines - ${order.itemCount} items</span>
      </div>
      <strong>${formatMoney(order.total)}</strong>
    `;
    host.appendChild(row);
  }
}

function renderEvents(events) {
  const host = document.querySelector("#events");
  host.innerHTML = "";
  for (const event of events) {
    const row = document.createElement("article");
    row.className = "event-row";
    row.innerHTML = `
      <span class="event-pill">${escapeHtml(event.eventType)}</span>
      <div>
        <strong>${escapeHtml(event.message)}</strong>
        <span class="muted">${new Date(event.createdUtc).toLocaleString()}</span>
      </div>
    `;
    host.appendChild(row);
  }
}

async function loadHistory(id) {
  const response = await fetch(`/api/temporal/products/${id}/history`);
  if (!response.ok) {
    setResult(temporalResult, `History read failed for product ${id}.`, "fail");
    return;
  }

  const rows = await response.json();
  renderTimeline(rows);
  setResult(temporalResult, JSON.stringify(rows.map(row => ({
    operation: row.operation,
    validFrom: row.validFrom,
    validTo: row.validTo,
    name: row.name,
    price: row.price
  })), null, 2), rows.length > 0 ? "pass" : "fail");
}

function renderTimeline(rows) {
  const host = document.querySelector("#historyTimeline");
  host.innerHTML = "";
  if (rows.length === 0) {
    host.innerHTML = `<p class="muted">No tenant-visible history rows.</p>`;
    return;
  }

  rows.forEach((row, index) => {
    const previous = rows[index - 1];
    const delta = previous ? Number(row.price) - Number(previous.price) : 0;
    const item = document.createElement("article");
    item.className = "timeline-item";
    item.innerHTML = `
      <div class="timeline-op">${escapeHtml(row.operation)}</div>
      <div>
        <strong>${escapeHtml(row.name)} ${formatMoney(row.price)}</strong>
        <span class="muted">${new Date(row.validFrom).toLocaleString()} -> ${new Date(row.validTo).toLocaleString()}</span>
        ${previous ? `<span class="delta ${delta >= 0 ? "up" : "down"}">${delta >= 0 ? "+" : ""}${formatMoney(delta)}</span>` : ""}
      </div>
    `;
    host.appendChild(item);
  });
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body)
  });
  if (!response.ok) throw new Error(`${url} failed: ${response.status}`);
  return response.json();
}

function setResult(element, text, state) {
  element.classList.remove("pass", "fail");
  if (state) element.classList.add(state);
  element.textContent = text;
}

function formatMoney(value) {
  return new Intl.NumberFormat(undefined, { style: "currency", currency: "USD" }).format(value);
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, c => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;"
  }[c]));
}
