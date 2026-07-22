// The store's data client. Every screen talks to the backend only through here — so if you were
// adapting this template to your own app, this is the one file that knows your endpoints.

async function req(method, url, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(url, opts);
  if (res.status === 401) throw new AuthError();
  if (!res.ok) {
    let message = res.statusText;
    try { const j = await res.json(); message = j.error || j.title || message; } catch { /* text body */ }
    throw new Error(message);
  }
  return res.status === 204 ? null : res.json();
}

export class AuthError extends Error { constructor() { super("Not signed in"); this.name = "AuthError"; } }

export const api = {
  me: () => req("GET", "/api/me"),
  login: (tenant) => req("POST", "/api/login", { tenant }),
  logout: () => req("POST", "/api/logout"),

  dashboard: () => req("GET", "/api/dashboard"),
  catalog: (page = 1, size = 6) => req("GET", `/api/catalog?page=${page}&size=${size}`),
  updatePrice: (id, price) => req("POST", `/api/products/${id}/price`, { price }),
  rename: (id, name) => req("POST", `/api/products/${id}/rename`, { name }),
  bulkEvents: () => req("POST", "/api/events/bulk"),

  createTag: (name) => req("POST", "/api/temporal/tags", { name }),
  asOf: (id, tag) => req("GET", `/api/temporal/products/${id}?tag=${encodeURIComponent(tag)}`),
  history: (id) => req("GET", `/api/temporal/products/${id}/history`),
  diff: (id) => req("GET", `/api/temporal/products/${id}/diff`),
  restore: (id, tag) => req("POST", `/api/temporal/products/${id}/restore`, { tag }),

  providers: () => req("GET", "/api/settings/providers"),
  testProvider: (name, connectionString) => req("POST", `/api/settings/providers/${name}/test`, { connectionString }),
  saveConnection: (name, connectionString) => req("POST", `/api/settings/providers/${name}/connection`, { connectionString }),
  activate: (name, connectionString = null) => req("POST", `/api/settings/providers/${name}/activate`, { connectionString }),

  isolationProof: () => req("POST", "/api/isolation/concurrent-proof"),
  crossTenantProof: () => req("POST", "/api/tenant-boundary/prove"),
  verifyScenario: () => req("POST", "/api/admin/verify"),
};
