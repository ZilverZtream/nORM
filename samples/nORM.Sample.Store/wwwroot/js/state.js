// Shared app state + the two demo stores. Each tenant is a genuinely different shop, so switching
// tenants feels like switching businesses — which is the multi-tenant point.

export const state = { me: null, activeEngine: "sqlite" };

export const TENANTS = {
  101: { id: 101, key: "tenant-a", name: "Aurora Coffee", tagline: "Small-batch roasts, shipped the day they're bagged.", avatar: "AC", color: "#8b5cf6" },
  202: { id: 202, key: "tenant-b", name: "Borealis Goods", tagline: "Cold-climate gear and quietly good basics.", avatar: "BG", color: "#0ea5a4" },
};
export const tenantOf = (id) => TENANTS[id] ?? { id, key: "", name: "Store", tagline: "", avatar: "?", color: "#888" };
export const otherTenant = (id) => (Number(id) === 101 ? TENANTS[202] : TENANTS[101]);

export function setActiveEngine(name) {
  state.activeEngine = name;
  document.documentElement.dataset.engine = name;
}

export const ENGINE_LABEL = { sqlite: "SQLite", sqlserver: "SQL Server", postgres: "PostgreSQL", mysql: "MySQL" };
