// A tiny hash router. Routes are "/segment/:param" patterns; the first match wins.
const routes = [];

export function route(pattern, handler) { routes.push({ parts: pattern.split("/").filter(Boolean), handler }); }
export function navigate(path) { if (currentPath() === path) render(); else location.hash = path; }
export function currentPath() { return decodeURI(location.hash.slice(1)) || "/store"; }

function match(path) {
  const segs = path.split("/").filter(Boolean);
  for (const r of routes) {
    if (r.parts.length !== segs.length) continue;
    const params = {};
    let ok = true;
    for (let i = 0; i < r.parts.length; i++) {
      if (r.parts[i].startsWith(":")) params[r.parts[i].slice(1)] = segs[i];
      else if (r.parts[i] !== segs[i]) { ok = false; break; }
    }
    if (ok) return { handler: r.handler, params };
  }
  return null;
}

let onChange = () => {};
export function start(cb) { onChange = cb; addEventListener("hashchange", render); render(); }

function render() {
  const path = currentPath();
  const m = match(path);
  onChange(path);
  if (m) m.handler(m.params);
}
