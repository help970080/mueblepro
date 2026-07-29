/* =====================================================================
   CobraPro · cliente de API compartido
   Incluye este archivo en los TRES HTML, antes de su <script> principal:
       <script src="api.js"></script>
   Convierte los prototipos (datos en memoria) en una app real conectada
   a un solo backend (tu server.js de Express + Sequelize + PostgreSQL).
   ===================================================================== */
(function (global) {
  // 1) Apunta a tu backend en Render (mismo origen de datos para los 3)
  const BASE = "https://TU-BACKEND.onrender.com";   // <-- cámbialo

  const TOKEN_KEY = "cobrapro_token";
  const getToken = () => localStorage.getItem(TOKEN_KEY);
  const setToken = (t) => localStorage.setItem(TOKEN_KEY, t);
  const clearToken = () => localStorage.removeItem(TOKEN_KEY);

  function headers(json = true) {
    const h = {};
    if (json) h["Content-Type"] = "application/json";
    const t = getToken();
    if (t) h["Authorization"] = "Bearer " + t;
    return h;
  }

  async function req(method, path, body) {
    const res = await fetch(BASE + path, {
      method,
      headers: headers(!!body),
      body: body ? JSON.stringify(body) : undefined,
    });
    if (res.status === 401) { clearToken(); throw new Error("No autorizado"); }
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.message || data.error || ("Error " + res.status));
    return data;
  }

  // genera una clave única por movimiento (clave de idempotencia)
  function uuid() {
    return "cp-" + Date.now() + "-" + Math.random().toString(36).slice(2, 9);
  }

  const API = {
    // ---- auth (rutas /api/auth de tu server.js) ----
    async login(email, password) {
      const d = await req("POST", "/api/auth/login", { email, password });
      if (d.token) setToken(d.token);
      return d; // {token, user:{role, tiendaId, ...}}
    },
    logout() { clearToken(); },
    isLogged: () => !!getToken(),

    // ---- cartera / clientes (/api/clients, /api/sales) ----
    getCartera: (params = "") => req("GET", "/api/sales" + params),
    getClientes: (q = "") => req("GET", "/api/clients?search=" + encodeURIComponent(q)),
    getEstadoCuenta: (saleId) => req("GET", "/api/sales/" + saleId + "/movimientos"),

    // ---- nuevo crédito (tu endpoint create-with-imei o /api/sales) ----
    crearCredito: (payload) => req("POST", "/api/sales", payload),

    // ---- pagos: SIEMPRE con clave de idempotencia (clave en campo offline) ----
    // payload: { saleId, monto, forma:'efectivo|transferencia|deposito|ajuste' }
    registrarPago(payload) {
      return req("POST", "/api/sales/" + payload.saleId + "/pago", {
        ...payload,
        idempotencyKey: payload.idempotencyKey || uuid(),
      });
    },

    // ---- gestión sin pago (alimenta collection_logs) ----
    registrarGestion: (saleId, payload) =>
      req("POST", "/api/collections/" + saleId + "/log", { ...payload, idempotencyKey: uuid() }),

    // ---- supervisor: cargo / abono / condonación ----
    agregarCargo: (saleId, p)  => req("POST", "/api/sales/" + saleId + "/cargo",  { ...p, idempotencyKey: uuid() }),
    agregarAbono: (saleId, p)  => req("POST", "/api/sales/" + saleId + "/abono",  { ...p, idempotencyKey: uuid() }),
    condonar:     (saleId, p)  => req("POST", "/api/sales/" + saleId + "/condonar",{ ...p, idempotencyKey: uuid() }),

    // ---- caja de sucursal ----
    getCaja: ()            => req("GET",  "/api/caja/hoy"),
    confirmarEntrega: (id) => req("POST", "/api/caja/entrega", { porEntregarId: id }),

    // ---- dashboard / reportes del admin ----
    getDashboard: (period = "semana") => req("GET", "/api/dashboard?period=" + period),
    getReportePagos: (period = "semana") => req("GET", "/api/reports/pagos?period=" + period),

    // ---- cola offline (para el cobrador en ruta) ----
    queue: {
      _k: "cobrapro_queue",
      add(item) { const q = this.all(); q.push({ ...item, key: uuid() }); localStorage.setItem(this._k, JSON.stringify(q)); },
      all() { try { return JSON.parse(localStorage.getItem(this._k)) || []; } catch { return []; } },
      clear() { localStorage.removeItem(this._k); },
      async flush() {
        const q = this.all(); const ok = [];
        for (const it of q) {
          try { await API.registrarPago({ ...it, idempotencyKey: it.key }); ok.push(it.key); }
          catch (e) { break; } // si falla, reintenta luego (la clave evita duplicados)
        }
        const rest = this.all().filter((it) => !ok.includes(it.key));
        localStorage.setItem(this._k, JSON.stringify(rest));
        return { enviados: ok.length, pendientes: rest.length };
      },
    },
  };

  // al recuperar conexión, vacía la cola pendiente automáticamente
  global.addEventListener("online", () => API.queue.flush());
  global.API = API;
})(window);

/* =====================================================================
   CÓMO SUSTITUIR LOS DATOS DE DEMO POR DATOS REALES
   ---------------------------------------------------------------------
   ANTES (prototipo, panel de sucursal):
       let clientes = [ {id:101, nombre:'María González', ...}, ... ];
       buscar();

   DESPUÉS (app real):
       let clientes = [];
       async function cargar(q='') {
         clientes = await API.getClientes(q);   // viene de PostgreSQL
         buscar();
       }
       cargar();

   PAGO EN RUTA (cobrador, tolerante a falta de señal):
       function confirmarCobro() {
         const item = { saleId: cr.id, monto, forma: formaPay };
         if (navigator.onLine) API.registrarPago(item).catch(()=>API.queue.add(item));
         else API.queue.add(item);   // se sube solo al reconectar
         render();
       }
   ===================================================================== */
