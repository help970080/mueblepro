'use strict';
/* ============================================================================
 * CobraPro · MÓDULO GRUPAL  (anexo, no toca server.js salvo 2 líneas al final)
 * ----------------------------------------------------------------------------
 * Fase 1: grupos, integrantes, expediente con candado de documentos,
 *         autorización (crea N ventas reales con grupoId), relevo de cobrador.
 *
 * Cómo se monta en server.js (hasta abajo, antes de app.listen):
 *   const grupal = require('./grupal');
 *   grupal.montar(app, ctx);
 * donde ctx trae las funciones del núcleo (ver montar()).
 *
 * DÓNDE VIVEN LOS DATOS
 *   El anexo de CADA agencia se guarda en cobrapro_state con id (9000+tenantId).
 *   Reusa saveRow(id,data): debounce 8s + firma sha1 + reintentos + flush SIGTERM.
 *   El bloque del tenant (id = tenantId) NO se toca ni crece.
 *
 * EL DINERO CORRE POR LOS RIELES DE HOY
 *   Cada integrante = 1 venta real en db.sales con grupoId. Su pago = movAdd normal.
 *   Caja, corte, entrega JC, comisiones, dashboard, aging: sin cambios.
 * ==========================================================================*/

module.exports.montar = function montar(app, ctx) {
  const {
    pool, USE_PG, als, db, saveDB, nextId,
    auth, rol, logOp, fotoGuardar, fotoExpandirLista,
    calcCredito, hoyMXISO, hoyMXDDMM, _normNombre, _canonProm,
    flujoAgregar, movAdd, saldoDe,
    jwt, JWT_SECRET, getTenant
  } = ctx;

  const FLAG = String(process.env.FLAG_GRUPAL || '0') === '1';
  if (!FLAG) {
    console.log('· Módulo grupal: FLAG_GRUPAL=0 (no montado)');
    return;
  }
  console.log('✓ Módulo grupal montado (/api/g/*)');

  // ---- id del anexo grupal a partir del tenant activo ----
  const GBASE = 9000;
  function _tid() { const s = als.getStore(); return s && s.tenantId != null ? +s.tenantId : null; }
  function _gid() { const t = _tid(); return t == null ? null : GBASE + t; }

  // ---- caché en memoria del anexo por tenant + carga perezosa ----
  const _gcache = new Map();   // tenantId -> objeto grupal
  function _vacio() {
    return {
      grupos: [], integrantes: [], juntas: [], solidarios: [],
      ahorro: [], multas: [], ciclos: [], expedientes: [],
      config: {
        tarifa: 's16', garantiaPct: 10, escalaPct: 20, min: 5, max: 10,
        multaRetardo: 20, multaFalta: 50,
        etiquetaRetencion: 'Garantía líquida',
        comision: { porGrupoCerrado: 0, sobreCobranza: 0, porPuntualidad: 0 },
        docsObligatorios: ['ineFrente', 'comprobanteDom', 'firma']
      }
    };
  }
  async function G() {
    const t = _tid();
    if (t == null) return null;
    if (_gcache.has(t)) return _gcache.get(t);
    let data = null;
    if (USE_PG) {
      try {
        const r = await pool.query('SELECT data FROM cobrapro_state WHERE id=$1', [GBASE + t]);
        data = r.rows[0] ? r.rows[0].data : null;
      } catch (e) { console.error('grupal load:', e.message); }
    }
    const obj = data || _vacio();
    // asegura colecciones nuevas si el anexo es viejo
    const base = _vacio();
    for (const k of Object.keys(base)) if (obj[k] == null) obj[k] = base[k];
    if (!obj.config) obj.config = base.config;
    else for (const k of Object.keys(base.config)) if (obj.config[k] == null) obj.config[k] = base.config[k];
    _gcache.set(t, obj);
    return obj;
  }
  function _saveG(g) {
    // reusa el mismo carril de guardado del núcleo, indexado por (9000+tenant)
    const id = _gid();
    if (id == null) return;
    ctx.saveRow(id, g);
  }
  function _nextId(g, coll) { return (g[coll] || []).reduce((m, x) => Math.max(m, x.id || 0), 0) + 1; }

  // ---- helpers de negocio ----
  function _cfg(g) { return g.config; }

  // monto del ciclo n: base * 1.20^(n-1), redondeado a centenas
  function _montoCiclo(base, ciclo, escalaPct) {
    const f = Math.pow(1 + (escalaPct || 20) / 100, (ciclo || 1) - 1);
    return Math.round((base * f) / 100) * 100;
  }

  // cálculo grupal: usa la tarifa configurada (s16 por defecto), SIN descontar primer pago.
  // La cuota se reparte entre TODOS los pagos.
  function _calcGrupal(monto, frecuencia, cfg) {
    const tarifa = cfg.tarifa || 's16';
    const r = calcCredito(tarifa, null, monto, null); // {total, pagos, ...} de la tarifa del tenant
    let total = r.total, pagos = r.pagos;
    if (frecuencia === 'catorcenal') { pagos = Math.ceil(pagos / 2); } // mismo total, mitad de pagos
    const cuota = Math.round((total / pagos) * 100) / 100;
    return { total, pagos, cuota };
  }

  // ¿qué documentos le faltan a un expediente?
  function _faltantes(exp, cfg) {
    const obl = cfg.docsObligatorios || [];
    const docs = (exp && exp.docs) || {};
    return obl.filter(d => !docs[d] || !docs[d].foto);
  }

  // nombre del cobrador que HOY cobra el grupo
  function _cobGrupo(gr) { return gr.cobradorActual || gr.asesor || ''; }

  // ¿el grupo ya está OPERATIVO? (todas sus ventas entregadas por el JC)
  // No dependemos de un flag manual: lo derivamos de la realidad del bloque del tenant.
  function _grupoOperativo(gr) {
    if (gr.estado === 'liquidado' || gr.estado === 'castigado') return false;
    if (gr.estado === 'formacion') return false;
    const ventas = (db.sales || []).filter(s => s.grupoId === gr.id);
    if (!ventas.length) return false;
    return ventas.every(s => s.entregado === true);
  }
  // estado mostrado: formacion → autorizado (esperando entrega) → activo (todo entregado)
  function _estadoVisible(gr) {
    if (gr.estado === 'autorizado' && _grupoOperativo(gr)) return 'activo';
    return gr.estado;
  }

  // ================= GRUPOS =================

  // lista con semáforo, filtrada por rol
  app.get('/api/g/grupos', auth, async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const u = req.user;
    // Solo roles grupales y jefatura ven grupos. El cobrador individual no ve nada grupal.
    const permitidos = ['admin', 'supervisor', 'sucursal', 'promotor_grupal'];
    if (!permitidos.includes(u.rol)) return res.json({ grupos: [], total: 0 });
    let lista = g.grupos.slice();
    // el promotor grupal solo ve los grupos que él cobra
    if (u.rol === 'promotor_grupal') {
      const yo = _normNombre(u.nombre);
      lista = lista.filter(gr => _normNombre(_cobGrupo(gr)) === yo);
    } else if (u.rol === 'sucursal') {
      const sid = u.sucursalId;
      lista = lista.filter(gr => gr.sucursalId == sid);
    }
    const out = lista.map(gr => {
      const ints = g.integrantes.filter(i => i.grupoId === gr.id && i.estado !== 'salida');
      const solVivos = g.solidarios.filter(s => s.grupoId === gr.id && !s.saldado).length;
      const cicloAct = g.ciclos.filter(c => c.grupoId === gr.id).sort((a, b) => b.numero - a.numero)[0] || null;
      // semáforo: verde si sin solidarios vivos, ámbar si 1-2, rojo si 3+
      let sem = 'verde';
      if (solVivos >= 3) sem = 'rojo'; else if (solVivos >= 1) sem = 'ambar';
      return {
        id: gr.id, nombre: gr.nombre, sucursalId: gr.sucursalId,
        asesor: gr.asesor, cobradorActual: _cobGrupo(gr),
        frecuencia: gr.frecuencia, ciclo: gr.ciclo, estado: _estadoVisible(gr),
        junta: gr.junta, integrantes: ints.length, solidariosVivos: solVivos,
        montoBase: gr.montoBase, semaforo: sem,
        juntasAl100: cicloAct ? cicloAct.juntasAl100 : 0,
        juntasConSolidario: cicloAct ? cicloAct.juntasConSolidario : 0
      };
    });
    res.json({ grupos: out, total: out.length });
  });

  // crear grupo en formación
  app.post('/api/g/grupos', auth, rol('admin', 'supervisor', 'sucursal', 'promotor_grupal'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const { nombre, sucursalId, frecuencia, junta, montoBase } = req.body;
    if (!nombre || !String(nombre).trim()) return res.status(400).json({ error: 'Falta el nombre del grupo' });
    const frec = (frecuencia === 'catorcenal') ? 'catorcenal' : 'semanal';
    // el que forma el grupo es el asesor y el cobrador inicial
    const asesor = _canonProm ? _canonProm(req.user.nombre) : req.user.nombre;
    const gr = {
      id: _nextId(g, 'grupos'), nombre: String(nombre).trim(),
      sucursalId: sucursalId != null ? +sucursalId : (req.user.sucursalId || null),
      asesor, asesorId: req.user.id,
      cobradorActual: asesor, cobradorActualId: req.user.id,
      frecuencia: frec,
      junta: junta || { dia: 1, hora: '10:00', lugar: '', lat: null, lng: null },
      ciclo: 1, montoBase: montoBase != null ? +montoBase : 0,
      estado: 'formacion',
      reglas: {
        garantiaPct: _cfg(g).garantiaPct, escalaPct: _cfg(g).escalaPct,
        min: _cfg(g).min, max: _cfg(g).max,
        multaRetardo: _cfg(g).multaRetardo, multaFalta: _cfg(g).multaFalta
      },
      creado: hoyMXISO(), creadoPor: req.user.nombre
    };
    g.grupos.push(gr);
    _saveG(g);
    logOp('g_grupo_crear', 'grupo:' + gr.id, { nombre: gr.nombre, asesor });
    res.json({ ok: true, grupo: gr });
  });

  // ficha del grupo
  app.get('/api/g/grupos/:id', auth, async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const gr = g.grupos.find(x => x.id == req.params.id);
    if (!gr) return res.status(404).json({ error: 'Grupo no encontrado' });
    const ints = g.integrantes.filter(i => i.grupoId === gr.id).map(i => {
      const exp = g.expedientes.find(e => e.integranteId === i.id);
      const cli = (db.clients || []).find(c => c.id === i.clientId) || {};
      return {
        id: i.id, clientId: i.clientId, nombre: cli.nombre || i.nombre || '—',
        rol: i.rol, monto: i.monto, cuota: i.cuota, saleId: i.saleId,
        garantia: i.garantia, estado: i.estado,
        expedienteCompleto: exp ? exp.completo : false,
        faltantes: exp ? _faltantes(exp, _cfg(g)) : _cfg(g).docsObligatorios
      };
    });
    const solidarios = g.solidarios.filter(s => s.grupoId === gr.id && !s.saldado);
    const cicloAct = g.ciclos.filter(c => c.grupoId === gr.id).sort((a, b) => b.numero - a.numero)[0] || null;
    res.json({ grupo: gr, integrantes: ints, solidariosVivos: solidarios, ciclo: cicloAct, cobradorActual: _cobGrupo(gr) });
  });

  // editar día/hora/lugar/frecuencia (no toca asesor ni cobrador)
  app.put('/api/g/grupos/:id', auth, rol('admin', 'supervisor', 'sucursal', 'promotor_grupal'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const gr = g.grupos.find(x => x.id == req.params.id);
    if (!gr) return res.status(404).json({ error: 'Grupo no encontrado' });
    const { junta, frecuencia, nombre, montoBase } = req.body;
    if (junta) gr.junta = Object.assign(gr.junta || {}, junta);
    if (frecuencia === 'semanal' || frecuencia === 'catorcenal') {
      if (gr.estado === 'formacion') gr.frecuencia = frecuencia;
    }
    if (nombre && String(nombre).trim()) gr.nombre = String(nombre).trim();
    if (montoBase != null && gr.estado === 'formacion') gr.montoBase = +montoBase;
    _saveG(g);
    logOp('g_grupo_editar', 'grupo:' + gr.id, {});
    res.json({ ok: true, grupo: gr });
  });

  // RELEVO: cambia quién cobra el grupo, conserva el asesor histórico
  app.put('/api/g/grupos/:id/relevar', auth, rol('admin', 'supervisor'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const gr = g.grupos.find(x => x.id == req.params.id);
    if (!gr) return res.status(404).json({ error: 'Grupo no encontrado' });
    const { nuevoCobradorId, motivo } = req.body;
    const nu = (db.users || []).find(u => u.id == nuevoCobradorId && u.rol === 'promotor_grupal' && u.activo);
    if (!nu) return res.status(404).json({ error: 'Cobrador destino no válido' });
    const anterior = _cobGrupo(gr);
    gr.cobradorActual = _canonProm ? _canonProm(nu.nombre) : nu.nombre;
    gr.cobradorActualId = nu.id;
    _saveG(g);
    logOp('g_relevo', 'grupo:' + gr.id, { de: anterior, a: gr.cobradorActual, motivo: motivo || '' });
    res.json({ ok: true, grupo: gr, de: anterior, a: gr.cobradorActual });
  });

  // ================= INTEGRANTES =================

  // agregar integrante (crea o reusa cliente en el bloque del tenant)
  app.post('/api/g/grupos/:id/integrantes', auth, rol('admin', 'supervisor', 'sucursal', 'promotor_grupal'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const gr = g.grupos.find(x => x.id == req.params.id);
    if (!gr) return res.status(404).json({ error: 'Grupo no encontrado' });
    if (gr.estado !== 'formacion') return res.status(409).json({ error: 'El grupo ya fue autorizado; no se pueden agregar integrantes' });
    const yaN = g.integrantes.filter(i => i.grupoId === gr.id && i.estado !== 'salida').length;
    if (yaN >= _cfg(g).max) return res.status(409).json({ error: 'El grupo ya tiene el máximo de ' + _cfg(g).max + ' integrantes' });

    const { clientId, nombre, tel, calle, col, ciudad, estado, curp, monto, rol: rolInt } = req.body;
    if (!nombre && !clientId) return res.status(400).json({ error: 'Falta el nombre de la integrante' });

    let cli;
    if (clientId) {
      cli = (db.clients || []).find(c => c.id == clientId);
      if (!cli) return res.status(404).json({ error: 'Cliente no encontrado' });
    } else {
      // crea cliente en el bloque del tenant (mismo formato que /api/sales)
      cli = {
        id: nextId('clients'), nombre: String(nombre).trim(), tel: tel || '',
        calle: calle || '', col: col || '', ciudad: ciudad || '', estado: estado || '',
        curp: String(curp || '').trim().toUpperCase(),
        sucursalId: gr.sucursalId, prom: _cobGrupo(gr)
      };
      if (!db.clients) db.clients = [];
      db.clients.push(cli);
      saveDB(); // persiste el bloque del tenant
    }

    const it = {
      id: _nextId(g, 'integrantes'), grupoId: gr.id, clientId: cli.id,
      nombre: cli.nombre,
      rol: ['presidenta', 'tesorera', 'secretaria'].includes(rolInt) ? rolInt : 'socia',
      monto: monto != null ? +monto : (gr.montoBase || 0),
      cuota: 0, saleId: null, ciclo: 1, garantia: 0,
      estado: 'activa', alta: hoyMXISO()
    };
    g.integrantes.push(it);
    // expediente vacío
    g.expedientes.push({
      id: _nextId(g, 'expedientes'), integranteId: it.id, grupoId: gr.id,
      docs: {}, completo: false, faltantes: _cfg(g).docsObligatorios.slice(),
      verificadoPor: null, verificadoAt: null
    });
    _saveG(g);
    logOp('g_integrante_alta', 'grupo:' + gr.id, { integranteId: it.id, clientId: cli.id });
    res.json({ ok: true, integrante: it });
  });

  // quitar integrante (solo en formación)
  app.delete('/api/g/integrantes/:id', auth, rol('admin', 'supervisor', 'sucursal', 'promotor_grupal'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const it = g.integrantes.find(i => i.id == req.params.id);
    if (!it) return res.status(404).json({ error: 'Integrante no encontrada' });
    const gr = g.grupos.find(x => x.id === it.grupoId);
    if (gr && gr.estado !== 'formacion') return res.status(409).json({ error: 'El grupo ya fue autorizado' });
    g.integrantes = g.integrantes.filter(i => i.id !== it.id);
    g.expedientes = g.expedientes.filter(e => e.integranteId !== it.id);
    _saveG(g);
    logOp('g_integrante_baja', 'grupo:' + it.grupoId, { integranteId: it.id });
    res.json({ ok: true });
  });

  // cambiar rol en la mesa directiva
  app.put('/api/g/integrantes/:id/rol', auth, rol('admin', 'supervisor', 'sucursal', 'promotor_grupal'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const it = g.integrantes.find(i => i.id == req.params.id);
    if (!it) return res.status(404).json({ error: 'Integrante no encontrada' });
    const r = req.body.rol;
    if (!['presidenta', 'tesorera', 'secretaria', 'socia'].includes(r)) return res.status(400).json({ error: 'Rol inválido' });
    // un solo cargo por tipo (excepto socia)
    if (r !== 'socia') {
      const ocupa = g.integrantes.find(i => i.grupoId === it.grupoId && i.rol === r && i.id !== it.id);
      if (ocupa) ocupa.rol = 'socia';
    }
    it.rol = r;
    _saveG(g);
    res.json({ ok: true, integrante: it });
  });

  // ================= EXPEDIENTE =================

  // subir/actualizar un documento del expediente (la imagen va a cobrapro_fotos)
  app.post('/api/g/integrantes/:id/doc', auth, rol('admin', 'supervisor', 'sucursal', 'promotor_grupal'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const it = g.integrantes.find(i => i.id == req.params.id);
    if (!it) return res.status(404).json({ error: 'Integrante no encontrada' });
    const { tipo, imagen, vence } = req.body;
    const validos = ['ineFrente', 'ineReverso', 'comprobanteDom', 'firma', 'curp', 'solicitud', 'fotoNegocio', 'pagare'];
    if (!validos.includes(tipo)) return res.status(400).json({ error: 'Tipo de documento inválido' });
    if (!imagen) return res.status(400).json({ error: 'Falta la imagen' });
    let exp = g.expedientes.find(e => e.integranteId === it.id);
    if (!exp) {
      exp = { id: _nextId(g, 'expedientes'), integranteId: it.id, grupoId: it.grupoId, docs: {}, completo: false, faltantes: [] };
      g.expedientes.push(exp);
    }
    const ref = 'grupal:exp:' + it.id + ':' + tipo;
    const marca = await fotoGuardar(imagen, ref);
    exp.docs[tipo] = { foto: marca, capturadoAt: hoyMXISO(), capturadoPor: req.user.nombre, vence: vence || null };
    exp.faltantes = _faltantes(exp, _cfg(g));
    exp.completo = exp.faltantes.length === 0;
    _saveG(g);
    logOp('g_doc', 'integrante:' + it.id, { tipo });
    res.json({ ok: true, completo: exp.completo, faltantes: exp.faltantes });
  });

  // estado del expediente del grupo (quién y qué falta)
  app.get('/api/g/grupos/:id/expediente', auth, async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const gr = g.grupos.find(x => x.id == req.params.id);
    if (!gr) return res.status(404).json({ error: 'Grupo no encontrado' });
    const ints = g.integrantes.filter(i => i.grupoId === gr.id && i.estado !== 'salida');
    const out = ints.map(i => {
      const exp = g.expedientes.find(e => e.integranteId === i.id);
      const cli = (db.clients || []).find(c => c.id === i.clientId) || {};
      return {
        integranteId: i.id, nombre: cli.nombre || i.nombre, rol: i.rol,
        completo: exp ? exp.completo : false,
        faltantes: exp ? _faltantes(exp, _cfg(g)) : _cfg(g).docsObligatorios,
        docs: exp ? Object.keys(exp.docs || {}) : []
      };
    });
    const listos = out.filter(x => x.completo).length;
    res.json({ integrantes: out, listos, total: out.length, todosListos: listos === out.length && out.length > 0 });
  });

  // ================= AUTORIZACIÓN =================
  // valida min/max + expedientes, calcula cuotas, CREA las N ventas con grupoId,
  // retiene la garantía, deja el grupo 'autorizado' listo para entregar por el JC.
  app.post('/api/g/grupos/:id/autorizar', auth, rol('admin', 'supervisor'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const gr = g.grupos.find(x => x.id == req.params.id);
    if (!gr) return res.status(404).json({ error: 'Grupo no encontrado' });
    if (gr.estado !== 'formacion') return res.status(409).json({ error: 'El grupo no está en formación (estado: ' + gr.estado + ')' });

    const cfg = _cfg(g);
    const ints = g.integrantes.filter(i => i.grupoId === gr.id && i.estado !== 'salida');
    if (ints.length < cfg.min) return res.status(409).json({ error: 'Mínimo ' + cfg.min + ' integrantes (hay ' + ints.length + ')' });
    if (ints.length > cfg.max) return res.status(409).json({ error: 'Máximo ' + cfg.max + ' integrantes' });

    // CANDADO DE DOCUMENTOS: rechaza con la lista exacta de quién y qué falta
    const faltan = [];
    for (const i of ints) {
      const exp = g.expedientes.find(e => e.integranteId === i.id);
      const f = exp ? _faltantes(exp, cfg) : cfg.docsObligatorios;
      if (f.length) {
        const cli = (db.clients || []).find(c => c.id === i.clientId) || {};
        faltan.push({ integranteId: i.id, nombre: cli.nombre || i.nombre, faltantes: f });
      }
    }
    if (faltan.length) return res.status(409).json({ error: 'Expedientes incompletos', detalle: faltan });

    // valida montos
    for (const i of ints) if (!(+i.monto > 0)) {
      const cli = (db.clients || []).find(c => c.id === i.clientId) || {};
      return res.status(409).json({ error: 'Falta el monto de ' + (cli.nombre || i.nombre) });
    }

    // crea las N ventas reales en el bloque del tenant
    if (!db.sales) db.sales = [];
    const creadas = [];
    const tarifa = cfg.tarifa || 's16';
    for (const i of ints) {
      const monto = +i.monto;
      const cal = _calcGrupal(monto, gr.frecuencia, cfg);
      const garantia = Math.round(monto * (cfg.garantiaPct / 100));
      const cli = (db.clients || []).find(c => c.id === i.clientId) || {};
      const folio = 'G-' + (1100 + nextId('sales'));
      const sale = {
        id: nextId('sales'), folio, clientId: i.clientId,
        nombre: cli.nombre, tel: cli.tel || '',
        tipo: gr.frecuencia === 'catorcenal' ? tarifa + 'c' : tarifa,
        monto, total: cal.total, pagos: cal.pagos, cuota: cal.cuota,
        plazo: cal.pagos, saldo: cal.total,
        prom: _cobGrupo(gr), sucursalId: gr.sucursalId,
        fecha: hoyMXDDMM(), fechaISO: hoyMXISO(),
        entregado: false,
        // marcas grupales — no rompen ningún filtro individual
        grupoId: gr.id, grupalRol: i.rol, garantiaRetenida: garantia,
        entregaCliente: monto - garantia
      };
      db.sales.push(sale);
      i.saleId = sale.id;
      i.cuota = cal.cuota;
      i.garantia = garantia;
      creadas.push({ integranteId: i.id, saleId: sale.id, folio, cuota: cal.cuota, entregaCliente: sale.entregaCliente, garantia });
    }

    // registra la garantía retenida como CUSTODIA en el flujo (pasivo, no utilidad)
    const totGar = ints.reduce((a, i) => a + (i.garantia || 0), 0);
    if (totGar > 0 && flujoAgregar) {
      flujoAgregar('entrada', 'garantia', cfg.etiquetaRetencion + ' · grupo ' + gr.nombre, totGar,
        { tipo: 'grupo', id: gr.id }, req.user.nombre);
    }

    // arma el registro de ciclo
    const cicloN = gr.ciclo || 1;
    g.ciclos.push({
      id: _nextId(g, 'ciclos'), grupoId: gr.id, numero: cicloN,
      montoAutorizado: ints.reduce((a, i) => a + (+i.monto), 0),
      inicio: hoyMXISO(), fin: null, estado: 'activo',
      integrantes: ints.map(i => i.id),
      juntasAl100: 0, juntasConSolidario: 0
    });

    gr.estado = 'autorizado';
    gr.autorizadoAt = hoyMXISO();
    gr.autorizadoPor = req.user.nombre;
    saveDB();     // persiste las ventas en el bloque del tenant
    _saveG(g);    // persiste el anexo grupal
    logOp('g_autorizar', 'grupo:' + gr.id, { integrantes: ints.length, ventas: creadas.length, garantia: totGar });

    res.json({
      ok: true, grupo: gr.nombre, estado: gr.estado,
      ventasCreadas: creadas, garantiaRetenida: totGar,
      nota: 'Las ' + creadas.length + ' ventas están listas para ENTREGA por el JC (proceso normal de entregas).'
    });
  });

  // ================= JUNTA DEL DÍA (Fase 2) =================

  // helpers de calendario de junta
  const _DIAS = ['domingo', 'lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado'];
  function _hoyDow() {
    // día de la semana en zona MX (hoyMXISO da YYYY-MM-DD)
    const iso = hoyMXISO();
    return new Date(iso + 'T12:00:00').getDay();
  }

  // número de junta actual dentro del ciclo activo del grupo
  function _numJunta(g, gr) {
    const ciclo = g.ciclos.filter(c => c.grupoId === gr.id && c.estado === 'activo').sort((a, b) => b.numero - a.numero)[0];
    if (!ciclo) return { ciclo: null, numero: 0 };
    const hechas = g.juntas.filter(j => j.grupoId === gr.id && j.ciclo === ciclo.numero && j.estado === 'cerrada').length;
    return { ciclo: ciclo.numero, numero: hechas + 1 };
  }

  // AGENDA: juntas de HOY del cobrador (grupos activos cuya junta cae hoy)
  app.get('/api/g/agenda', auth, async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const yo = _normNombre(req.user.nombre);
    const dow = _hoyDow();
    const esJefe = req.user.rol === 'admin' || req.user.rol === 'supervisor';
    let grupos = g.grupos.filter(gr => _estadoVisible(gr) === 'activo');
    if (!esJefe) grupos = grupos.filter(gr => _normNombre(_cobGrupo(gr)) === yo);
    // por defecto solo los de hoy; ?todos=1 trae todos los activos del cobrador
    const soloHoy = req.query.todos !== '1';
    const out = [];
    for (const gr of grupos) {
      const esHoy = (gr.junta && gr.junta.dia === dow);
      if (soloHoy && !esHoy) continue;
      const ints = g.integrantes.filter(i => i.grupoId === gr.id && i.estado === 'activa');
      // ¿ya hay junta abierta o cerrada hoy?
      const hoy = hoyMXISO();
      const juntaHoy = g.juntas.find(j => j.grupoId === gr.id && j.fecha === hoy);
      const nj = _numJunta(g, gr);
      const esperado = ints.reduce((a, i) => a + (i.cuota || 0), 0);
      out.push({
        grupoId: gr.id, nombre: gr.nombre, hora: gr.junta ? gr.junta.hora : '',
        lugar: gr.junta ? gr.junta.lugar : '', lat: gr.junta ? gr.junta.lat : null, lng: gr.junta ? gr.junta.lng : null,
        integrantes: ints.length, esperado: Math.round(esperado),
        cobrador: _cobGrupo(gr), esHoy,
        numeroJunta: nj.numero, ciclo: nj.ciclo,
        juntaEstado: juntaHoy ? juntaHoy.estado : 'pendiente',
        juntaId: juntaHoy ? juntaHoy.id : null
      });
    }
    // orden por hora
    out.sort((a, b) => String(a.hora).localeCompare(String(b.hora)));
    res.json({ fecha: hoyMXISO(), dia: _DIAS[dow], agenda: out, total: out.length });
  });

  // ABRIR junta: devuelve las N cuotas esperadas del grupo (para capturar)
  app.post('/api/g/juntas/abrir', auth, async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const gr = g.grupos.find(x => x.id == req.body.grupoId);
    if (!gr) return res.status(404).json({ error: 'Grupo no encontrado' });
    if (_estadoVisible(gr) !== 'activo') return res.status(409).json({ error: 'El grupo aún no está activo. Falta que el JC entregue los créditos (estado: ' + _estadoVisible(gr) + ')' });
    // control de acceso: cobrador solo su grupo
    if (req.user.rol === 'promotor_grupal' && _normNombre(_cobGrupo(gr)) !== _normNombre(req.user.nombre))
      return res.status(403).json({ error: 'Ese grupo no es de tu ruta' });

    const hoy = hoyMXISO();
    let junta = g.juntas.find(j => j.grupoId === gr.id && j.fecha === hoy);
    const nj = _numJunta(g, gr);
    if (!junta) {
      junta = {
        id: _nextId(g, 'juntas'), grupoId: gr.id, ciclo: nj.ciclo, numero: nj.numero,
        fecha: hoy, esperado: 0, recibido: 0, faltante: 0, detalle: [],
        foto: null, lat: null, lng: null, estado: 'abierta',
        abiertaBy: req.user.nombre, abiertaAt: hoyMXISO()
      };
      g.juntas.push(junta);
      _saveG(g);
    }
    if (junta.estado === 'cerrada') return res.status(409).json({ error: 'La junta de hoy ya fue cerrada' });

    const ints = g.integrantes.filter(i => i.grupoId === gr.id && i.estado === 'activa').map(i => {
      const cli = (db.clients || []).find(c => c.id === i.clientId) || {};
      const saldo = i.saleId != null ? saldoDe(i.saleId) : 0;
      return {
        integranteId: i.id, saleId: i.saleId, nombre: cli.nombre || i.nombre,
        rol: i.rol, cuota: i.cuota, saldo: Math.round(saldo),
        // ¿ya pagó su cuota de esta semana? (heurístico: saldo <= 0 => liquidada)
        liquidada: saldo <= 0.5
      };
    });
    const esperado = ints.reduce((a, i) => a + (i.liquidada ? 0 : i.cuota), 0);
    res.json({
      juntaId: junta.id, grupoId: gr.id, grupo: gr.nombre,
      numeroJunta: junta.numero, ciclo: junta.ciclo,
      integrantes: ints, esperado: Math.round(esperado),
      ahorroSugerido: 0,
      etiquetaRetencion: _cfg(g).etiquetaRetencion
    });
  });

  // PAGO de junta: el asesor captura UN total; el sistema reparte contra las N ventas.
  // Genera solidarios cuando alguien puso de más por otra. Ahorro va a custodia (no baja saldo).
  app.post('/api/g/juntas/:id/pago', auth, async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const junta = g.juntas.find(j => j.id == req.params.id);
    if (!junta) return res.status(404).json({ error: 'Junta no encontrada' });
    if (junta.estado === 'cerrada') return res.status(409).json({ error: 'La junta ya fue cerrada' });
    const gr = g.grupos.find(x => x.id === junta.grupoId);
    if (!gr) return res.status(404).json({ error: 'Grupo no encontrado' });
    if (req.user.rol === 'promotor_grupal' && _normNombre(_cobGrupo(gr)) !== _normNombre(req.user.nombre))
      return res.status(403).json({ error: 'Ese grupo no es de tu ruta' });

    // idempotencia por clave
    const idk = req.body.idempotencyKey;
    if (idk && junta._idem && junta._idem[idk]) return res.json({ ok: true, duplicado: true });

    const { total, detalle } = req.body;
    if (!Array.isArray(detalle) || !detalle.length) return res.status(400).json({ error: 'Falta el detalle del reparto' });

    // valida que la suma capturada cuadre con el total declarado
    const sumaAbonos = detalle.reduce((a, d) => a + (+d.propio || 0) + (+d.solidario || 0), 0);
    const sumaMultas = detalle.reduce((a, d) => a + (+d.multa || 0), 0);
    const sumaAhorro = detalle.reduce((a, d) => a + (+d.ahorro || 0), 0);
    const totalCalc = Math.round((sumaAbonos + sumaMultas + sumaAhorro) * 100) / 100;
    const totalDecl = Math.round((+total || 0) * 100) / 100;
    if (total != null && Math.abs(totalCalc - totalDecl) > 1) {
      return res.status(409).json({ error: 'El total no cuadra con el reparto', totalDeclarado: totalDecl, totalCapturado: totalCalc });
    }

    const cob = _cobGrupo(gr);
    const cobU = (db.users || []).find(u => u.rol === 'promotor_grupal' && _normNombre(u.nombre) === _normNombre(cob));
    const sidCobro = String((cobU && cobU.sucursalId) || gr.sucursalId || 1);
    const fecha = hoyMXDDMM();
    const cfg = _cfg(g);

    // asegura estructuras de caja/porEntregar en el bloque del tenant
    if (!db.porEntregar) db.porEntregar = [];
    if (!db.caja) db.caja = {};
    db.caja[sidCobro] = db.caja[sidCobro] || { inicial: 0, efectivo: 0, banco: 0, entregas: 0, retiros: 0 };

    const resultado = [];
    let recibidoTotal = 0;
    let ahorroTotal = 0;
    for (const d of detalle) {
      const it = g.integrantes.find(i => i.id == d.integranteId && i.grupoId === gr.id);
      if (!it || it.saleId == null) continue;
      const propio = +d.propio || 0;
      const solidario = +d.solidario || 0;
      const multa = +d.multa || 0;
      const ahorro = +d.ahorro || 0;

      // 1) PROPIO: baja el saldo de ESTA integrante
      if (propio > 0) {
        const saldoV = saldoDe(it.saleId);
        const real = Math.min(propio, saldoV);
        if (real > 0) {
          movAdd({
            id: nextId('movimientos'), saleId: it.saleId, fecha,
            concepto: 'Abono junta ' + junta.numero, origen: cob,
            cargo: 0, abono: real, forma: 'efectivo',
            sucursalCobro: +sidCobro, sucursalCredito: +(it.sucursalId || gr.sucursalId || 1),
            grupoId: gr.id, juntaId: junta.id
          });
          let pe = db.porEntregar.find(p => _normNombre(p.prom) === _normNombre(cob) && String(p.sucursalId) === sidCobro);
          if (pe) pe.monto += real; else db.porEntregar.push({ id: nextId('porEntregar'), sucursalId: +sidCobro, prom: cob, monto: real });
          recibidoTotal += real;
        }
      }

      // 2) SOLIDARIO: el efectivo lo pone ESTA integrante, pero baja el saldo del CRÉDITO DEL DEUDOR
      //    (a quien se le está cubriendo). Y queda la deuda interna acreedor→deudor.
      if (solidario > 0 && d.porQuien && d.porQuien != d.integranteId) {
        const deudor = g.integrantes.find(i => i.id == d.porQuien && i.grupoId === gr.id);
        if (deudor && deudor.saleId != null) {
          const saldoD = saldoDe(deudor.saleId);
          const real = Math.min(solidario, saldoD);
          if (real > 0) {
            movAdd({
              id: nextId('movimientos'), saleId: deudor.saleId, fecha,
              concepto: 'Abono solidario junta ' + junta.numero + ' (cubre ' + (it.nombre || '') + ')', origen: cob,
              cargo: 0, abono: real, forma: 'efectivo',
              sucursalCobro: +sidCobro, sucursalCredito: +(deudor.sucursalId || gr.sucursalId || 1),
              grupoId: gr.id, juntaId: junta.id, solidario: true
            });
            let pe = db.porEntregar.find(p => _normNombre(p.prom) === _normNombre(cob) && String(p.sucursalId) === sidCobro);
            if (pe) pe.monto += real; else db.porEntregar.push({ id: nextId('porEntregar'), sucursalId: +sidCobro, prom: cob, monto: real });
            recibidoTotal += real;
          }
          // deuda interna: quien puso (it) es acreedor, el cubierto (deudor) es deudor
          g.solidarios.push({
            id: _nextId(g, 'solidarios'), grupoId: gr.id, juntaId: junta.id, fecha: hoyMXISO(),
            acreedorId: it.id, deudorId: deudor.id, monto: solidario, saldado: false
          });
        }
      }

      // 3) MULTA (retardo/falta): entra a caja como ingreso, no baja saldo del crédito
      if (multa > 0) {
        g.multas.push({
          id: _nextId(g, 'multas'), grupoId: gr.id, juntaId: junta.id, integranteId: it.id,
          tipo: d.multaTipo === 'falta' ? 'falta' : 'retardo', monto: multa, pagada: true, fecha: hoyMXISO()
        });
        // la multa es efectivo real: va también a por entregar
        let pe2 = db.porEntregar.find(p => _normNombre(p.prom) === _normNombre(cob) && String(p.sucursalId) === sidCobro);
        if (pe2) pe2.monto += multa; else db.porEntregar.push({ id: nextId('porEntregar'), sucursalId: +sidCobro, prom: cob, monto: multa });
        recibidoTotal += multa;
      }

      // 4) AHORRO/GARANTÍA en CUSTODIA: efectivo real que la agencia resguarda.
      //    Entra a "por entregar" (para que cuadre el corte) pero NO baja saldo ni cuenta como cobranza.
      if (ahorro > 0) {
        g.ahorro.push({
          id: _nextId(g, 'ahorro'), grupoId: gr.id, juntaId: junta.id, integranteId: it.id,
          fecha: hoyMXISO(), monto: ahorro, tipo: 'aporta'
        });
        let pe3 = db.porEntregar.find(p => _normNombre(p.prom) === _normNombre(cob) && String(p.sucursalId) === sidCobro);
        if (pe3) pe3.monto += ahorro; else db.porEntregar.push({ id: nextId('porEntregar'), sucursalId: +sidCobro, prom: cob, monto: ahorro });
        recibidoTotal += ahorro;
        ahorroTotal += ahorro;
      }

      resultado.push({ integranteId: it.id, propio, solidario, multa, ahorro });
      junta.detalle = junta.detalle.filter(x => x.integranteId !== it.id);
      junta.detalle.push({
        integranteId: it.id, cuota: it.cuota, propio, solidarioRecibido: solidario,
        multa, ahorro, presente: d.presente !== false
      });
    }

    // la custodia (ahorro) se refleja en el flujo como pasivo, no utilidad
    if (ahorroTotal > 0 && flujoAgregar) {
      flujoAgregar('entrada', 'garantia', cfg.etiquetaRetencion + ' · junta ' + junta.numero + ' grupo ' + gr.nombre,
        ahorroTotal, { tipo: 'grupo', id: gr.id }, cob);
    }

    junta.recibido = Math.round((junta.recibido || 0) + recibidoTotal);
    if (idk) { junta._idem = junta._idem || {}; junta._idem[idk] = 1; }
    saveDB();   // persiste movimientos/porEntregar/caja del tenant
    _saveG(g);  // persiste el anexo
    logOp('g_junta_pago', 'junta:' + junta.id, { grupoId: gr.id, recibido: Math.round(recibidoTotal), ahorro: ahorroTotal, n: resultado.length });

    res.json({ ok: true, juntaId: junta.id, recibido: Math.round(recibidoTotal), reparto: resultado });
  });

  // CERRAR junta: foto, GPS, calcula faltante, marca al 100% o con solidario
  app.post('/api/g/juntas/:id/cerrar', auth, async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const junta = g.juntas.find(j => j.id == req.params.id);
    if (!junta) return res.status(404).json({ error: 'Junta no encontrada' });
    if (junta.estado === 'cerrada') return res.status(409).json({ error: 'La junta ya está cerrada' });
    const gr = g.grupos.find(x => x.id === junta.grupoId);
    if (!gr) return res.status(404).json({ error: 'Grupo no encontrado' });
    if (req.user.rol === 'promotor_grupal' && _normNombre(_cobGrupo(gr)) !== _normNombre(req.user.nombre))
      return res.status(403).json({ error: 'Ese grupo no es de tu ruta' });

    const { foto, lat, lng } = req.body;
    if (foto) junta.foto = await fotoGuardar(foto, 'grupal:junta:' + junta.id);
    if (lat != null) junta.lat = lat;
    if (lng != null) junta.lng = lng;

    // esperado de la semana = suma de cuotas de integrantes activas
    const ints = g.integrantes.filter(i => i.grupoId === gr.id && i.estado === 'activa');
    const esperado = ints.reduce((a, i) => a + (i.cuota || 0), 0);
    const abonado = (junta.detalle || []).reduce((a, d) => a + (d.propio || 0) + (d.solidarioRecibido || 0), 0);
    junta.esperado = Math.round(esperado);
    junta.faltante = Math.max(0, Math.round(esperado - abonado));
    junta.estado = 'cerrada';
    junta.cerradaBy = req.user.nombre;
    junta.cerradaAt = new Date().toISOString();

    // actualiza métricas del ciclo: ¿al 100%? ¿hubo solidario?
    const huboSolidario = (junta.detalle || []).some(d => (d.solidarioRecibido || 0) > 0);
    const ciclo = g.ciclos.filter(c => c.grupoId === gr.id && c.estado === 'activo').sort((a, b) => b.numero - a.numero)[0];
    if (ciclo) {
      if (junta.faltante <= 0.5) ciclo.juntasAl100 = (ciclo.juntasAl100 || 0) + 1;
      if (huboSolidario) ciclo.juntasConSolidario = (ciclo.juntasConSolidario || 0) + 1;
    }
    _saveG(g);
    logOp('g_junta_cerrar', 'junta:' + junta.id, { grupoId: gr.id, esperado: junta.esperado, faltante: junta.faltante, solidario: huboSolidario });
    res.json({ ok: true, esperado: junta.esperado, faltante: junta.faltante, al100: junta.faltante <= 0.5, huboSolidario });
  });

  // SOLIDARIOS vivos de un grupo (quién le debe a quién)
  app.get('/api/g/grupos/:id/solidarios', auth, async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const gr = g.grupos.find(x => x.id == req.params.id);
    if (!gr) return res.status(404).json({ error: 'Grupo no encontrado' });
    const nombre = (id) => { const it = g.integrantes.find(i => i.id === id); if (!it) return '—'; const c = (db.clients || []).find(x => x.id === it.clientId) || {}; return c.nombre || it.nombre || '—'; };
    const vivos = g.solidarios.filter(s => s.grupoId === gr.id && !s.saldado)
      .map(s => ({ id: s.id, fecha: s.fecha, acreedor: nombre(s.acreedorId), deudor: nombre(s.deudorId), monto: s.monto }));
    res.json({ solidarios: vivos, total: vivos.length });
  });

  app.post('/api/g/solidarios/:id/saldar', auth, rol('admin', 'supervisor', 'sucursal', 'promotor_grupal'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const s = g.solidarios.find(x => x.id == req.params.id);
    if (!s) return res.status(404).json({ error: 'Registro no encontrado' });
    s.saldado = true; s.saldadoAt = new Date().toISOString();
    _saveG(g);
    logOp('g_solidario_saldar', 'solidario:' + s.id, {});
    res.json({ ok: true });
  });

  // ================= DOCUMENTOS: CONTRATO Y PAGARÉS (mutuo con obligación solidaria) =================

  function _fechaLarga(iso) {
    const m = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre'];
    const d = new Date((iso || hoyMXISO()) + 'T12:00:00');
    return d.getDate() + ' de ' + m[d.getMonth()] + ' de ' + d.getFullYear();
  }
  function _frecTxt(f) { return f === 'catorcenal' ? 'catorcenal' : 'semanal'; }
  function _esc(s) { return String(s == null ? '' : s).replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c])); }

  // auth que acepta token por header O por ?t= (window.open no manda headers).
  // Corre el handler dentro del ALS del tenant, igual que el auth normal.
  async function authDoc(req, res, next) {
    const t = (req.headers.authorization || '').replace('Bearer ', '') || req.query.t || '';
    let payload;
    try { payload = jwt.verify(t, JWT_SECRET); } catch { return res.status(401).send('No autorizado'); }
    req.user = payload;
    if (payload.tenantId == null) return res.status(403).send('Sin agencia');
    try {
      const blob = await getTenant(payload.tenantId);
      if (!blob) return res.status(401).send('Agencia no encontrada');
      return als.run({ tenantId: +payload.tenantId, db: blob }, () => next());
    } catch (e) { return res.status(500).send('Error'); }
  }

  // Genera el paquete de documentos del grupo: 1 contrato grupal + N pagarés.
  // Devuelve HTML listo para imprimir. Cláusulas MÍNIMAS de un mutuo con obligación solidaria.
  app.get('/api/g/grupos/:id/documentos', authDoc, async (req, res) => {
    const g = await G(); if (!g) return res.status(400).send('Sin agencia');
    const gr = g.grupos.find(x => x.id == req.params.id);
    if (!gr) return res.status(404).send('Grupo no encontrado');
    const ints = g.integrantes.filter(i => i.grupoId === gr.id && i.estado === 'activa');
    if (!ints.length) return res.status(409).send('El grupo no tiene integrantes');

    const cfg = _cfg(g);
    const marca = (db.config && db.config.brand && db.config.brand.nombre) || 'La Acreedora';
    const suc = (db.sucursales || []).find(s => s.id === gr.sucursalId);
    const lugar = (gr.junta && gr.junta.lugar) || (suc && suc.nombre) || '';
    const fechaHoy = _fechaLarga(hoyMXISO());
    const etq = cfg.etiquetaRetencion || 'Garantía líquida';

    const datos = ints.map(i => {
      const cli = (db.clients || []).find(c => c.id === i.clientId) || {};
      const monto = i.monto || 0;
      const cal = _calcGrupal(monto, gr.frecuencia, cfg);
      const gar = i.garantia || Math.round(monto * (cfg.garantiaPct / 100));
      return {
        nombre: cli.nombre || i.nombre || '—',
        domicilio: [cli.calle, cli.col, cli.ciudad, cli.estado].filter(Boolean).join(', ') || '________________',
        rol: i.rol, monto, total: cal.total, cuota: cal.cuota, pagos: cal.pagos,
        garantia: gar, entrega: monto - gar
      };
    });
    const totalPrestado = datos.reduce((a, d) => a + d.monto, 0);
    const nombres = datos.map(d => d.nombre).join('; ');

    // ---------- CONTRATO GRUPAL ----------
    let contrato = `
<div class="doc">
  <h1>CONTRATO DE MUTUO CON INTERÉS Y OBLIGACIÓN SOLIDARIA</h1>
  <p>En ${_esc(lugar)}, a ${fechaHoy}, celebran el presente contrato: por una parte <b>${_esc(marca)}</b> (en adelante <b>LA ACREEDORA</b>); y por la otra, las personas que integran el grupo denominado <b>"${_esc(gr.nombre)}"</b> (en adelante <b>LAS DEUDORAS</b>), a saber: ${_esc(nombres)}.</p>

  <h2>DECLARACIONES</h2>
  <p>Declaran LAS DEUDORAS ser mayores de edad, con capacidad legal para obligarse, y haber recibido de LA ACREEDORA las cantidades que a cada una se señalan en la cláusula PRIMERA, a su entera satisfacción.</p>

  <h2>CLÁUSULAS</h2>
  <p><b>PRIMERA. Monto.</b> LA ACREEDORA entrega a cada integrante, en calidad de mutuo, la cantidad individual señalada en la tabla siguiente, sumando un total prestado de <b>$${totalPrestado.toLocaleString('es-MX')}</b>:</p>
  <table>
    <tr><th>Integrante</th><th>Cargo</th><th>Monto</th><th>Garantía</th><th>Recibe</th><th>Cuota ${_frecTxt(gr.frecuencia)}</th></tr>
    ${datos.map(d => `<tr><td>${_esc(d.nombre)}</td><td>${_esc(d.rol)}</td><td>$${d.monto.toLocaleString('es-MX')}</td><td>$${d.garantia.toLocaleString('es-MX')}</td><td>$${d.entrega.toLocaleString('es-MX')}</td><td>$${Math.round(d.cuota).toLocaleString('es-MX')}</td></tr>`).join('')}
  </table>

  <p><b>SEGUNDA. Interés y forma de pago.</b> Cada integrante pagará su crédito en <b>${datos[0].pagos} pagos ${_frecTxt(gr.frecuencia)}es</b>, mediante la cuota señalada, que comprende capital e intereses ya pactados. El pago se realiza en la junta del grupo.</p>

  <p><b>TERCERA. Lugar y fecha de pago.</b> Los pagos se entregan en ${_esc(lugar)}${gr.junta && gr.junta.dia != null ? ', los días ' + _DIAS[gr.junta.dia] : ''}${gr.junta && gr.junta.hora ? ' a las ' + gr.junta.hora + ' horas' : ''}, iniciando a partir de la fecha de entrega y hasta la total liquidación.</p>

  <p><b>CUARTA. Obligación solidaria.</b> LAS DEUDORAS se constituyen en <b>obligadas solidarias</b> entre sí respecto de la totalidad de los créditos otorgados a las integrantes del grupo. En caso de que alguna no cubra su pago, las demás responderán por el adeudo en la proporción que corresponda, sin necesidad de requerimiento previo, en términos de los artículos aplicables del Código Civil en materia de solidaridad pasiva.</p>

  <p><b>QUINTA. ${_esc(etq)}.</b> LA ACREEDORA retiene de cada integrante la cantidad señalada como ${_esc(etq)}, que garantiza el cumplimiento de las obligaciones del grupo. Podrá aplicarse a cubrir adeudos vencidos; el remanente se devuelve al liquidar totalmente el crédito.</p>

  <p><b>SEXTA. Vencimiento anticipado.</b> La falta de pago de una o más cuotas faculta a LA ACREEDORA a exigir el saldo total insoluto de la integrante y, en su caso, del grupo por su obligación solidaria.</p>

  <p><b>SÉPTIMA. Consentimiento.</b> Leído el presente y enteradas de su contenido y alcance legal, las partes lo firman de conformidad.</p>

  <div class="firmas">
    <div class="firma"><div class="linea"></div>LA ACREEDORA<br>${_esc(marca)}</div>
    ${datos.map(d => `<div class="firma"><div class="linea"></div>${_esc(d.nombre)}<br><span class="mini">${_esc(d.rol)}</span></div>`).join('')}
  </div>
</div>`;

    // ---------- PAGARÉS INDIVIDUALES ----------
    const pagares = datos.map(d => `
<div class="doc pagare">
  <h1>PAGARÉ</h1>
  <p class="folio">Grupo "${_esc(gr.nombre)}" · ${_esc(d.nombre)}</p>
  <p>Debo(emos) y pagaré(mos) incondicionalmente a la orden de <b>${_esc(marca)}</b> la cantidad de <b>$${d.total.toLocaleString('es-MX')}</b> (${_numLetra(d.total)}), en ${d.pagos} pagos ${_frecTxt(gr.frecuencia)}es de $${Math.round(d.cuota).toLocaleString('es-MX')} cada uno, en ${_esc(lugar)}, a partir del ${fechaHoy}.</p>
  <p>Este pagaré forma parte del contrato de mutuo con obligación solidaria del grupo, causando intereses moratorios en caso de falta de pago oportuno. Valor recibido a mi entera satisfacción.</p>
  <div class="firma-pagare"><div class="linea"></div>${_esc(d.nombre)}<br><span class="mini">${_esc(d.domicilio)}</span></div>
</div>`).join('');

    const html = `<!DOCTYPE html><html lang="es-MX"><head><meta charset="UTF-8"><title>Documentos · ${_esc(gr.nombre)}</title>
<style>
  body{font-family:Georgia,'Times New Roman',serif;color:#111;max-width:720px;margin:0 auto;padding:24px;line-height:1.5;font-size:13px}
  .doc{page-break-after:always;padding:20px 0}
  h1{font-size:16px;text-align:center;letter-spacing:.5px;margin-bottom:16px;text-transform:uppercase}
  h2{font-size:13px;margin:16px 0 6px;border-bottom:1px solid #999;padding-bottom:3px}
  p{margin:8px 0;text-align:justify}
  table{width:100%;border-collapse:collapse;margin:10px 0;font-size:11px}
  th,td{border:1px solid #999;padding:5px 7px;text-align:left}
  th{background:#eee}
  .firmas{display:flex;flex-wrap:wrap;gap:24px;margin-top:40px;justify-content:space-around}
  .firma{text-align:center;font-size:11px;min-width:180px;margin-bottom:20px}
  .firma .linea{border-top:1px solid #111;margin-bottom:5px;height:1px;width:190px}
  .mini{color:#666;font-size:10px}
  .pagare{border:2px solid #111;padding:22px;margin-bottom:24px}
  .pagare .folio{text-align:center;color:#555;font-size:11px;margin-bottom:14px}
  .firma-pagare{text-align:center;margin-top:44px}
  .firma-pagare .linea{border-top:1px solid #111;width:240px;margin:0 auto 5px}
  @media print{.noprint{display:none}body{padding:0}}
  .noprint{position:fixed;top:10px;right:10px;background:#178a55;color:#fff;border:0;padding:10px 18px;border-radius:8px;font-family:sans-serif;cursor:pointer;font-size:14px}
</style></head><body>
<button class="noprint" onclick="window.print()">🖨 Imprimir</button>
${contrato}
${pagares}
</body></html>`;

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(html);
  });

  // convierte número a letra (simplificado para montos comunes de crédito)
  function _numLetra(n) {
    n = Math.round(n);
    const U = ['', 'un', 'dos', 'tres', 'cuatro', 'cinco', 'seis', 'siete', 'ocho', 'nueve', 'diez', 'once', 'doce', 'trece', 'catorce', 'quince', 'dieciséis', 'diecisiete', 'dieciocho', 'diecinueve', 'veinte'];
    const D = ['', '', 'veinti', 'treinta', 'cuarenta', 'cincuenta', 'sesenta', 'setenta', 'ochenta', 'noventa'];
    const C = ['', 'ciento', 'doscientos', 'trescientos', 'cuatrocientos', 'quinientos', 'seiscientos', 'setecientos', 'ochocientos', 'novecientos'];
    function tres(x) {
      if (x === 0) return '';
      if (x === 100) return 'cien';
      let r = '';
      const c = Math.floor(x / 100), d = Math.floor((x % 100) / 10), u = x % 10, du = x % 100;
      if (c) r += C[c] + ' ';
      if (du <= 20) r += U[du];
      else if (du < 30) r += 'veinti' + U[u];
      else { r += D[d]; if (u) r += ' y ' + U[u]; }
      return r.trim();
    }
    if (n === 0) return 'cero pesos';
    let r = '';
    const millones = Math.floor(n / 1000000), miles = Math.floor((n % 1000000) / 1000), resto = n % 1000;
    if (millones) r += (millones === 1 ? 'un millón ' : tres(millones) + ' millones ');
    if (miles) r += (miles === 1 ? 'mil ' : tres(miles) + ' mil ');
    if (resto) r += tres(resto);
    return r.trim() + ' pesos 00/100 M.N.';
  }

  // ================= CICLO: RENOVACIÓN Y CIERRE (Fase 3) =================

  // ¿todas las ventas del grupo están liquidadas? (saldo <= 0)
  function _grupoLiquidado(gr, g) {
    const ints = g.integrantes.filter(i => i.grupoId === gr.id && i.estado === 'activa');
    if (!ints.length) return false;
    return ints.every(i => i.saleId == null || saldoDe(i.saleId) <= 0.5);
  }

  // estado de cierre: cuánto falta para poder renovar
  app.get('/api/g/grupos/:id/cierre', auth, async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const gr = g.grupos.find(x => x.id == req.params.id);
    if (!gr) return res.status(404).json({ error: 'Grupo no encontrado' });
    const ints = g.integrantes.filter(i => i.grupoId === gr.id && i.estado === 'activa');
    const det = ints.map(i => {
      const cli = (db.clients || []).find(c => c.id === i.clientId) || {};
      const saldo = i.saleId != null ? saldoDe(i.saleId) : 0;
      return { integranteId: i.id, nombre: cli.nombre || i.nombre, saldo: Math.round(saldo), liquidada: saldo <= 0.5, garantia: i.garantia || 0 };
    });
    const solVivos = g.solidarios.filter(s => s.grupoId === gr.id && !s.saldado);
    const cicloAct = g.ciclos.filter(c => c.grupoId === gr.id && c.estado === 'activo').sort((a, b) => b.numero - a.numero)[0] || null;
    const puedeRenovar = det.every(d => d.liquidada) && solVivos.length === 0;
    // monto propuesto del siguiente ciclo por integrante (escala 20%)
    const cfg = _cfg(g);
    const siguiente = det.map(d => {
      const it = ints.find(x => x.id === d.integranteId);
      return { integranteId: d.integranteId, nombre: d.nombre, montoActual: it.monto, montoSiguiente: _montoCiclo(it.monto, (gr.ciclo || 1) + 1, cfg.escalaPct) };
    });
    res.json({
      grupo: gr.nombre, ciclo: gr.ciclo, integrantes: det,
      solidariosVivos: solVivos.length, puedeRenovar,
      garantiaTotal: det.reduce((a, d) => a + d.garantia, 0),
      siguienteCiclo: siguiente,
      pendiente: det.filter(d => !d.liquidada).map(d => d.nombre)
    });
  });

  // CERRAR ciclo (liquidado): devuelve remanente de garantía como custodia_devuelta
  app.post('/api/g/grupos/:id/cerrar', auth, rol('admin', 'supervisor'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const gr = g.grupos.find(x => x.id == req.params.id);
    if (!gr) return res.status(404).json({ error: 'Grupo no encontrado' });
    if (!_grupoLiquidado(gr, g)) return res.status(409).json({ error: 'No todas las integrantes han liquidado' });
    const solVivos = g.solidarios.filter(s => s.grupoId === gr.id && !s.saldado);
    if (solVivos.length && !req.body.forzar) return res.status(409).json({ error: 'Hay ' + solVivos.length + ' deuda(s) solidaria(s) sin saldar', solidarios: solVivos.length });

    const ints = g.integrantes.filter(i => i.grupoId === gr.id && i.estado === 'activa');
    const totGar = ints.reduce((a, i) => a + (i.garantia || 0), 0);
    // devuelve la garantía retenida (custodia sale)
    if (totGar > 0 && flujoAgregar) {
      flujoAgregar('salida', 'garantia_devuelta', _cfg(g).etiquetaRetencion + ' devuelta · grupo ' + gr.nombre, totGar, { tipo: 'grupo', id: gr.id }, req.user.nombre);
    }
    // cierra el ciclo activo
    const ciclo = g.ciclos.filter(c => c.grupoId === gr.id && c.estado === 'activo').sort((a, b) => b.numero - a.numero)[0];
    if (ciclo) { ciclo.estado = 'liquidado'; ciclo.fin = hoyMXISO(); }
    gr.estado = 'liquidado';
    gr.liquidadoAt = hoyMXISO();
    _saveG(g);
    logOp('g_cerrar_ciclo', 'grupo:' + gr.id, { ciclo: gr.ciclo, garantiaDevuelta: totGar });
    res.json({ ok: true, garantiaDevuelta: totGar, estado: 'liquidado' });
  });

  // RENOVAR: liquida ciclo actual, escala 20%, arma ciclo n+1 con quien renueva.
  // Crea nuevas ventas (nuevos créditos) para las integrantes que continúan.
  app.post('/api/g/grupos/:id/renovar', auth, rol('admin', 'supervisor'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const gr = g.grupos.find(x => x.id == req.params.id);
    if (!gr) return res.status(404).json({ error: 'Grupo no encontrado' });
    if (!_grupoLiquidado(gr, g)) return res.status(409).json({ error: 'No todas han liquidado el ciclo actual' });
    const solVivos = g.solidarios.filter(s => s.grupoId === gr.id && !s.saldado);
    if (solVivos.length) return res.status(409).json({ error: 'Salda las ' + solVivos.length + ' deuda(s) solidaria(s) antes de renovar' });

    // quiénes renuevan: por defecto todas las activas; req.body.excluir = [integranteId] las saca
    const excluir = new Set((req.body.excluir || []).map(Number));
    const cfg = _cfg(g);
    const nuevoCiclo = (gr.ciclo || 1) + 1;
    const activas = g.integrantes.filter(i => i.grupoId === gr.id && i.estado === 'activa');
    const renuevan = activas.filter(i => !excluir.has(i.id));
    if (renuevan.length < cfg.min) return res.status(409).json({ error: 'Con ' + renuevan.length + ' integrantes no se alcanza el mínimo de ' + cfg.min });

    // cierra ciclo actual
    const cicloAct = g.ciclos.filter(c => c.grupoId === gr.id && c.estado === 'activo').sort((a, b) => b.numero - a.numero)[0];
    if (cicloAct) { cicloAct.estado = 'liquidado'; cicloAct.fin = hoyMXISO(); }

    // las excluidas salen del grupo (devuelve su garantía)
    let garDevuelta = 0;
    for (const i of activas) {
      if (excluir.has(i.id)) {
        i.estado = 'salida'; i.baja = hoyMXISO(); i.motivoBaja = req.body.motivo || 'no renovó';
        garDevuelta += (i.garantia || 0);
      }
    }
    if (garDevuelta > 0 && flujoAgregar) {
      flujoAgregar('salida', 'garantia_devuelta', cfg.etiquetaRetencion + ' devuelta (no renovaron) · ' + gr.nombre, garDevuelta, { tipo: 'grupo', id: gr.id }, req.user.nombre);
    }

    // crea ventas nuevas del ciclo n+1 (escala 20%), retiene garantía nueva
    if (!db.sales) db.sales = [];
    const creadas = [];
    const tarifa = cfg.tarifa || 's16';
    let garNueva = 0;
    for (const i of renuevan) {
      const monto = _montoCiclo(i.monto, nuevoCiclo, cfg.escalaPct);
      const cal = _calcGrupal(monto, gr.frecuencia, cfg);
      const garantia = Math.round(monto * (cfg.garantiaPct / 100));
      const cli = (db.clients || []).find(c => c.id === i.clientId) || {};
      const folio = 'G-' + (1100 + nextId('sales'));
      const sale = {
        id: nextId('sales'), folio, clientId: i.clientId, nombre: cli.nombre, tel: cli.tel || '',
        tipo: gr.frecuencia === 'catorcenal' ? tarifa + 'c' : tarifa,
        monto, total: cal.total, pagos: cal.pagos, cuota: cal.cuota, plazo: cal.pagos, saldo: cal.total,
        prom: _cobGrupo(gr), sucursalId: gr.sucursalId, fecha: hoyMXDDMM(), fechaISO: hoyMXISO(),
        entregado: false, grupoId: gr.id, grupalRol: i.rol, grupalCiclo: nuevoCiclo,
        garantiaRetenida: garantia, entregaCliente: monto - garantia
      };
      db.sales.push(sale);
      i.saleId = sale.id; i.monto = monto; i.cuota = cal.cuota; i.garantia = garantia; i.ciclo = nuevoCiclo;
      garNueva += garantia;
      creadas.push({ integranteId: i.id, saleId: sale.id, folio, monto, cuota: cal.cuota, garantia });
    }
    if (garNueva > 0 && flujoAgregar) {
      flujoAgregar('entrada', 'garantia', cfg.etiquetaRetencion + ' · ciclo ' + nuevoCiclo + ' grupo ' + gr.nombre, garNueva, { tipo: 'grupo', id: gr.id }, req.user.nombre);
    }

    g.ciclos.push({
      id: _nextId(g, 'ciclos'), grupoId: gr.id, numero: nuevoCiclo,
      montoAutorizado: renuevan.reduce((a, i) => a + i.monto, 0),
      inicio: hoyMXISO(), fin: null, estado: 'activo',
      integrantes: renuevan.map(i => i.id), juntasAl100: 0, juntasConSolidario: 0
    });
    gr.ciclo = nuevoCiclo;
    gr.estado = 'autorizado';   // vuelve a esperar entrega del JC
    saveDB(); _saveG(g);
    logOp('g_renovar', 'grupo:' + gr.id, { ciclo: nuevoCiclo, renuevan: renuevan.length, salieron: excluir.size, garantiaNueva: garNueva });
    res.json({ ok: true, ciclo: nuevoCiclo, ventasCreadas: creadas, garantiaNueva: garNueva, garantiaDevuelta: garDevuelta,
      nota: creadas.length + ' créditos del ciclo ' + nuevoCiclo + ' listos para entrega por el JC.' });
  });

  // ================= REPORTES (Fase 3) =================

  // semáforo por grupo y por asesor
  app.get('/api/g/reportes/grupos', auth, rol('admin', 'supervisor', 'sucursal'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    let grupos = g.grupos.filter(gr => gr.estado !== 'formacion');
    if (req.user.rol === 'sucursal') grupos = grupos.filter(gr => gr.sucursalId == req.user.sucursalId);
    const porGrupo = grupos.map(gr => {
      const ints = g.integrantes.filter(i => i.grupoId === gr.id && i.estado === 'activa');
      const cartera = ints.reduce((a, i) => a + (i.saleId != null ? Math.max(0, saldoDe(i.saleId)) : 0), 0);
      const solVivos = g.solidarios.filter(s => s.grupoId === gr.id && !s.saldado).length;
      const cicloAct = g.ciclos.filter(c => c.grupoId === gr.id && c.estado === 'activo').sort((a, b) => b.numero - a.numero)[0] || null;
      const j100 = cicloAct ? (cicloAct.juntasAl100 || 0) : 0;
      const jSol = cicloAct ? (cicloAct.juntasConSolidario || 0) : 0;
      let sem = 'verde'; if (solVivos >= 3) sem = 'rojo'; else if (solVivos >= 1) sem = 'ambar';
      return {
        grupoId: gr.id, nombre: gr.nombre, asesor: _cobGrupo(gr), sucursalId: gr.sucursalId,
        estado: _estadoVisible(gr), ciclo: gr.ciclo, integrantes: ints.length,
        cartera: Math.round(cartera), solidariosVivos: solVivos,
        juntasAl100: j100, juntasConSolidario: jSol, semaforo: sem
      };
    });
    // agregado por asesor
    const porAsesor = {};
    porGrupo.forEach(x => {
      const k = x.asesor || '—';
      porAsesor[k] = porAsesor[k] || { asesor: k, grupos: 0, integrantes: 0, cartera: 0, solidariosVivos: 0, rojos: 0 };
      porAsesor[k].grupos++; porAsesor[k].integrantes += x.integrantes; porAsesor[k].cartera += x.cartera;
      porAsesor[k].solidariosVivos += x.solidariosVivos; if (x.semaforo === 'rojo') porAsesor[k].rojos++;
    });
    res.json({ grupos: porGrupo, asesores: Object.values(porAsesor), total: porGrupo.length });
  });

  // comisiones grupales por asesor (tres conceptos configurables)
  app.get('/api/g/reportes/comisiones', auth, rol('admin', 'supervisor'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const cfg = _cfg(g); const com = cfg.comision || {};
    // por asesor: cobranza de la semana (movimientos grupales) + grupos cerrados + puntualidad
    const porAsesor = {};
    for (const gr of g.grupos) {
      const k = _cobGrupo(gr) || '—';
      porAsesor[k] = porAsesor[k] || { asesor: k, cobranza: 0, gruposCerrados: 0, juntasAl100: 0, juntasTotal: 0, comision: 0 };
      // cobranza grupal: abonos de movimientos con grupoId de este grupo
      const ints = g.integrantes.filter(i => i.grupoId === gr.id);
      const saleIds = new Set(ints.map(i => i.saleId).filter(x => x != null));
      const cob = (db.movimientos || []).filter(m => m.grupoId === gr.id || (m.saleId != null && saleIds.has(m.saleId) && m.abono > 0))
        .reduce((a, m) => a + (m.abono || 0), 0);
      porAsesor[k].cobranza += cob;
      const cicloAct = g.ciclos.filter(c => c.grupoId === gr.id && c.estado === 'activo').sort((a, b) => b.numero - a.numero)[0];
      if (cicloAct) { porAsesor[k].juntasAl100 += (cicloAct.juntasAl100 || 0); }
      if (gr.estado === 'liquidado') porAsesor[k].gruposCerrados++;
    }
    const out = Object.values(porAsesor).map(a => {
      const c1 = (com.sobreCobranza || 0) / 100 * a.cobranza;
      const c2 = (com.porGrupoCerrado || 0) * a.gruposCerrados;
      const c3 = (com.porPuntualidad || 0) * a.juntasAl100;
      a.comision = Math.round(c1 + c2 + c3);
      a.cobranza = Math.round(a.cobranza);
      a.detalle = { sobreCobranza: Math.round(c1), porGrupoCerrado: Math.round(c2), porPuntualidad: Math.round(c3) };
      return a;
    });
    res.json({ asesores: out, config: com });
  });

  // ================= CONFIG =================
  app.get('/api/g/config', auth, rol('admin', 'supervisor'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    res.json({ config: _cfg(g) });
  });
  app.post('/api/g/config', auth, rol('admin'), async (req, res) => {
    const g = await G(); if (!g) return res.status(400).json({ error: 'Sin agencia' });
    const c = _cfg(g);
    const b = req.body || {};
    ['tarifa', 'etiquetaRetencion'].forEach(k => { if (b[k] != null) c[k] = b[k]; });
    ['garantiaPct', 'escalaPct', 'min', 'max', 'multaRetardo', 'multaFalta'].forEach(k => { if (b[k] != null) c[k] = +b[k]; });
    if (b.comision) c.comision = Object.assign(c.comision || {}, b.comision);
    if (Array.isArray(b.docsObligatorios)) c.docsObligatorios = b.docsObligatorios;
    // sanidad
    if (c.min < 3) c.min = 3;
    if (c.max < c.min) c.max = c.min;
    _saveG(g);
    logOp('g_config', 'config', {});
    res.json({ ok: true, config: c });
  });

  // salud del módulo
  app.get('/api/g/salud', auth, async (req, res) => {
    const g = await G();
    res.json({ ok: true, flag: FLAG, anexoId: _gid(), grupos: g ? g.grupos.length : 0 });
  });

  // lista de promotores grupales (para relevo y asignación)
  app.get('/api/g/promotores', auth, rol('admin', 'supervisor', 'sucursal'), async (req, res) => {
    const sucMap = {}; (db.sucursales || []).forEach(s => sucMap[s.id] = s.nombre);
    const esSuc = req.user.rol === 'sucursal';
    const lista = (db.users || [])
      .filter(u => u.rol === 'promotor_grupal' && u.activo && (!esSuc || u.sucursalId === req.user.sucursalId))
      .map(u => ({ id: u.id, nombre: u.nombre, sucursalId: u.sucursalId, sucursal: sucMap[u.sucursalId] || null }));
    res.json(lista);
  });
};
