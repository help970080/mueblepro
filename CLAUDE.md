# MueblePro — reglas para Claude

Venta de muebles a crédito, multi-tenant. Clon de CobraPro (help970080/CrediaProMax).
Dueño: Leo (LeGaXi Tech). Responder siempre en español, al grano.

## Estructura
- `server.js` + `grupal.js` (Node/Express + PostgreSQL en Render). `public/` = frontends JS vanilla.
- Reglas de negocio: enganche se descuenta antes del crédito (Modelo A); la cobranza corre desde la fecha de entrega; el stock baja de la sucursal vendedora.
- Fotos fuera del blob JSON (tabla `mp_fotos`, `FLAG_FOTOS=1`).
- El servicio en Render está APAGADO mientras no haya clientes; la base del blueprint es plan free (expira ~30 días): no meter datos reales ahí.

## Antes de entregar cualquier cambio
1. Diagnosticar y reproducir antes de tocar código. Un arreglo verificado, no diez sugerencias.
2. `node --check` en TODO archivo JS tocado (incluye `public/*.js`).
3. Diff contra `origin/main` y confirmar cero pérdidas (funciones, rutas, campos).
4. Revisión tipo /review: bugs de lógica, casos no contemplados, regresiones.
5. Rutas nuevas o cambios de seguridad: aislamiento de tenant (todo query filtrado por tenant), SQL parametrizado, auth/JWT por rol, sin secretos en el código, XSS (también en atributos), frenos de fuerza bruta que no dependan de `X-Forwarded-For`.
6. Cambios grandes: probar a tamaño celular los flujos tocados (la mayoría usa celular).
7. Entregar archivos completos, no diffs.

## Deploy
- Push a `main` = autodeploy en Render. Leo decide cuándo sube `server.js`; los HTML se pueden subir cuando sea.
- Después del deploy verificar `/api/health`. No quedarse esperando a Render.
- No reusar secretos de CobraPro (`JWT_SECRET`, `SUPER_PASS`).
