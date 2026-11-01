# Base de datos de WM_Central

Solo WM_Central tiene base de datos: un fichero **SQLite** cuya ruta llega por la variable de entorno `WM_DB_PATH` (por defecto, `watermanagement.db` en la carpeta desde la que se lanza; en Docker, `/data/watermanagement.db`, dentro de un volumen).

Toda la base de datos se maneja desde la clase `DatabaseManager`. Las tablas se declaran `STRICT` y los valores que llegan de fuera van siempre en un `PreparedStatement`.

## Tabla `estaciones`

Una fila por cada estación dada de alta.

| Campo | Tipo | Restricción | Contenido |
|---|---|---|---|
| `id` | TEXT | PRIMARY KEY | ID de la estación (por ejemplo, `WS-01`) |
| `ubicacion` | TEXT | NOT NULL | Parque o jardín; llega en el `ALTA` del Monitor |
| `estado` | TEXT | NOT NULL | Último estado conocido: `DISPONIBLE`, `REGANDO`, `FUGA` o `FUERA_DE_SERVICIO` |

- `DESCONECTADA` no se guarda nunca: al arrancar Central, todas las estaciones se cargan como desconectadas hasta que su Monitor se autentica.
- Si una estación está bloqueada, se guarda siempre `FUERA_DE_SERVICIO`, aunque en ese momento tenga una fuga. Así el bloqueo sobrevive a los reinicios.

## Tabla `operarios`

| Campo | Tipo | Restricción | Contenido |
|---|---|---|---|
| `id` | TEXT | PRIMARY KEY | ID del operario (por ejemplo, `OP01`) |

Cada operario se da de alta solo con su primera petición (`INSERT OR IGNORE`). `CENTRAL` y `WS` no se guardan: son los identificadores de los riegos pedidos desde el menú de Central y desde la propia estación.

## Lo que no se guarda

- El riego en curso: si Central se reinicia, lo reconstruye con el siguiente mensaje `DATOS` del Engine.
- Los litros suministrados que muestra el panel, que cuentan desde el arranque de Central.