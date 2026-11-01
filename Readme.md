
    # WaterManagement · Release 1

Práctica de **Sistemas Distribuidos** (Universidad de Alicante, curso 2026/27): simulación de la gestión centralizada del riego de los parques y jardines de una ciudad.

**Integrantes:** Carlos Ichim y Osama Etalai Talea.

## Módulos

| Módulo | Carpeta | Qué hace |
|---|---|---|
| WM_Central | `WM_Central` | Registra y autentica las estaciones, autoriza los riegos, envía órdenes y sirve el panel web. Base de datos SQLite |
| WM_WS_M (Monitor) | `WM_WS/WS_M` | Autentica la estación en Central y le informa cada segundo de la salud de su Engine (sockets) |
| WM_WS_E (Engine) | `WM_WS/WS_E` | Riega, simula el caudal, publica la telemetría en Kafka y simula averías |
| WM_FO (operario) | `WM_FO` | Pide riegos desde un menú o desde un fichero y sigue su evolución (Kafka) |

## Tecnologías

Java 25, Maven, sockets TCP, Apache Kafka 4.3.1, SQLite, Docker y Docker Compose, AWS (EC2).

## Compilar

Desde la carpeta de cada módulo:

```
mvn clean package
```

Cada módulo genera en `target` un jar con todas sus dependencias.

## Ejecutar en local

Con Kafka escuchando en `localhost:9092`, cada módulo en su propia terminal y desde su carpeta:

```
java --enable-native-access=ALL-UNNAMED -jar target/WM_Central-1.0-SNAPSHOT.jar 9090 localhost 9092 8080
java -jar target/WS_M-1.0-SNAPSHOT.jar 5001 localhost 9090 WS-01 "Parque Norte"
java -jar target/WS_E-1.0-SNAPSHOT.jar localhost 9092 localhost 5001
java -jar target/WM_FO-1.0-SNAPSHOT.jar localhost 9092 OP01
```

Panel web de Central: `http://localhost:8080`.

## Despliegue

- **Cloud (AWS EC2):** `WM_Central/docker-compose.yml` levanta Kafka, crea los topics y arranca Central. La IP pública de la instancia va en `WM_Central/.env`.
- **Laboratorio:** `WM_WS/docker-compose.yml` (estaciones) y `WM_FO/docker-compose.yml` (operarios). La dirección de Central y de Kafka va en el `.env` de cada carpeta.

La guía de despliegue completa está en la memoria de la práctica.

## Documentación

- `docs/DB.md`: base de datos de Central.
- `prompts/prompts.md`: consultas realizadas a la IA durante el desarrollo.