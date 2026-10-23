# =========================================================
# WM_WS_M (Monitor)
# Se construye desde la raiz del repositorio:
#   docker build -t wm-ws-monitor ./WM_WS/WS_M
# Se lanza con el puerto del motor publicado (el Engine se conecta a el) y
# con los argumentos de esa estacion:
#   docker run --name <nombre> -p <puerto_PC>:5000 wm-ws-monitor 5000 <IP_central> <puerto_central> <ID_estacion> ["<ubicacion>"]
# Ejemplo:
#   docker run --name monitor-ws01 -p 5000:5000 wm-ws-monitor 5000 192.168.1.50 9090 WS-01 "Parque Norte"
# No necesita -it (no tiene menu) ni volumen (no guarda nada).
# =========================================================

# ---------- FASE 1: compilar con Maven ----------
# Imagen con Maven y JDK 25 (el pom.xml compila para Java 25).
FROM maven:3.9-eclipse-temurin-25 AS builder

WORKDIR /build

# Primero solo el pom.xml: mientras no cambie, Docker reutiliza esta capa.
# El Monitor no tiene dependencias, pero asi se descargan una vez los
# plugins de Maven (compiler, jar, shade) y no en cada build.
COPY pom.xml .
RUN mvn -B dependency:go-offline

# Despues el codigo, y se empaqueta: target/wm-ws-monitor-1.0-SNAPSHOT.jar
COPY src ./src
RUN mvn -B package

# ---------- FASE 2: imagen final ----------
# Solo el JRE 25, sin Maven ni el codigo fuente.
FROM eclipse-temurin:25-jre

WORKDIR /app

COPY --from=builder /build/target/wm-ws-monitor-1.0-SNAPSHOT.jar wm-ws-monitor.jar

# Puerto donde el Monitor espera al Engine (informativo: se publica con -p).
EXPOSE 5000

# ENTRYPOINT lanza el Monitor. No hay CMD: los argumentos son de cada
# estacion (sobre todo el ID), asi que se ponen siempre en "docker run"
# o en el "command:" del compose. Sin argumentos, el Monitor muestra el
# uso correcto y termina.
# puerto_escucha_motor IP_central puerto_central ID_estacion [ubicacion]
ENTRYPOINT ["java", "-jar", "wm-ws-monitor.jar"]