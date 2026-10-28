# =========================================================
# WM_WS_E (Engine)
# Se construye desde la raiz del repositorio:
#   docker build -t wm-ws-engine ./WM_WS/WS_E
# Se lanza con -it (tiene menu), con un volumen para el fichero del riego
# y con los argumentos de esa estacion:
#   docker run -it --name <nombre> -v wm-riegos:/data wm-ws-engine <IP_kafka> <puerto_kafka> <IP_monitor> <puerto_monitor>
# Ejemplo:
#   docker run -it --name engine-ws01 -v wm-riegos:/data wm-ws-engine 192.168.1.50 9092 192.168.1.50 5000
# =========================================================

# ---------- FASE 1: compilar con Maven ----------
# Imagen con Maven y JDK 25 (el pom.xml compila para Java 25).
FROM maven:3.9-eclipse-temurin-25 AS builder

WORKDIR /build

# Primero solo el pom.xml y la descarga de dependencias.
# Mientras el pom.xml no cambie, Docker reutiliza esta capa y no
# vuelve a descargar Kafka en cada build.
COPY pom.xml .
RUN mvn -B dependency:go-offline

# Despues el codigo, y se empaqueta. El plugin shade deja en target/
# el jar con todas las dependencias: wm-ws-engine-1.0-SNAPSHOT.jar
# (el original-...jar no las lleva).
COPY src ./src
RUN mvn -B package

# ---------- FASE 2: imagen final ----------
# Solo el JRE 25, sin Maven ni el codigo fuente.
FROM eclipse-temurin:25-jre

WORKDIR /app

COPY --from=builder /build/target/WS_E-1.0-SNAPSHOT.jar WS_E.jar

# El fichero riego-<ID_estacion>.txt (el riego a medias, para no perderlo
# si el Engine se cae) va en /data, que es un volumen: asi sobrevive aunque
# se borre y se vuelva a crear el contenedor. Como el ID va en el nombre,
# varios Engines pueden compartir el mismo volumen.
ENV WM_RIEGO_DIR=/data
VOLUME ["/data"]

# No hay EXPOSE: el Engine no escucha en ningun puerto, solo se conecta
# al Monitor y a Kafka.

# ENTRYPOINT lanza el Engine. No hay CMD: la direccion del Monitor es de
# cada estacion, asi que los argumentos se ponen siempre en "docker run"
# o en el "command:" del compose:
# IP_kafka puerto_kafka IP_monitor puerto_monitor
# Sin argumentos, el Engine los pide por teclado (por eso tambien -it).
ENTRYPOINT ["java", "-jar", "WS_E.jar"]