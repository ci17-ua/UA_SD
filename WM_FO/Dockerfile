# =========================================================
# WM_FO (operario)
# Se construye desde la raiz del repositorio:
#   docker build -t wm-fo ./WM_FO
# Con menu (interactivo), con -it:
#   docker run -it --name <nombre> wm-fo <IP_kafka> <puerto_kafka> <ID_operario>
# Con fichero de peticiones (prueba base, sin interaccion): la carpeta del PC
# donde esta el fichero se monta en /peticiones y se pasa su ruta dentro:
#   docker run --name <nombre> -v "<carpeta_del_PC>:/peticiones" wm-fo <IP_kafka> <puerto_kafka> <ID_operario> /peticiones/<fichero>
# Ejemplos:
#   docker run -it --name fo-op01 wm-fo 192.168.1.50 9092 OP01
#   docker run --name fo-op02 -v "C:\dev\pruebas:/peticiones" wm-fo 192.168.1.50 9092 OP02 /peticiones/peticiones.txt
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
# el jar con todas las dependencias: wm-fo-1.0-SNAPSHOT.jar
# (el original-...jar no las lleva).
COPY src ./src
RUN mvn -B package

# ---------- FASE 2: imagen final ----------
# Solo el JRE 25, sin Maven ni el codigo fuente.
FROM eclipse-temurin:25-jre

WORKDIR /app

COPY --from=builder /build/target/wm-fo-1.0-SNAPSHOT.jar wm-fo.jar

# No hay VOLUME: el FO no guarda nada (lo que lleva leido lo recuerda Kafka
# con el grupo fo-<ID_operario>). El fichero de peticiones es del PC y se
# monta al lanzar con -v, no se mete en la imagen.
# No hay EXPOSE: el FO no escucha en ningun puerto, solo se conecta a Kafka.

# ENTRYPOINT lanza el FO. No hay CMD: el ID es de cada operario, asi que
# los argumentos se ponen siempre en "docker run" o en el "command:" del compose:
# IP_kafka puerto_kafka ID_operario [fichero_peticiones]
# Sin argumentos, el FO los pide por teclado (hace falta -it).
ENTRYPOINT ["java", "-jar", "wm-fo.jar"]