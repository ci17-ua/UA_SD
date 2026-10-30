# =========================================================
# WM_Central
# Se construye desde la raiz del repositorio:
#   docker build -t wm-central ./WM_Central
# Se lanza con -it (tiene menu) y con un volumen para la BBDD:
#   docker run -it --name central -p 9090:9090 -p 8080:8080 -v wm-central-datos:/data wm-central 9090 <IP_kafka> 9092 8080
# =========================================================

# ---------- FASE 1: compilar con Maven ----------
# Imagen con Maven y JDK 25 (el pom.xml compila para Java 25).
# Esta fase no llega a la imagen final: solo se usa para sacar el jar.
FROM maven:3.9-eclipse-temurin-25 AS builder

WORKDIR /build

# Primero solo el pom.xml y la descarga de dependencias.
# Mientras el pom.xml no cambie, Docker reutiliza esta capa y no
# vuelve a descargar Kafka ni SQLite en cada build.
COPY pom.xml .
RUN mvn -B dependency:go-offline

# Despues el codigo, y se empaqueta. El plugin shade deja en target/
# el jar con todas las dependencias: wm-central-1.0-SNAPSHOT.jar
# (el original-...jar no las lleva).
COPY src ./src
RUN mvn -B package

# ---------- FASE 2: imagen final ----------
# Solo el JRE 25, sin Maven ni el codigo fuente.
# No es la variante alpine: sqlite-jdbc trae una libreria nativa y con
# la imagen normal (Ubuntu) no hay problemas de compatibilidad.
FROM eclipse-temurin:25-jre

WORKDIR /app

COPY --from=builder /build/target/WM_Central-1.0-SNAPSHOT.jar WM_Central.jar

# La BBDD va en /data, que es un volumen: asi sobrevive aunque se borre
# y se vuelva a crear el contenedor (Central recupera las estaciones).
ENV WM_DB_PATH=/data/watermanagement.db
VOLUME ["/data"]

# Puertos de Central (informativos: se publican con -p):
#   9090 los Monitors
#   8080 el panel web
EXPOSE 9090 8080

# ENTRYPOINT lanza Central y CMD son los argumentos por defecto
# (puerto_escucha_monitor IP_kafka puerto_kafka puerto_panel_web). Los argumentos de
# "docker run WM_Central ..." sustituyen a los de CMD.
# --enable-native-access evita el aviso de Java 25 al cargar la
# libreria nativa de SQLite.
ENTRYPOINT ["java", "--enable-native-access=ALL-UNNAMED", "-jar", "WM_Central.jar"]
CMD ["9090", "kafka", "9092", "8080"]
