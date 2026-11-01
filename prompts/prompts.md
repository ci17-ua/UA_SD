# Prompts utilizados en el desarrollo de WaterManagement

**Herramienta:** Claude (Anthropic).

**Cómo la hemos usado.** Como apoyo para entender las tecnologías nuevas, diseñar el flujo y los mensajes antes de programar, revisar nuestro código, encontrar errores y preparar el despliegue. El código lo hemos escrito o adaptado nosotros, y cada propuesta la hemos revisado y probado antes de incorporarla.

**Sobre este documento.** Durante el desarrollo hicimos muchas consultas parecidas o repetidas, a medida que avanzábamos y surgían dudas nuevas. Aquí las recogemos **agrupadas por temas y resumidas**, sin repeticiones, para que se entienda qué preguntamos y para qué.

---

## 1. Tecnologías nuevas

### Maven
- ¿Qué es Maven y para qué sirve el `pom.xml`? ¿Qué significan `groupId`, `artifactId` y `version`, y por qué el jar se llama así?
- ¿Qué hace exactamente `mvn clean package`? ¿Qué fases se ejecutan y qué deja en la carpeta `target`?
- ¿Por qué un jar normal no se puede lanzar con `java -jar` si usa librerías? ¿Qué hace el plugin shade y qué diferencia hay entre el jar que genera y el `original-...jar`?
- ¿Qué es el `dependency-reduced-pom.xml`? ¿Tenemos que subirlo al repositorio?

### Kafka
- Explícanos qué es Kafka y en qué se diferencia de comunicar dos programas con sockets.
- ¿Qué son los topics, las particiones y los offsets? ¿Qué pasa con un mensaje si nadie lo está leyendo cuando se publica?
- ¿Qué es un grupo de consumidores? ¿Qué cambia si dos consumidores están en el mismo grupo o en grupos distintos?
- ¿Qué significa `auto.offset.reset=latest`? ¿Se aplica siempre o solo la primera vez que se usa un grupo?
- ¿Para qué sirve la clave de un mensaje? ¿Por qué influye en el orden en que llegan?
- ¿Qué es el modo KRaft? ¿Necesitamos ZooKeeper con Kafka 4?

### Sockets
- ¿Qué diferencia hay entre `ServerSocket` y `Socket`? ¿Cómo atiende un servidor a varios clientes a la vez?
- ¿Qué hacen `writeUTF` y `readUTF`? ¿Cómo sabe el que recibe dónde acaba cada mensaje?
- ¿Cómo detectamos que el otro extremo se ha caído si no cierra el socket? ¿Qué hace `setSoTimeout`?
- El enunciado recomienda el empaquetado `<STX><DATA><ETX><LRC>`. ¿Qué es el LRC y cómo funcionan ACK y NACK? ¿Tiene sentido si TCP ya comprueba los errores?

### Docker
- ¿Qué diferencia hay entre una imagen y un contenedor? ¿Qué es un volumen y cuándo hace falta?
- ¿Qué es un Dockerfile multi-stage? ¿Por qué la imagen final no necesita Maven ni el JDK?
- ¿Qué diferencia hay entre `ENTRYPOINT` y `CMD`? ¿Cómo se pasan argumentos distintos a cada contenedor?
- Dentro de un contenedor, ¿a qué apunta `localhost`? ¿Cómo se conectan dos contenedores entre sí?
- ¿Qué es Docker Compose? ¿Qué ventajas tiene frente a lanzar cada contenedor con `docker run`?

### La nube (AWS)
- ¿Qué es AWS Academy Learner Lab y cómo funciona? ¿Qué desplegamos exactamente en la nube: el código, los jars o los contenedores?
- ¿Qué es una instancia EC2? ¿Qué tamaño necesitamos para Kafka y Central?
- ¿Qué es un grupo de seguridad? ¿Qué puertos tenemos que abrir y por qué cada uno?
- ¿Qué es una IP elástica? ¿Por qué la necesitamos si la instancia ya tiene IP pública?

### Base de datos (SQLite)
- ¿Por qué SQLite es suficiente para esta práctica? ¿Qué diferencia hay con un servidor de base de datos?
- ¿Cómo se usa SQLite desde Java con JDBC? ¿Qué diferencia hay entre `Statement` y `PreparedStatement`?
- ¿Qué significa declarar una tabla `STRICT`?
- ¿Por qué puede aparecer `database is locked` si escriben varios hilos a la vez?

### Web con Java
- ¿Se puede servir una página web desde Java sin usar ningún framework? ¿Cómo funciona `com.sun.net.httpserver`?
- ¿Cómo hace una página para pedir datos al servidor cada segundo y actualizarse sin recargar?

---

## 2. La práctica y la estructura del proyecto
- Hemos leído el enunciado y la guía de corrección. Resúmenos qué hace cada módulo y qué se evalúa en cada apartado, para no dejarnos nada.
- La guía pide las carpetas `WM_Central`, `WM_WS` y `WM_FO`. ¿Dónde ponemos el Monitor y el Engine? ¿Es mejor un proyecto Maven por módulo o uno común?
- ¿Qué tiene que ir por sockets y qué por Kafka? ¿Por qué el enunciado reparte así las comunicaciones?
- ¿Qué cosas no fija el enunciado y tenemos que decidir nosotros? Por ejemplo, el formato del fichero de peticiones o qué pasa con un operario nuevo.

---

## 3. Flujo de cada módulo
- Ayúdanos a definir qué hace Central al arrancar, en qué orden, y qué hace con cada mensaje que le llega.
- ¿Cómo se autentica una estación? ¿Qué pasa si se conecta un Monitor con un ID que no está registrado, o con uno que ya está conectado?
- El Monitor tiene que hablar con Central y con el Engine a la vez. ¿Cómo lo organizamos para que un Engine colgado no impida avisar a Central?
- Describe paso a paso un riego completo, desde que el operario lo pide hasta que recibe el resumen, indicando qué mensaje va por qué canal.
- Si hay una fuga en mitad de un riego, el aviso del Engine y el del Monitor llegan por caminos distintos. ¿Cómo hacemos que Central funcione bien lleguen en el orden que lleguen?
- ¿Qué tiene que pasar cuando cae cada módulo (Monitor, Engine, operario, Central)? ¿Cómo se recupera cada uno al volver?
- ¿Cómo hace el operario para procesar un fichero de peticiones sin ninguna interacción, esperando 4 segundos entre una y otra?

---

## 4. Estructuras de datos y mensajes
- ¿Qué tablas necesita la base de datos de Central y con qué campos? ¿Hay que guardar a los operarios?
- ¿Guardamos en la base de datos el estado DESCONECTADA? ¿Qué estado tiene que tener una estación al arrancar Central?
- ¿Qué clases de datos necesita cada módulo? ¿Qué campos debería tener `Estacion`, y cuáles `Riego` en Central y en el Engine?
- Proponnos un formato de mensajes común para sockets y Kafka. ¿Qué campos necesita cada mensaje? ¿Por qué conviene que el `FIN` lleve el operario y la duración?
- ¿Cuántos topics necesitamos y por qué? ¿Es mejor uno por estación o pocos topics fijos?

---

## 5. Desarrollo de cada módulo y revisión del código
- Este es nuestro `Hilo_Central`. ¿Está bien estructurado? ¿Cumple el flujo que definimos o falta algún caso?
- Revisa el Monitor: ¿qué pasa si Central se cae mientras está enviando el mensaje de vida? ¿Se reconecta bien?
- Revisa el Engine: ¿está bien que el riego se haga en un hilo aparte? ¿Qué pasa si llega una orden de parar mientras riega?
- Revisa el operario: ¿filtra bien los mensajes que no son suyos? ¿Qué pasa si se reinicia en mitad de un riego?
- ¿Hay código repetido entre módulos que deberíamos ordenar mejor? ¿Los nombres de clases y métodos se entienden?
- ¿Cómo validamos los argumentos de cada módulo para que no arranque a medias si falta alguno o está mal?

---

## 6. Hilos y datos compartidos
- ¿Qué hilos necesita cada módulo? ¿Por qué el consumidor de Kafka tiene que ir en su propio hilo?
- ¿Se puede compartir un `KafkaConsumer` entre hilos? ¿Y un `KafkaProducer`?
- ¿Cuándo hace falta `synchronized`, cuándo `volatile` y cuándo un `ConcurrentHashMap`? Explícalo con nuestro registro de estaciones.
- Si dos operarios piden la misma estación a la vez, ¿cómo evitamos que los dos se la queden?
- En el Engine, el riego lo tocan el consumidor de órdenes, el hilo del riego, el menú y el hilo del Monitor. ¿Cómo evitamos que se pisen?
- ¿Cómo espera el hilo principal del operario la respuesta que recibe el hilo de Kafka? ¿Para qué sirve una `BlockingQueue`?

---

## 7. Corrección de errores
- Los decimales llegan a Central como `10,5` y `Double.parseDouble` falla. ¿Por qué pasa en un Windows en español y cómo lo arreglamos?
- Al relanzar un Monitor caído, Central responde `YA_CONECTADA`. ¿Por qué, y cuánto hay que esperar?
- Un Engine reiniciado ejecuta órdenes antiguas que llegaron mientras estaba caído. ¿Cómo lo evitamos?
- Central no envía el resumen al operario si se reinicia en mitad de un riego. ¿Qué le falta?
- Con `mvn package clean` la carpeta `target` aparece mientras se ejecuta y desaparece al terminar. ¿Por qué?
- Al arrancar Central sale un aviso de SLF4J. ¿Qué significa y cómo se quita?
- Con Docker Compose, el primer latido de Central falla porque Kafka todavía no está listo. ¿Cómo hacemos que Central espere?
- Al construir las imágenes de las 20 estaciones con `docker compose up --build` sale un error de que la imagen ya existe. ¿Por qué?
- En el laboratorio, Central rechaza las peticiones porque las considera anteriores a su arranque, y el reloj de los PCs va retrasado. ¿Cómo hacemos que no dependa de ese reloj?
- Con Kafka en Windows, un Engine se queda sin conectar y en la ventana de Kafka pone «Seleccionar». ¿Qué está pasando?

---

## 8. Ejemplos básicos de Kafka y sockets
- Danos un ejemplo mínimo de productor y consumidor de Kafka en Java, para entender cómo funcionan antes de usarlos en la práctica.
- ¿Cómo probamos un topic a mano con `kafka-console-producer.sh` y `kafka-console-consumer.sh`?
- Danos un ejemplo mínimo de servidor de sockets que atienda a varios clientes con un hilo por cliente.
- ¿Cómo sería un intercambio de petición y respuesta por socket con un tiempo máximo de espera?

---

## 9. Ejemplos de Docker y Maven
- ¿Cómo sería un Dockerfile en dos fases para un proyecto Maven con Java 25? ¿Cómo aprovechamos la caché para no descargar las dependencias en cada build?
- ¿Cómo se configura Kafka en Docker para que lo usen tanto un contenedor del mismo compose como programas de otros PCs? ¿Qué son los listeners y los advertised listeners?
- ¿Cómo lanzamos varias estaciones en el mismo PC sin que choquen los puertos?
- ¿Cómo vemos lo que muestra un contenedor y cómo usamos su menú? ¿Qué diferencia hay entre `docker logs` y `docker attach`?
- ¿Cómo preparamos las imágenes para no tener que compilar el día de la corrección?

---

## 10. Productores, consumidores y base de datos en Java
- ¿Conviene que el productor espere la confirmación de Kafka (`send().get()`)? ¿Qué ganamos y qué perdemos?
- ¿Cómo sabe un consumidor que ya tiene particiones asignadas y puede empezar a recibir? ¿Por qué importa con `latest`?
- ¿Qué configuración necesitan el productor y el consumidor (`acks`, tiempos máximos, deserializadores)?
- ¿Cómo organizamos el acceso a SQLite en una clase `DatabaseManager`? ¿Abrimos una conexión por operación o una para todo el programa?
- ¿Cómo damos de alta una estación sin que la memoria y la base de datos queden distintas si la inserción falla?

---

## 11. Panel web de Central
- Central va a estar en la nube, sin pantalla. ¿Es mejor un panel por consola o uno web?
- ¿Cómo diseñamos una API mínima para el panel: una ruta para pedir el estado y otra para enviar órdenes?
- ¿Cómo mostramos los colores que pide la guía, incluido el verde parpadeante mientras riega?
- ¿Cómo hacemos que el panel muestre lo mismo que la terminal de Central sin tocar el resto del código?
- ¿Cómo reutilizamos las órdenes del menú (iniciar, bloquear, activar) para los botones del panel?

---

## 12. Despliegue en AWS
- ¿Qué pasos hay que seguir para crear la instancia EC2 en el Learner Lab, desde elegir el sistema operativo hasta tenerla en marcha?
- ¿Cómo instalamos Docker en la instancia y hacemos que Kafka y Central arranquen solos al encenderla?
- El repositorio es privado: ¿cómo lo clonamos en la instancia sin poner nuestra contraseña? ¿Qué es una clave de despliegue?
- Cuando no estamos trabajando, ¿hay que parar algo para no gastar crédito? ¿Qué pasa con la instancia al acabar la sesión del laboratorio?
- ¿Cómo comprobamos desde el laboratorio que llegamos a los puertos de la instancia?
- ¿Cómo sería el ciclo VS Code → GitHub → AWS? ¿Cómo puede una GitHub Action desplegar en la instancia si las credenciales del laboratorio cambian en cada sesión?

---

## 13. Pruebas, memoria y presentación
- Ayúdanos a preparar un plan de pruebas por fases que cubra el funcionamiento base y las caídas de cada módulo.
- ¿Cómo lanzamos 20 estaciones y 20 operarios a la vez para probar la escalabilidad?
- ¿Qué apartados debería tener la memoria según lo que pide el enunciado? Revisa nuestro borrador para que se explique bien.
- ¿Cómo explicarías la práctica a alguien que no la conoce? Queremos prepararla para presentarla.