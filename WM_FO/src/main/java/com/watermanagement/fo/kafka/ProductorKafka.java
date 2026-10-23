package com.watermanagement.fo.kafka;
 
import java.time.Duration;
import java.util.Properties;
 
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
 
// KafkaProducer se puede usar desde varios hilos a la vez: cread UNO por
// programa y pasádselo a todos los hilos que necesiten enviar.
public class ProductorKafka {
 
    private final KafkaProducer<String, String> productor;
 
    // servidorKafka: "ip:puerto", el que llega por argumentos
    public ProductorKafka(String servidorKafka){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidorKafka);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
 
        // Un mensaje no se da por enviado hasta que el broker lo ha guardado.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
 
        // Si Kafka no está, send() no se queda bloqueado más de 5 s (por defecto
        // serían 60 s) y un mensaje se da por perdido a los 15 s (por defecto 2 min).
        // Así un fallo de Kafka no congela el menú ni los hilos.
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15000);
 
        productor = new KafkaProducer<>(props);
    }
 
    // Envía un mensaje. Es asíncrono: vuelve enseguida y Kafka lo envía por detrás.
    //   topic:   "peticiones-riego", "ordenes-ws"...
    //   clave:   el idEstacion en ordenes-ws y telemetria-ws, para que Kafka
    //            mantenga el orden de los mensajes de cada estación (el FIN no
    //            adelanta al último DATOS). null si el orden no importa.
    //   mensaje: el texto con los campos separados por #
    public void enviar(String topic, String clave, String mensaje){
        ProducerRecord<String, String> registro = new ProducerRecord<>(topic, clave, mensaje);
        try{
            productor.send(registro, (metadatos, error) -> {
                // Se ejecuta cuando Kafka confirma o rechaza el envío
                if(error != null){
                    System.err.println("[KAFKA] No se ha podido enviar a " + topic + ": " + mensaje
                                       + " (" + error.getMessage() + ")");
                }
            });
        }catch(KafkaException e){
            // Por ejemplo, no encuentra el broker en 5 s (max.block.ms)
            System.err.println("[KAFKA] No se ha podido enviar a " + topic + ": " + mensaje
                               + " (" + e.getMessage() + ")");
        }
    }
 
    // Envía lo que quede pendiente y cierra. Llamadlo al terminar el programa:
    // como enviar() es asíncrono, si el programa acaba justo después, el
    // último mensaje podría no salir.
    public void cerrar(){
        productor.close(Duration.ofSeconds(5));
    }
}