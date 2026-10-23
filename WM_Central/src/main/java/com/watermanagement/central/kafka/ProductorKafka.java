package com.watermanagement.central.kafka;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

//PRODUCTOR DE KAFKA. ES IGUAL EN CENTRAL, ENGINE Y FO: SOLO CAMBIA LA LINEA package
//KafkaProducer SE PUEDE USAR DESDE VARIOS HILOS A LA VEZ: SE CREA UNO POR PROGRAMA Y SE PASA A TODOS LOS HILOS
public class ProductorKafka{

    public KafkaProducer<String, String> productor;

    public ProductorKafka(String servidor_kafka){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servidor_kafka);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //UN MENSAJE NO SE DA POR ENVIADO HASTA QUE KAFKA LO HA GUARDADO
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        //SI KAFKA NO ESTA, enviar() SE RINDE A LOS 5 SEGUNDOS EN VEZ DE QUEDARSE BLOQUEADO 1 MINUTO
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15000);

        productor = new KafkaProducer<>(props);
    }

    //ENVIA UN MENSAJE Y ESPERA A QUE KAFKA CONFIRME QUE LO HA GUARDADO
    //  topic:  peticiones-riego, ordenes-ws...
    //  clave:  EL ID DE LA ESTACION EN ordenes-ws Y telemetria-ws (ASI KAFKA MANTIENE EL ORDEN DE LOS MENSAJES
    //          DE CADA ESTACION Y EL FIN NO ADELANTA AL ULTIMO DATOS). NULL EN EL RESTO
    //  cadena: EL MENSAJE CON LOS CAMPOS SEPARADOS POR #
    //DEVUELVE FALSE SI NO SE HA PODIDO ENVIAR
    public boolean enviar(String topic, String clave, String cadena){
        try{
            productor.send(new ProducerRecord<>(topic, clave, cadena)).get();
            return true;
        }catch(Exception e){
            System.err.println("NO SE HA PODIDO ENVIAR A " + topic + ": " + cadena);
            System.err.println("Error: " + e.toString());
            return false;
        }
    }

    //CERRAR AL TERMINAR EL PROGRAMA
    public void cerrar(){
        productor.close(Duration.ofSeconds(5));
    }
}