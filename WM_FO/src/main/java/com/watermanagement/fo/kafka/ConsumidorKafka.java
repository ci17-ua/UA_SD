package com.watermanagement.fo.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

//CONSUMIDOR DE KAFKA. ES IGUAL EN CENTRAL, ENGINE Y FO: SOLO CAMBIA LA LINEA package
//LO QUE CAMBIA EN CADA MODULO (GRUPO Y TOPICS) SE PASA AL CREARLO, Y LO QUE SE HACE CON CADA MENSAJE
//VA EN EL HILO DE CADA MODULO QUE LO USA (POR EJEMPLO Hilo_Kafka_Central)
//KafkaConsumer NO SE PUEDE USAR DESDE VARIOS HILOS: CREADLO Y USADLO SOLO DENTRO DE ESE HILO
public class ConsumidorKafka{

    public KafkaConsumer<String, String> consumidor;

    //TRUE CUANDO KAFKA YA LE HA ASIGNADO PARTICIONES: A PARTIR DE AHI NO SE PIERDE NINGUN MENSAJE NUEVO
    //ES volatile PORQUE LO LEE OTRO HILO (EL ENGINE LO USA PARA NO RESPONDER OK A SALUD ANTES DE TIEMPO)
    public volatile boolean listo;

    public ConsumidorKafka(String servidor_kafka, String grupo, List<String> topics){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidor_kafka);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, grupo);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //SOLO CUENTA LA PRIMERA VEZ QUE SE USA UN GRUPO: EMPIEZA POR LOS MENSAJES NUEVOS, NO POR TODO EL HISTORIAL
        //CON UN GRUPO YA USADO, KAFKA SIGUE DONDE SE QUEDO
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        consumidor = new KafkaConsumer<>(props);
        consumidor.subscribe(topics);
        listo = false;
    }

    //ESPERA HASTA MEDIO SEGUNDO A QUE LLEGUEN MENSAJES Y LOS DEVUELVE (PUEDEN SER 0)
    //SI KAFKA NO ESTA, NO LANZA EXCEPCION: SIMPLEMENTE NO DEVUELVE NADA
    public ConsumerRecords<String, String> recibir(){
        ConsumerRecords<String, String> mensajes = consumidor.poll(Duration.ofMillis(500));
        if(!listo && !consumidor.assignment().isEmpty()){
            listo = true;
        }
        return mensajes;
    }

    //CERRAR AL TERMINAR: AVISA AL GRUPO DE QUE ESTE CONSUMIDOR SE VA
    //(IMPORTANTE EN EL FO: SI NO, AL RELANZARLO KAFKA LO SEGUIRIA CONTANDO COMO MIEMBRO UNOS SEGUNDOS)
    public void cerrar(){
        consumidor.close();
    }
}