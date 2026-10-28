package com.watermanagement.ws.engine;

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.watermanagement.ws.engine.kafka.ConsumidorKafka;

//HILO QUE LEE DE KAFKA LAS ORDENES DE CENTRAL PARA ESTA ESTACION (ordenes-ws)
//LO QUE HACE CADA ORDEN ESTA EN LOS METODOS synchronized DE WM_WS_E (LOS USAN TAMBIEN EL MENU Y EL HILO DEL MONITOR)
public class Hilo_Kafka_Engine extends Thread{

    public String servidor_kafka;
    public String id_estacion;

    //SE CREA DENTRO DE run() (KafkaConsumer NO SE PUEDE COMPARTIR ENTRE HILOS)
    //ES volatile PORQUE EL HILO QUE RESPONDE A SALUD LO CONSULTA CON estaListo()
    public volatile ConsumidorKafka consumidor;

    public Hilo_Kafka_Engine(String servidor_kafka, String id_estacion){
        this.servidor_kafka = servidor_kafka;
        this.id_estacion = id_estacion;
        this.consumidor = null;
    }

    //TRUE CUANDO EL CONSUMIDOR YA PUEDE RECIBIR ORDENES
    //HASTA ENTONCES EL ENGINE RESPONDE KO A SALUD: SI NO, CENTRAL PODRIA ENVIARLE UN INICIAR QUE NUNCA LEERIA
    public boolean estaListo(){
        ConsumidorKafka c = consumidor;
        return c != null && c.listo;
    }

    public void run(){
        //GRUPO NUEVO EN CADA ARRANQUE: CON latest NO SE LEEN LAS ORDENES QUE LLEGARON MIENTRAS EL ENGINE ESTABA CAIDO
        String grupo = "ws-" + id_estacion + "-" + UUID.randomUUID();
        consumidor = new ConsumidorKafka(servidor_kafka, grupo, List.of("ordenes-ws"));
        System.out.println("SE HA CREADO EL HILO CONSUMIDOR DE KAFKA CORRECTAMENTE");

        String cadena;
        String[] split;

        while(true){
            for(ConsumerRecord<String, String> mensaje : consumidor.recibir()){
                cadena = mensaje.value();
                split = cadena.split("#");

                //TODAS LAS ORDENES LLEVAN EL ID DE LA ESTACION EN EL SEGUNDO CAMPO: SOLO NOS INTERESAN LAS NUESTRAS
                if(split.length < 2 || !split[1].equals(id_estacion)){
                    continue;
                }

                //UN MENSAJE MAL FORMADO NO PUEDE MATAR EL HILO
                try{
                    switch(split[0]){
                        case "INICIAR":
                            //INICIAR#ID_ESTACION#ID_PETICION#ID_OPERARIO#DURACION
                            System.out.println("ORDEN DE CENTRAL: REGAR " + split[4] + " SEGUNDOS (PETICION DE " + split[3] + ")");
                            int duracion;
                            try{
                                duracion = Integer.parseInt(split[4]);
                            }catch(NumberFormatException e){
                                duracion = 0;
                            }
                            WM_WS_E.iniciarRiego(split[2], split[3], duracion);
                        break;

                        case "PARAR":
                            //PARAR#ID_ESTACION#MOTIVO (CENTRAL SOLO LA ENVIA CUANDO HAY UNA FUGA EN MITAD DE UN RIEGO)
                            System.out.println("ORDEN DE CENTRAL: PARAR EL RIEGO (" + split[2] + ")");
                            if(!WM_WS_E.terminarRiego(split[2])){
                                System.out.println("NO HABIA NINGUN RIEGO EN MARCHA");
                            }
                        break;

                        case "BLOQUEAR":
                            //BLOQUEAR#ID_ESTACION
                            System.out.println("ORDEN DE CENTRAL: BLOQUEAR LA ESTACION");
                            WM_WS_E.bloquear();
                        break;

                        case "ACTIVAR":
                            //ACTIVAR#ID_ESTACION
                            System.out.println("ORDEN DE CENTRAL: ACTIVAR LA ESTACION");
                            WM_WS_E.activar();
                        break;

                        default:
                            System.out.println("ORDEN DESCONOCIDA: " + cadena);
                    }
                }catch(Exception e){
                    System.err.println("ERROR AL PROCESAR LA ORDEN: " + cadena);
                    System.err.println("Error: " + e.toString());
                }
            }
        }
    }
}