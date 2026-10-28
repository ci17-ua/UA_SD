package com.watermanagement.fo;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.watermanagement.fo.kafka.ConsumidorKafka;

//HILO QUE LEE DE KAFKA:
//  - LAS RESPUESTAS DE CENTRAL PARA ESTE OPERARIO (respuestas-fo)
//  - LA TELEMETRIA DEL RIEGO QUE LE ESTAN HACIENDO (telemetria-ws)
//  - EL LATIDO DE CENTRAL Y EL ESTADO DE TODAS LAS ESTACIONES (estado-estaciones)
//LO QUE LLEGA LO MUESTRA ESTE HILO. AL HILO PRINCIPAL SOLO LE PASA LAS RESPUESTAS DE LA PETICION QUE ESTA ESPERANDO
public class Hilo_Kafka_FO extends Thread{

    public String servidor_kafka;
    public String id_operario;

    //SE CREA DENTRO DE run() (KafkaConsumer NO SE PUEDE COMPARTIR ENTRE HILOS)
    //ES volatile PORQUE EL HILO PRINCIPAL LO CONSULTA CON estaListo()
    public volatile ConsumidorKafka consumidor;

    //------------------------------------------------------------------------------------------------------------------------------------
    //LO QUE COMPARTE CON EL HILO PRINCIPAL DEL FO

    //PETICION EN CURSO: LA PONE EL HILO PRINCIPAL ANTES DE ENVIAR LA PETICION (NULL SI NO HAY NINGUNA)
    //LOS DATOS DEL RIEGO SE FILTRAN POR LA PETICION (DATOS LLEVA EL ID DE LA PETICION)
    public volatile String id_peticion_actual;
    public volatile String id_estacion_actual;

    //AQUI SE DEJAN EL AUTORIZADO, EL DENEGADO O EL RESUMEN DE LA PETICION EN CURSO
    //EL HILO PRINCIPAL LOS ESPERA CON respuestas.poll(TIEMPO, UNIDAD), QUE DEVUELVE NULL SI NO LLEGA NADA A TIEMPO
    public BlockingQueue<String> respuestas;

    //ULTIMO ESTADO DE CADA ESTACION: ID -> "UBICACION#ESTADO". PARA "VER ESTACIONES"
    public Map<String, String> estaciones;

    //HORA DEL ULTIMO LATIDO O ESTADO RECIBIDO. SI PASAN MAS DE 6 SEGUNDOS SIN NINGUNO, CENTRAL HA CAIDO
    public volatile long ultimo_latido;

    //EL HILO PRINCIPAL LO PONE A FALSE AL SALIR DEL FO, PARA QUE ESTE HILO CIERRE EL CONSUMIDOR
    public volatile boolean activo;

    public Hilo_Kafka_FO(String servidor_kafka, String id_operario){
        this.servidor_kafka = servidor_kafka;
        this.id_operario = id_operario;
        this.consumidor = null;
        this.id_peticion_actual = null;
        this.id_estacion_actual = null;
        this.respuestas = new LinkedBlockingQueue<>();
        this.estaciones = new ConcurrentHashMap<>();
        this.ultimo_latido = System.currentTimeMillis();
        this.activo = true;
    }

    //TRUE CUANDO EL CONSUMIDOR YA TIENE PARTICIONES ASIGNADAS
    //HASTA ENTONCES NO SE DEBE PEDIR NINGUN RIEGO: LA RESPUESTA DE CENTRAL PODRIA LLEGAR ANTES DE QUE ESTEMOS ESCUCHANDO
    public boolean estaListo(){
        ConsumidorKafka c = consumidor;
        return c != null && c.listo;
    }

    public void run(){
        //GRUPO FIJO: AL RELANZAR EL FO, KAFKA LE ENTREGA LO QUE SE PUBLICO MIENTRAS ESTABA CERRADO
        //(ASI VE EL RESUMEN DE UN RIEGO QUE TERMINO CON EL FO CERRADO, COMO PIDE LA GUIA)
        consumidor = new ConsumidorKafka(servidor_kafka, "fo-" + id_operario,
                                         List.of("respuestas-fo", "telemetria-ws", "estado-estaciones"));
        System.out.println("SE HA CREADO EL HILO CONSUMIDOR DE KAFKA CORRECTAMENTE");

        String cadena;
        String[] split;

        try{
            while(activo){
                for(ConsumerRecord<String, String> mensaje : consumidor.recibir()){
                    cadena = mensaje.value();
                    split = cadena.split("#");

                    //UN MENSAJE MAL FORMADO NO PUEDE MATAR EL HILO
                    try{
                        switch(split[0]){
                            //------------------------------------------------------------------------------------------------------------
                            //respuestas-fo: SOLO LAS DE ESTE OPERARIO (EL ID DEL OPERARIO VA EN EL TERCER CAMPO)
                            case "PASO":
                                //PASO#ID_PETICION#ID_OPERARIO#TEXTO
                                if(split[2].equals(id_operario)){
                                    System.out.println("CENTRAL: " + split[3] + anterior(split[1]));
                                }
                            break;

                            case "AUTORIZADO":
                                //AUTORIZADO#ID_PETICION#ID_OPERARIO
                                if(split[2].equals(id_operario)){
                                    System.out.println("RIEGO AUTORIZADO!!" + anterior(split[1]));
                                    avisarSiEsLaActual(split[1], cadena);
                                }
                            break;

                            case "DENEGADO":
                                //DENEGADO#ID_PETICION#ID_OPERARIO#MOTIVO
                                if(split[2].equals(id_operario)){
                                    System.out.println("RIEGO DENEGADO POR: " + split[3] + anterior(split[1]));
                                    avisarSiEsLaActual(split[1], cadena);
                                }
                            break;

                            case "RESUMEN":
                                //RESUMEN#ID_PETICION#ID_OPERARIO#VOLUMEN#DURACION#MOTIVO
                                if(split[2].equals(id_operario)){
                                    System.out.println("RIEGO TERMINADO: " + split[3] + " LITROS EN " + split[4] + " SEGUNDOS (" + split[5] + ")" + anterior(split[1]));
                                    avisarSiEsLaActual(split[1], cadena);
                                }
                            break;

                            //------------------------------------------------------------------------------------------------------------
                            //telemetria-ws: SOLO LOS DATOS DEL RIEGO QUE ME ESTAN HACIENDO AHORA
                            case "DATOS":
                                //DATOS#ID_ESTACION#ID_PETICION#ID_OPERARIO#CAUDAL#VOLUMEN
                                if(split[2].equals(id_peticion_actual)){
                                    System.out.println("ESTACION " + split[1] + ": " + split[4] + " L/MIN, " + split[5] + " LITROS");
                                }
                            break;

                            case "INICIADO":
                            case "RECHAZADO":
                            case "FIN":
                                //SON PARA CENTRAL: EL OPERARIO RECIBE SU RESULTADO POR respuestas-fo
                            break;

                            //------------------------------------------------------------------------------------------------------------
                            //estado-estaciones: TODO. CADA MENSAJE ES ADEMAS UN LATIDO DE CENTRAL
                            case "LATIDO":
                                //LATIDO (CADA 2 SEGUNDOS, AUNQUE CENTRAL NO TENGA NINGUNA ESTACION)
                                ultimo_latido = System.currentTimeMillis();
                            break;

                            case "ESTADO":
                                //ESTADO#ID_ESTACION#UBICACION#ESTADO
                                estaciones.put(split[1], split[2] + "#" + split[3]);
                                ultimo_latido = System.currentTimeMillis();
                            break;

                            default:
                                System.out.println("MENSAJE DE KAFKA DESCONOCIDO: " + cadena);
                        }
                    }catch(Exception e){
                        System.err.println("ERROR AL PROCESAR EL MENSAJE DE KAFKA: " + cadena);
                        System.err.println("Error: " + e.toString());
                    }
                }
            }
        }finally{
            //AVISA A KAFKA DE QUE ESTE FO SE VA: AL RELANZARLO ENTRA EN SU GRUPO SIN ESPERAS
            consumidor.cerrar();
        }
    }

    //SI EL MENSAJE ES DE UNA PETICION QUE NO ES LA ACTUAL (UN RESUMEN QUE LLEGA TARDE O LO QUE SE PUBLICO
    //MIENTRAS EL FO ESTABA CERRADO), SE MARCA PARA QUE NO SE CONFUNDA CON LA PETICION EN CURSO
    public String anterior(String id_peticion){
        if(id_peticion.equals(id_peticion_actual)){
            return "";
        }
        return " [PETICION ANTERIOR " + id_peticion + "]";
    }

    //AL HILO PRINCIPAL SOLO SE LE PASA LO QUE ES DE LA PETICION QUE ESTA ESPERANDO
    //(TRAS RELANZAR EL FO PUEDE LLEGAR EL RESUMEN DE UNA PETICION ANTERIOR: ESE SOLO SE MUESTRA)
    public void avisarSiEsLaActual(String id_peticion, String cadena){
        if(id_peticion.equals(id_peticion_actual)){
            respuestas.offer(cadena);
        }
    }
}