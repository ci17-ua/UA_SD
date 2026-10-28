package com.watermanagement.central;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.watermanagement.central.db.DatabaseManager;
import com.watermanagement.central.kafka.ProductorKafka;

public class WM_Central{

    public static void main(String args[]){

        //--------------------------------------------------------------------------------------------------------------------------------
        //VALIDAR ARGUMENTOS
        if(args.length != 3){
            System.err.println("ARGUMENTOS INCORRECTOS!!");
            System.err.println("USO: /puerto_escucha_monitor /IP_kafka /puerto_escucha_kafka");
            System.exit(1);
        }

        //VALIDACION DE PUERTOS
        try{
            Integer.parseInt(args[0]);
            Integer.parseInt(args[2]);
        }catch(NumberFormatException e){
            System.err.println("LOS PUERTOS TIENEN QUE SER NUMEROS!!");
            System.exit(1);
        }
        
        System.out.println(("El sistema central escucha al monitor por el puerto: " + args[0]));
        System.out.println("El Kafka se encuentra en: " + args[1] + ":" + args[2]);

        String puerto = args[0];
        
        //--------------------------------------------------------------------------------------------------------------------------------
        //ABRIR LA BASE DE DATOS, CREAR TABLAS SI NO EXISTEN
        //(LOS OPERARIOS NO SE INSERTAN AQUI: CADA UNO SE DA DE ALTA CON SU PRIMERA PETICION)
        String rutaDB = System.getenv().getOrDefault("WM_DB_PATH", "watermanagement.db");
        DatabaseManager dbmg = new DatabaseManager(rutaDB);

        //--------------------------------------------------------------------------------------------------------------------------------
        //CARGAR LAS ESTACIONES DE LA BBDD A MEMORIA
        Map<String, Estacion> registro = new ConcurrentHashMap<>();
        List<Estacion> estaciones = new ArrayList<>();

        try{
            dbmg.crearTablas();
            estaciones = dbmg.cargarEstaciones();
            
            for (Estacion e : estaciones){
                registro.put(e.idEstacion, e);
            }
            System.out.println("SE HA CARGADO LA BASE DE DATOS CORRECTAMENTE!!");
        }catch(Exception e){
            System.err.println("ERROR AL INICIAR LA BASE DE DATOS!!");
            System.err.println("Error: " + e.toString());
            System.exit(1);
        }
        
        //--------------------------------------------------------------------------------------------------------------------------------
        //ANOTAR HORA DE ARRANQUE, COMPROBAR PARA CADA RIEGO QUE EL TIEMPO ES POSTERIOR
        long horaArranque = System.currentTimeMillis();

        //--------------------------------------------------------------------------------------------------------------------------------
        //CREAR UN PRODUCTOR DE KAFKA COMPARTIDO (LO USAN TODOS LOS HILOS)
        String servidor_kafka = args[1] + ":" + args[2];
        ProductorKafka productor = new ProductorKafka(servidor_kafka);

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO QUE ACEPTA CONEXIONES DE ESTACIONES
        Thread aceptador = new Hilo_Acept_Sock(puerto, dbmg, registro, productor);
        System.out.println("SE ACABA DE DELEGAR LA ACEPTACIÓN DE ESTACIONES A OTRO HILO!!");
        aceptador.start();

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO CONSUMIDOR DE KAFKA: PETICIONES DE RIEGO Y TELEMETRIA DE LAS ESTACIONES
        Thread consumidor = new Hilo_Kafka_Central(servidor_kafka, dbmg, registro, productor, horaArranque);
        consumidor.start();

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO DEL LATIDO: ESTADO DE LAS ESTACIONES PARA LOS FO CADA 2 SEGUNDOS Y RESERVAS SIN RESPUESTA DEL ENGINE
        Thread latido = new Hilo_Latido(registro, productor);
        latido.start();

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO PRINCIPAL MUESTRA EL MENÚ DE CENTRAL (SIGUIENTE PASO): INICIAR RIEGO, BLOQUEAR Y ACTIVAR
        

    }
}