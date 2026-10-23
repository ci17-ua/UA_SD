package com.watermanagement.central;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.watermanagement.central.db.DatabaseManager;

public class WM_Central{

    public static void main(String args[]){

        //--------------------------------------------------------------------------------------------------------------------------------
        //VALIDAR ARGUMENTOS
        if(args.length != 3){
            System.err.println("ARGUMENTOS INCORRECTOS!!");
            System.err.println("USO: /puerto_escucha_monitor /IP_kafka /puerto_escucha_kafka");
            System.exit(1);
        }
        
        System.out.println(("El sistema central escucha al monitor por el puerto: " + args[0]));
        System.out.println("El Kafka se encuentra en: " + args[1] + ":" + args[2]);

        String puerto = args[0];
        
        //--------------------------------------------------------------------------------------------------------------------------------
        //ABRIR LA BASE DE DATOS, CREAR TABLAS SI NO EXISTEN E INSERTAR OPERARIOS INICIALES
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
        //CREAR UN PRODUCTOR DE KAFKA COMPARTIDO

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO QUE ACEPTA CONEXIONES DE ESTACIONES
        Thread aceptador = new Hilo_Acept_Sock(puerto, dbmg, registro);
        System.out.println("SE ACABA DE DELEGAR LA ACEPTACIÓN DE ESTACIONES A OTRO HILO!!");
        aceptador.start();
        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO PRINCIPAL MUESTRA EL MENÚ DE CENTRAL
        

    }
}