package com.watermanagement.central;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.watermanagement.central.db.DatabaseManager;

public class WM_Central{

    public static void validacionArgumentos(String args[], BufferedReader buffer, String argumentos[]){

        if(args.length != 3){
            System.err.println("ARGUMENTOS INCORRECTOS!!");
            System.err.println("USO: puerto_escucha_monitor IP_kafka puerto_escucha_kafka");
            System.exit(1);
        }else{
            argumentos[0]=args[0];
            argumentos[1]=args[1];
            argumentos[2]=args[2];
        }
    }
    public static void main(String args[]){

        //--------------------------------------------------------------------------------------------------------------------------------
        //VALIDAR ARGUMENTOS
        InputStreamReader reader = new InputStreamReader(System.in);
        BufferedReader buffer = new BufferedReader(reader);

        String[] argumentos = new String[3];

        validacionArgumentos(args, buffer, argumentos);

        System.out.println(("El sistema central escucha al monitor por el puerto: " + argumentos[0]));
        System.out.println("El Kafka se encuentra en: " + argumentos[1] + ":" + argumentos[2]);

        String puerto = argumentos[0];
        
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
            for(int i=0; i<estaciones.size(); i++){
                registro.put(estaciones.get(i).idEstacion, estaciones.get(i));
            }
        }catch(Exception e){
            System.err.println("Error: " + e.toString());
        }
        
        //--------------------------------------------------------------------------------------------------------------------------------
        //ANOTAR HORA DE ARRANQUE, COMPROBAR PARA CADA RIEGO QUE EL TIEMPO ES POSTERIOR

        //--------------------------------------------------------------------------------------------------------------------------------
        //CREAR UN PRODUCTOR DE KAFKA COMPARTIDO

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO PRINCIPAL MUESTRA EL MENÚ DE CENTRAL
        
        Hilo_Acept_Sock aceptador = new Hilo_Acept_Sock(puerto, dbmg, registro);



    }
}