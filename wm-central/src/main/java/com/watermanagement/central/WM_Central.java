package com.watermanagement.central;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.watermanagement.central.db.DatabaseManager;

public class WM_Central{

    public static void validacionArgumentos(String args[], BufferedReader buffer, String argumentos[]){

        if(args.length != 3){
            try{
                System.out.println("Introduce el puerto por el que se escucha al monitor: ");
                argumentos[0] = buffer.readLine();
                System.out.println("Introduce la direccion de Kafka: ");
                argumentos[1] = buffer.readLine();
                System.out.println("Introduce el puerto por el que te escucha el Kafka: ");
                argumentos[2] = buffer.readLine();
            }catch(Exception e){
                System.err.println("Error: " + e.toString());
            }
        }else{
            argumentos[0]=args[0];
            argumentos[1]=args[1];
            argumentos[2]=args[2];
        }
    }
    public static void main(String args[]){
        
        InputStreamReader reader = new InputStreamReader(System.in);
        BufferedReader buffer = new BufferedReader(reader);

        String[] argumentos = new String[3];

        validacionArgumentos(args, buffer, argumentos);

        System.out.println(("El sistema central escucha al monitor por el puerto: " + argumentos[0]));
        System.out.println("El Kafka se encuentra en: " + argumentos[1] + ":" + argumentos[2]);

        String puerto = argumentos[0];
        String rutaDB = System.getenv().getOrDefault("WM_DB_PATH", "watermanagement.db");

        DatabaseManager dbmg = new DatabaseManager(rutaDB);
        Map<String, Estacion> registro = new ConcurrentHashMap<>();
        List<Estacion> estaciones = new ArrayList<>();

        try{
            dbmg.crearTabla();
            estaciones = dbmg.cargarEstaciones();
            for(int i=0; i<estaciones.size(); i++){
                registro.put(estaciones.get(i).idEstacion, estaciones.get(i));
            }
        }catch(Exception e){
            System.err.println("Error: " + e.toString());
        }
        
        try{
            ServerSocket socket_servidor = new ServerSocket(Integer.parseInt(puerto));
            
            while(true){
                Socket socket_cliente = socket_servidor.accept();
                System.out.println("Se me acaba de conectar una WS");

                Thread hilo = new Hilo_Central(socket_cliente);
                hilo.start();
            }

        }catch(Exception e){
            System.err.println("Error: " + e.toString());
        }
    }
}