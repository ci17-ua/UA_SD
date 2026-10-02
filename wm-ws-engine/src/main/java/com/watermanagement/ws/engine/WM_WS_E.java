package com.watermanagement.ws.engine;

import java.io.*;
import java.net.*;

public class WM_WS_E{

    public static void validacionArgumentos(String args[], BufferedReader buffer, String argumentos[]){

        if(args.length != 4){
            try{
                System.out.println("Introduce la direccion de Kafka: ");
                argumentos[0] = buffer.readLine();
                System.out.println("Introduce el puerto de escucha de Kafka: ");
                argumentos[1] = buffer.readLine();
                System.out.println("Introduce la direccion del Monitor: ");
                argumentos[2] = buffer.readLine();
                System.out.println("Introduce el puerto de escucha del Monitor: ");
                argumentos[3] = buffer.readLine();
            }catch(Exception e){
                System.err.println("Error: " + e.toString());
            }
        }else{
            argumentos[0]=args[0];
            argumentos[1]=args[1];
            argumentos[2]=args[2];
            argumentos[3]=args[3];
        }
    }

    public static void main(String args[]){

        InputStreamReader reader = new InputStreamReader(System.in);
        BufferedReader buffer = new BufferedReader(reader);

        String[] argumentos = new String[4];

        validacionArgumentos(args, buffer, argumentos);

        System.out.println("Kafka se encuentra en: " + argumentos[0] + ":" + argumentos[1]);
        System.out.println("El monitor se encuentra en: " + argumentos[2] + ":" + argumentos[3]);
    
    }
}