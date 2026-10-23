package com.watermanagement.fo;

import java.io.*;
import java.net.*;

public class WM_FO{

    public static void validacionArgumentos(String args[], BufferedReader buffer, String argumentos[]){

        if(args.length != 3){
            try{
                System.out.println("Introduce la direccion de Kafka: ");
                argumentos[0] = buffer.readLine();
                System.out.println("Introduce el puerto para conectarte a Kafka: ");
                argumentos[1] = buffer.readLine();
                System.out.println("Introduce el ID del operador: ");
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

        System.out.println("Kafka se encuentra en: " + argumentos[0] + ":" + argumentos[1]);
        System.out.println("El identificador del operario es: " + argumentos[2]);
    }
}