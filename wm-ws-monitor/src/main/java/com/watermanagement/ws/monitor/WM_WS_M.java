package com.watermanagement.ws.monitor;

import java.io.*;
import java.net.*;

public class WM_WS_M{

    public static void validacionArgumentos(String args[], BufferedReader buffer, String argumentos[]){

        if(args.length != 4){
            try{
                System.out.println("Introduce el puerto por el que se escucha al motor: ");
                argumentos[0] = buffer.readLine();
                System.out.println("Introduce la direccion del Sistema Central: ");
                argumentos[1] = buffer.readLine();
                System.out.println("Introduce el puerto por el que te escucha el Sistema Central: ");
                argumentos[2] = buffer.readLine();
                System.out.println("Introduce el identificador de la estacion: ");
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

    public static void escribeSocket(Socket socket, String cadena){
        try{
            OutputStream stream = socket.getOutputStream();
            DataOutputStream data = new DataOutputStream(stream);
            data.writeUTF(cadena);
        }catch(Exception e){
            System.err.println("Error: " + e.toString());
        }
        return;
    }

    public static String leeSocket(Socket socket){
        String cadena = "";

        try{
            InputStream stream = socket.getInputStream();
            DataInputStream data = new DataInputStream(stream);
            cadena = data.readUTF();

        }catch(Exception e){
            System.err.println("Error: " + e.toString());
        }

        return cadena;
    }

    public static void main(String args[]){

        InputStreamReader reader = new InputStreamReader(System.in);
        BufferedReader buffer = new BufferedReader(reader);

        String[] argumentos = new String[4];

        validacionArgumentos(args, buffer, argumentos);

        System.out.println(("El monitor escucha al motor por el puerto: " + argumentos[0]));
        System.out.println("El sistema central se encuentra en: " + argumentos[1] + ":" + argumentos[2]);
        System.out.println("El identificador de la estacion es: " + argumentos[3]);

        WM_WS_M monitor = new WM_WS_M();

        String host = argumentos[1];
        String puerto = argumentos[2];

        try{
            Socket socket_cliente_central = new Socket(host, Integer.parseInt(puerto));

            System.out.println("Me acabo de conectar a la central, le voy a mandar un saludo y me desconecto.");
            escribeSocket(socket_cliente_central, "Hola soy: " + argumentos[3]);
            
            socket_cliente_central.close();
        }catch(Exception e){
            System.err.println("Error: " + e.toString());
        }
    }

}