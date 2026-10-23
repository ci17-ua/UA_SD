package com.watermanagement.ws.monitor;

import java.io.*;
import java.net.*;

public class WM_WS_M{

    public static void escribeSocket (Socket socket, String cadena) throws IOException{
        
        OutputStream stream = socket.getOutputStream();
        DataOutputStream data = new DataOutputStream(stream);
        data.writeUTF(cadena);
    }

    public static String leeSocket(Socket socket) throws IOException{
        String cadena = "";
    
        InputStream stream = socket.getInputStream();
        DataInputStream data = new DataInputStream(stream);
        cadena = data.readUTF();

        return cadena;
    }

    public static void main(String args[]){

        if(args.length < 4 || args.length > 5){
            System.err.println("ARGUMENTOS INCORRECTOS!!");
            System.err.println("USO: /puerto_escucha_motor /IP_central /puerto_escucha_central /ID_estacion /ubicacion(necesario para registrar la estacion y entre comillas)");
            System.exit(1);
        }

        System.out.println(("El monitor escucha al motor por el puerto: " + args[0]));
        System.out.println("El sistema central se encuentra en: " + args[1] + ":" + args[2]);
        System.out.println("El identificador de la estacion es: " + args[3]);

        Socket socket_cliente_central;

        try{
            socket_cliente_central = new Socket(args[1], Integer.parseInt(args[2]));
            
        }catch(UnknownHostException e){
            System.err.println("EL NOMBRE DEL HOST NO EXISTE!!");
            System.err.println("Error: " + e.toString());
            System.exit(1);
            return;
        }catch(Exception e){
            System.err.println("CENTRAL APAGADA O PUERTO EQUIVOCADO!!");
            System.err.println("Error: " + e.toString());
            System.exit(1);
            return;
        }

        String cadena;
        String[] split;
        try{
            //MENSAJE DE IDENTIFICACION (TEMPORAL)
            escribeSocket(socket_cliente_central, args[3]);

            //AUTENTICACION
            if(args.length == 4){
                escribeSocket(socket_cliente_central, "AUTENTICAR#" + args[3]);
                cadena = leeSocket(socket_cliente_central);
                if(cadena.equals("OK") && !cadena.contains("#")){
                    System.out.println("AUTENTICACION CORRECTA!!");
                }
                else{
                    split = cadena.split("#");
                    System.out.println("ERROR EN LA AUTENTICACION POR: " + split[1]);
                    socket_cliente_central.close();
                    System.exit(1);
                }
            }  
            //ALTA
            else{
                escribeSocket(socket_cliente_central, "ALTA#" + args[3] + "#" + args[4]);
                cadena = leeSocket(socket_cliente_central);
                if(cadena.equals("OK") && !cadena.contains("#")){
                    System.out.println("ALTA CORRECTA!!");
                }
                else{
                    split = cadena.split("#");
                    System.out.println("ERROR EN EL ALTA POR: " + split[1]);
                    socket_cliente_central.close();
                    System.exit(1);
                }
            }

            
            
        }catch(Exception e){
            System.err.println("Error: " + e.toString());
        }

        while(true);

    }

}