package com.watermanagement.central;

import java.net.*;
import java.util.*;

import com.watermanagement.central.db.DatabaseManager;

import java.io.*;

public class Hilo_Central extends Thread{
    
    public Socket socket_cliente;
    public DatabaseManager dbmg;
    public Map<String, Estacion> registro;

    public Hilo_Central(Socket socket_cliente, DatabaseManager dbmg, Map<String, Estacion> registro){
        this.socket_cliente = socket_cliente;
        this.dbmg = dbmg;
        this.registro = registro;
    }

    public static void escribeSocket(Socket socket, String cadena){
        
        try{
            OutputStream output = socket.getOutputStream();
            DataOutputStream data = new DataOutputStream(output);
            data.writeUTF(cadena);
        }catch(Exception e){
            System.err.println("Error: " + e.toString());
        }
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

    public void run(){
        String cadena;

        cadena = leeSocket(socket_cliente);

        System.out.println("SE ACABA DE CONECTAR: " + cadena);

        String id_estacion = cadena;

        String[] split;

        cadena = leeSocket(socket_cliente);
        split = cadena.split("#");

        switch(split[0]){
            case "ALTA":
                if(registro.containsKey(split[1])){
                    escribeSocket(socket_cliente, "KO#YA_REGISTRADA");
                }else{
                    Estacion e = new Estacion(split[1], split[2], false, EstadoWS.DESCONECTADA);
                    try{
                        dbmg.insertarEstacion(e);
                        System.out.println("SE HA REGISTRADO CORRECTAMENTE LA ESTACION EN LA BBDD!!");
                        escribeSocket(socket_cliente, "OK");
                    }catch(Exception ex){
                        System.err.println("NO SE HA PODIDO INSERTAR LA ESTACION EN LA BBDD!!");
                        System.err.println("Error:" + ex.toString());
                    }
                    registro.put(e.idEstacion, e);
                }
            break;

            case "AUTENTICAR":
                if(registro.containsKey(split[1]) && registro.get(split[1]).estado == EstadoWS.DESCONECTADA){
                    System.out.println("AUTENTICACION CORRECTA DE " + id_estacion);
                    escribeSocket(socket_cliente, "OK");
                    if(registro.get(split[1]).bloqueada){
                        registro.get(split[1]).estado = EstadoWS.FUERA_DE_SERVICIO;
                    }else{
                        registro.get(split[1]).estado = EstadoWS.DISPONIBLE;
                    }
                }
                else if(!registro.containsKey(split[1])){
                    System.out.println("LA ESTACION " + id_estacion + " NO ESTÁ REGISTRADA");
                    escribeSocket(socket_cliente, "KO#NO_REGISTRADA");
                }
                else{
                    System.out.println("YA HAY UNA ESTACION " + id_estacion + " CONECTADA");
                    escribeSocket(socket_cliente, "KO#YA_CONECTADA");
                }
            break;
        }
    }
}
