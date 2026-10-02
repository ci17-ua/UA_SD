package com.watermanagement.central;

import java.net.*;
import java.util.*;

import com.watermanagement.central.db.DatabaseManager;

import java.io.*;

public class Hilo_Central extends Thread{
    
    Socket socket_cliente;
    DatabaseManager dbmg;
    Map<String, Estacion> registro;

    public Hilo_Central(Socket socket_cliente){
        this.socket_cliente = socket_cliente;
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

        System.out.println(cadena);
    }
}
