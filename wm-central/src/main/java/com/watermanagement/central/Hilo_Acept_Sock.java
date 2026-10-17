package com.watermanagement.central;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import com.watermanagement.central.db.DatabaseManager;

public class Hilo_Acept_Sock extends Thread{

    String puerto;
    DatabaseManager dbmg;
    Map<String, Estacion> registro;

    public Hilo_Acept_Sock(String puerto, DatabaseManager dbmg, Map<String, Estacion> registro){
        this.puerto = puerto;
        this.dbmg = dbmg;
        this. registro = registro;
    } 

    public void run(){
        
        try{
            ServerSocket socket_servidor = new ServerSocket(Integer.parseInt(puerto));
            
            while(true){
                Socket socket_cliente = socket_servidor.accept();
                System.out.println("Se me acaba de conectar una WS");

                Thread hilo = new Hilo_Central(socket_cliente, dbmg, registro);
                hilo.start();
            }

        }catch(Exception e){
            System.err.println("Error: " + e.toString());
        }
    }
}