package com.watermanagement.central;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import com.watermanagement.central.db.DatabaseManager;
import com.watermanagement.central.kafka.ProductorKafka;

public class Hilo_Acept_Sock extends Thread{

    String puerto;
    DatabaseManager dbmg;
    Map<String, Estacion> registro;
    ProductorKafka productor; //SE LO PASA A CADA Hilo_Central (PARA ENVIAR PARAR, BLOQUEAR Y PASO)

    public Hilo_Acept_Sock(String puerto, DatabaseManager dbmg, Map<String, Estacion> registro, ProductorKafka productor){
        this.puerto = puerto;
        this.dbmg = dbmg;
        this. registro = registro;
        this.productor = productor;
    } 

    public void run(){
        
        System.out.println("SE HA CREADO EL HILO ACEPTADOR CORRECTAMENTE Y ESTOY A LA ESPERA DE NUEVAS CONEXIONES");

        try{
            ServerSocket socket_servidor = new ServerSocket(Integer.parseInt(puerto));
            
            while(true){
                Socket socket_cliente = socket_servidor.accept();
                System.out.println("SE ACABA DE CREAR UNA CONEXION!!");
                Thread hilo = new Hilo_Central(socket_cliente, dbmg, registro, productor);
                hilo.start();
            }

        }catch(Exception e){
            System.err.println("Error: " + e.toString());
        }
    }
}