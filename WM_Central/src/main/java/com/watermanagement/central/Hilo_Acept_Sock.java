package com.watermanagement.central;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import com.watermanagement.central.db.DatabaseManager;
import com.watermanagement.central.kafka.ProductorKafka;

//HILO QUE ACEPTA LAS CONEXIONES DE LOS MONITORS. CADA MONITOR SE ATIENDE EN SU PROPIO Hilo_Central:
//ASI CENTRAL ES UN SERVIDOR CONCURRENTE Y SIGUE ACEPTANDO ESTACIONES MIENTRAS ATIENDE A LAS DEMAS
public class Hilo_Acept_Sock extends Thread{

    public int puerto;
    public DatabaseManager dbmg;
    public Map<String, Estacion> registro;
    public ProductorKafka productor; //SE LO PASA A CADA Hilo_Central (PARA ENVIAR PARAR, BLOQUEAR Y PASO)

    public Hilo_Acept_Sock(int puerto, DatabaseManager dbmg, Map<String, Estacion> registro, ProductorKafka productor){
        this.puerto = puerto;
        this.dbmg = dbmg;
        this.registro = registro;
        this.productor = productor;
    }

    public void run(){
        ServerSocket socket_servidor;

        //SI EL PUERTO ESTA OCUPADO, NINGUN MONITOR PUEDE CONECTARSE: SE TERMINA EL PROGRAMA
        try{
            socket_servidor = new ServerSocket(puerto);
        }catch(IOException e){
            System.err.println("NO SE PUEDE ESCUCHAR A LOS MONITORS EN EL PUERTO " + puerto + "!!");
            System.err.println("Error: " + e.toString());
            System.exit(1);
            return;
        }

        System.out.println("SE HA CREADO EL HILO ACEPTADOR CORRECTAMENTE Y ESTOY A LA ESPERA DE NUEVAS CONEXIONES");

        while(true){
            //UN FALLO AL ACEPTAR UNA CONEXION NO PUEDE DEJAR A CENTRAL SIN ACEPTAR LAS SIGUIENTES
            try{
                Socket socket_cliente = socket_servidor.accept();
                System.out.println("SE ACABA DE CREAR UNA CONEXION!!");
                Thread hilo = new Hilo_Central(socket_cliente, dbmg, registro, productor);
                hilo.start();
            }catch(IOException e){
                System.err.println("Error: " + e.toString());
            }
        }
    }
}
