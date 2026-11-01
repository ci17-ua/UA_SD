package com.watermanagement.central;

import java.net.*;
import java.sql.SQLException;
import java.util.*;

import com.watermanagement.central.db.DatabaseManager;
import com.watermanagement.central.kafka.ProductorKafka;

import java.io.*;

public class Hilo_Central extends Thread{

    public Socket socket_cliente;
    public DatabaseManager dbmg;
    public Map<String, Estacion> registro;
    public ProductorKafka productor; //PARA ENVIAR PARAR Y BLOQUEAR AL ENGINE Y PASO AL OPERARIO
    public Estacion estacion; //ESTACION QUE HA CONECTADO ESTE HILO (NULL HASTA QUE SE AUTENTICA)

    public Hilo_Central(Socket socket_cliente, DatabaseManager dbmg, Map<String, Estacion> registro, ProductorKafka productor){
        this.socket_cliente = socket_cliente;
        this.dbmg = dbmg;
        this.registro = registro;
        this.productor = productor;
        this.estacion = null;
    }

    //CADA MENSAJE VIAJA EN UNA TRAMA <STX><DATOS><ETX><LRC> Y SE CONFIRMA CON ACK O NACK (CLASE Trama)
    //LAS EXCEPCIONES NO SE CAPTURAN AQUI: SI EL MONITOR SE CAE, run() TIENE QUE ENTERARSE
    public static void escribeSocket(Socket socket, String cadena) throws IOException{
        Trama.enviar(socket, cadena);
    }

    public static String leeSocket(Socket socket) throws IOException{
        return Trama.recibir(socket);
    }

    public void run(){
        String cadena;
        String[] split;

        try{
            //SI EN 3 SEGUNDOS NO LLEGA NINGUN MENSAJE, leeSocket LANZA SocketTimeoutException
            //VALE PARA EL ALTA, LA AUTENTICACION Y LOS MENSAJES DE VIDA
            socket_cliente.setSoTimeout(3000);

            //EL MONITOR ABRE LA COMUNICACION CON ENQ Y CENTRAL LE CONTESTA ACK
            Trama.esperarEnq(socket_cliente);

            cadena = leeSocket(socket_cliente);
            split = cadena.split("#");

            switch(split[0]){
                case "ALTA":
                    //ALTA Y DESPUES AUTENTICACION
                    if(!alta(split)){
                        break;
                    }
                    cadena = leeSocket(socket_cliente);
                    split = cadena.split("#");
                    if(!split[0].equals("AUTENTICAR")){
                        System.out.println("MENSAJE INVALIDO DESPUES DEL ALTA: " + cadena);
                        escribeSocket(socket_cliente, "KO#MENSAJE_INVALIDO");
                        break;
                    }
                    autenticar(split);
                break;

                case "AUTENTICAR":
                    autenticar(split);
                break;

                default:
                    System.out.println("MENSAJE INVALIDO: " + cadena);
                    escribeSocket(socket_cliente, "KO#MENSAJE_INVALIDO");
            }

            //SI SE HA AUTENTICADO, SE QUEDA ATENDIENDO SUS MENSAJES DE VIDA
            if(estacion != null){
                vida();
            }

        }catch(SocketTimeoutException e){
            if(estacion != null){
                System.out.println("LA ESTACION " + estacion.idEstacion + " LLEVA 3 SEGUNDOS SIN ENVIAR VIDA");
            }
        }catch(IOException e){
            //EL MONITOR HA CERRADO LA CONEXION O SE HA CAIDO
        }finally{
            if(estacion != null){
                estacion.desconectar();
                System.out.println("LA ESTACION " + estacion.idEstacion + " SE HA DESCONECTADO");

                //SI ESTABA REGANDO, SE AVISA AL OPERARIO. EL ENGINE, SIN MONITOR, PARA SOLO Y ENVIA SU FIN (Y CON EL, EL RESUMEN)
                Riego riego = estacion.getRiego();
                if(riego != null){
                    Hilo_Kafka_Central.avisar(productor, "PASO", riego.idPeticion, riego.idOperario,
                                              "SE HA PERDIDO LA CONEXION CON LA ESTACION " + estacion.idEstacion);
                }
            }
            try{
                socket_cliente.close();
            }catch(IOException e){
                System.err.println("Error: " + e.toString());
            }
        }
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //ALTA#ID#UBICACION
    //DEVUELVE FALSE SI HAY QUE CORTAR LA CONEXION. YA REGISTRADA NO ES UN ERROR: EL MONITOR SIGUE CON AUTENTICAR
    public boolean alta(String[] split) throws IOException{
        //MENSAJE MAL FORMADO
        if(split.length != 3){
            escribeSocket(socket_cliente, "KO#MENSAJE_INVALIDO");
            return false;
        }

        Estacion e = new Estacion(split[1], split[2], false, EstadoWS.DESCONECTADA);

        //YA REGISTRADA
        //putIfAbsent COMPRUEBA Y AÑADE A LA VEZ: DEVUELVE NULL SI LA HA AÑADIDO O LA ESTACION QUE YA ESTABA
        if(registro.putIfAbsent(split[1], e) != null){
            System.out.println("LA ESTACION " + split[1] + " YA ESTABA REGISTRADA");
            escribeSocket(socket_cliente, "KO#YA_REGISTRADA");
            return true;
        }

        //REGISTRO NUEVO
        try{
            dbmg.insertarEstacion(e);
        }catch(SQLException ex){
            //SE DESHACE PARA QUE LA MEMORIA Y LA BBDD DIGAN LO MISMO
            registro.remove(split[1], e);
            System.err.println("NO SE HA PODIDO INSERTAR LA ESTACION EN LA BBDD!!");
            System.err.println("Error: " + ex.toString());
            escribeSocket(socket_cliente, "KO#ERROR_BD");
            return false;
        }

        System.out.println("SE HA REGISTRADO CORRECTAMENTE LA ESTACION " + split[1] + " EN " + split[2]);
        escribeSocket(socket_cliente, "OK");
        return true;
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //AUTENTICAR#ID
    //SI VA BIEN, LA ESTACION QUEDA GUARDADA EN this.estacion
    public void autenticar(String[] split) throws IOException{
        //MENSAJE MAL FORMADO
        if(split.length != 2){
            escribeSocket(socket_cliente, "KO#MENSAJE_INVALIDO");
            return;
        }

        Estacion e = registro.get(split[1]);

        //NO REGISTRADA
        if(e == null){
            System.out.println("LA ESTACION " + split[1] + " NO ESTA REGISTRADA");
            escribeSocket(socket_cliente, "KO#NO_REGISTRADA");
            return;
        }

        //YA CONECTADA
        //autenticar() COMPRUEBA Y CAMBIA EL ESTADO A LA VEZ
        if(!e.autenticar()){
            System.out.println("YA HAY UNA ESTACION " + split[1] + " CONECTADA");
            escribeSocket(socket_cliente, "KO#YA_CONECTADA");
            return;
        }

        //REGISTRADA Y DESCONECTADA: A PARTIR DE AQUI, SI LA CONEXION SE CORTA, EL finally LA DESCONECTA
        estacion = e;
        guardarEstado();

        System.out.println("AUTENTICACION CORRECTA DE " + split[1] + ": " + e.getEstado());
        escribeSocket(socket_cliente, "OK");

        //UN ENGINE REINICIADO NO RECUERDA QUE ESTABA BLOQUEADO (NO LEE LAS ORDENES ANTIGUAS): SE LE VUELVE A DECIR
        if(e.estaBloqueada()){
            reenviarBloqueo();
        }
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //VIDA#ID#OK O VIDA#ID#KO, CADA SEGUNDO. CENTRAL RESPONDE OK A CADA UNO
    //SI PASAN 3 SEGUNDOS SIN MENSAJE (setSoTimeout DE run()), leeSocket LANZA SocketTimeoutException
    //Y EL finally DE run() DESCONECTA LA ESTACION
    public void vida() throws IOException{
        String cadena;
        String[] split;
        Riego riego;

        while(true){
            cadena = leeSocket(socket_cliente);
            split = cadena.split("#");

            //MENSAJE MAL FORMADO O CON EL ID DE OTRA ESTACION
            if(split.length != 3 || !split[0].equals("VIDA") || !split[1].equals(estacion.idEstacion)){
                System.out.println("MENSAJE INVALIDO DE " + estacion.idEstacion + ": " + cadena);
                escribeSocket(socket_cliente, "KO#MENSAJE_INVALIDO");
                continue;
            }

            switch(split[2]){
                case "KO":
                    //EL MOTOR NO RESPONDE O HA RESPONDIDO KO: FUGA (SOLO SE AVISA Y SE GUARDA AL CAMBIAR)
                    if(estacion.marcarFuga()){
                        System.out.println("LA ESTACION " + estacion.idEstacion + " TIENE UNA FUGA O UNA AVERIA!!");
                        guardarEstado();

                        //SI ESTABA REGANDO, SE PARA EL RIEGO Y SE AVISA AL OPERARIO
                        //(SI EL ENGINE ESTA VIVO, PARA Y ENVIA FIN; SI ESTA CAIDO, ENVIARA EL FIN AL VOLVER)
                        riego = estacion.getRiego();
                        if(riego != null){
                            productor.enviar("ordenes-ws", estacion.idEstacion, "PARAR#" + estacion.idEstacion + "#FUGA");
                            System.out.println("ORDEN PARAR ENVIADA A " + estacion.idEstacion);
                            Hilo_Kafka_Central.avisar(productor, "PASO", riego.idPeticion, riego.idOperario,
                                                      "LA ESTACION " + estacion.idEstacion + " TIENE UNA FUGA O UNA AVERIA: SE PARA EL RIEGO");
                        }
                    }
                break;

                case "OK":
                    //SI ESTABA EN FUGA, SE HA RESUELTO
                    if(estacion.fugaResuelta()){
                        System.out.println("LA ESTACION " + estacion.idEstacion + " SE HA RECUPERADO: " + estacion.getEstado());
                        guardarEstado();

                        //EL ENGINE PUEDE HABERSE REINICIADO: SI ESTA BLOQUEADA, SE LE VUELVE A DECIR
                        if(estacion.estaBloqueada()){
                            reenviarBloqueo();
                        }
                    }
                break;

                default:
                    System.out.println("MENSAJE INVALIDO DE " + estacion.idEstacion + ": " + cadena);
                    escribeSocket(socket_cliente, "KO#MENSAJE_INVALIDO");
                    continue;
            }

            escribeSocket(socket_cliente, "OK");
        }
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //BLOQUEAR#ID AL ENGINE DE ESTA ESTACION
    public void reenviarBloqueo(){
        if(productor.enviar("ordenes-ws", estacion.idEstacion, "BLOQUEAR#" + estacion.idEstacion)){
            System.out.println("ORDEN BLOQUEAR REENVIADA A " + estacion.idEstacion + " (SIGUE FUERA DE SERVICIO)");
        }
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //GUARDA EN LA BBDD EL ESTADO DE LA ESTACION. UN FALLO AL GUARDAR NO CORTA LA CONEXION
    public void guardarEstado(){
        try{
            dbmg.actualizarEstado(estacion.idEstacion, estacion.estadoParaGuardar());
        }catch(SQLException e){
            System.err.println("NO SE HA PODIDO GUARDAR EL ESTADO DE LA ESTACION " + estacion.idEstacion + " EN LA BBDD!!");
            System.err.println("Error: " + e.toString());
        }
    }
}