package com.watermanagement.ws.engine;

import java.io.*;
import java.net.*;

//HILO QUE HABLA CON EL MONITOR (WM_WS_M)
//SE LANZA UNA SOLA VEZ AL ARRANCAR EL MOTOR Y DURA TODO EL PROGRAMA:
//  - SE CONECTA AL MONITOR (SI NO ESTA, REINTENTA CADA 3 SEGUNDOS)
//  - EL PRIMER MENSAJE DEL MONITOR ES ID#ID_ESTACION: ASI SABE EL MOTOR QUE ESTACION ES
//  - DESPUES RESPONDE OK O KO A CADA SALUD
//  - SI PASAN 3 SEGUNDOS SIN SALUD O SE CORTA LA CONEXION, EL MONITOR HA CAIDO: SIN MONITOR NO SE RIEGA
public class Hilo_Monitor extends Thread{

    public String ip_monitor;
    public int puerto_monitor;

    public Hilo_Monitor(String ip_monitor, int puerto_monitor){
        this.ip_monitor = ip_monitor;
        this.puerto_monitor = puerto_monitor;
    }

    public void run(){
        String cadena;
        String[] split;

        while(true){
            //try-with-resources: EL SOCKET SE CIERRA SOLO AL SALIR DEL try, PASE LO QUE PASE
            try(Socket socket_monitor = new Socket(ip_monitor, puerto_monitor)){

                //SI EL MONITOR TARDA MAS DE 3 SEGUNDOS EN ENVIAR ALGO, leeSocket LANZA SocketTimeoutException
                //(VALE PARA EL ID Y PARA CADA SALUD)
                socket_monitor.setSoTimeout(3000);

                //PRIMER MENSAJE: ID#ID_ESTACION
                cadena = WM_WS_E.leeSocket(socket_monitor);
                split = cadena.split("#");

                if(split.length != 2 || !split[0].equals("ID")){
                    System.out.println("EL MONITOR NO HA ENVIADO SU ID: " + cadena);
                }
                else{
                    //LA PRIMERA VEZ SE GUARDA EL ID. AL RECONECTAR TIENE QUE SER EL MISMO:
                    //EL CONSUMIDOR DE KAFKA Y EL FICHERO DEL RIEGO YA ESTAN HECHOS PARA ESE ID
                    if(WM_WS_E.id_estacion == null){
                        WM_WS_E.id_estacion = split[1];
                        System.out.println("SOY EL MOTOR DE LA ESTACION " + split[1]);
                    }
                    else if(!WM_WS_E.id_estacion.equals(split[1])){
                        System.err.println("ESTE MOTOR ES DE LA ESTACION " + WM_WS_E.id_estacion + " Y EL MONITOR ES DE LA " + split[1] + "!!");
                        System.err.println("RELANZA EL MOTOR PARA CAMBIAR DE ESTACION");
                        System.exit(1);
                    }

                    WM_WS_E.sin_monitor = false;
                    System.out.println("CONECTADO AL MONITOR!!");

                    //BUCLE DE SALUD: SE RESPONDE KO SI HAY AVERIA O SI KAFKA TODAVIA NO ESTA LISTO
                    //LA RESPUESTA SOLO SE MUESTRA POR PANTALLA CUANDO CAMBIA, PARA NO LLENARLA CADA SEGUNDO
                    String ultimo_estado = "";
                    String estado;

                    while(true){
                        cadena = WM_WS_E.leeSocket(socket_monitor);

                        if(!cadena.equals("SALUD")){
                            System.out.println("MENSAJE INESPERADO DEL MONITOR: " + cadena);
                            continue;
                        }

                        if(WM_WS_E.averia || !WM_WS_E.kafkaListo()){
                            estado = "KO";
                        }else{
                            estado = "OK";
                        }

                        WM_WS_E.escribeSocket(socket_monitor, estado);

                        if(!estado.equals(ultimo_estado)){
                            if(estado.equals("OK")){
                                System.out.println("ESTADO ENVIADO AL MONITOR: OK");
                            }else if(WM_WS_E.averia){
                                System.out.println("ESTADO ENVIADO AL MONITOR: KO (AVERIA)");
                            }else{
                                System.out.println("ESTADO ENVIADO AL MONITOR: KO (KAFKA TODAVIA NO ESTA LISTO)");
                            }
                            ultimo_estado = estado;
                        }
                    }
                }

            }catch(UnknownHostException e){
                System.err.println("EL NOMBRE DEL HOST NO EXISTE!!");
                System.err.println("Error: " + e.toString());
                System.exit(1);
            }catch(SocketTimeoutException e){
                if(WM_WS_E.sin_monitor){
                    //SE HA CONECTADO PERO NO LE HA LLEGADO EL ID: EL MONITOR ATIENDE A UN SOLO MOTOR A LA VEZ
                    System.out.println("EL MONITOR NO HA ENVIADO EL ID EN 3 SEGUNDOS (PUEDE QUE YA TENGA OTRO MOTOR CONECTADO)");
                }else{
                    System.out.println("EL MONITOR LLEVA 3 SEGUNDOS SIN ENVIAR SALUD!!");
                }
            }catch(IOException e){
                if(WM_WS_E.sin_monitor){
                    System.err.println("IMPOSIBLE CONECTAR CON EL MONITOR!!");
                    System.err.println("Error: " + e.toString());
                }else{
                    System.out.println("SE HA PERDIDO LA CONEXION CON EL MONITOR!!");
                }
            }

            //SIN MONITOR LA ESTACION QUEDA AVERIADA: NO SE ADMITEN RIEGOS, NI SIQUIERA MANUALES, HASTA QUE VUELVA
            if(!WM_WS_E.sin_monitor){
                WM_WS_E.sin_monitor = true;
                System.out.println("ESTACION AVERIADA: SIN MONITOR NO SE PUEDE REGAR");
                //SI ESTA REGANDO, SE CIERRA LA VALVULA Y SE ENVIA EL FIN CON MOTIVO SIN_MONITOR
                WM_WS_E.monitorCaido();
            }

            //ESPERAR 3 SEGUNDOS ANTES DE REINTENTAR
            System.out.println("REINTENTANDO EN 3 SEGUNDOS...");
            try{
                Thread.sleep(3000);
            }catch(InterruptedException e){
                return;
            }
        }
    }
}