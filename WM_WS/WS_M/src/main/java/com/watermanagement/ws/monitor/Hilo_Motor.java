package com.watermanagement.ws.monitor;

import java.io.*;
import java.net.*;

//HILO QUE HABLA CON EL MOTOR (WM_WS_E)
//SE LANZA UNA SOLA VEZ AL ARRANCAR EL MONITOR Y DURA TODO EL PROGRAMA:
//  - ABRE EL ServerSocket Y ESPERA A QUE SE CONECTE EL MOTOR
//  - LE ENVIA ID#ID_ESTACION PARA QUE SEPA QUE ESTACION ES
//  - CADA SEGUNDO LE ENVIA SALUD Y GUARDA SU RESPUESTA EN WM_WS_M.engine_ok, QUE LEE EL BUCLE DE VIDA
//  - SI EL MOTOR SE CAE O DEJA DE RESPONDER, PONE engine_ok A FALSE Y VUELVE A ESPERAR A QUE SE CONECTE
public class Hilo_Motor extends Thread{

    public int puerto_motor;
    public String id_estacion;

    public Hilo_Motor(int puerto_motor, String id_estacion){
        this.puerto_motor = puerto_motor;
        this.id_estacion = id_estacion;
    }

    public void run(){
        ServerSocket socket_servidor;

        //SI EL PUERTO ESTA OCUPADO, EL MONITOR NO PUEDE VIGILAR AL MOTOR: SE TERMINA EL PROGRAMA
        try{
            socket_servidor = new ServerSocket(puerto_motor);
        }catch(IOException e){
            System.err.println("NO SE PUEDE ESCUCHAR AL MOTOR EN EL PUERTO " + puerto_motor + "!!");
            System.err.println("Error: " + e.toString());
            System.exit(1);
            return;
        }

        String cadena;

        while(true){
            System.out.println("ESPERANDO A QUE SE CONECTE EL MOTOR POR EL PUERTO " + puerto_motor + "...");

            //try-with-resources: EL SOCKET DEL MOTOR SE CIERRA SOLO AL SALIR DEL try, PASE LO QUE PASE
            try(Socket socket_motor = socket_servidor.accept()){

                //SI EL MOTOR TARDA MAS DE 3 SEGUNDOS EN RESPONDER A SALUD, leeSocket LANZA SocketTimeoutException
                socket_motor.setSoTimeout(3000);
                System.out.println("MOTOR CONECTADO DESDE " + socket_motor.getInetAddress().getHostAddress());

                //EL MOTOR NO SABE QUE ESTACION ES HASTA QUE SE LO DICE EL MONITOR
                WM_WS_M.escribeSocket(socket_motor, "ID#" + id_estacion);

                //BUCLE DE SALUD: CADA SEGUNDO SE PREGUNTA AL MOTOR Y SE GUARDA SU RESPUESTA
                //NO SE MUESTRA NADA AQUI: EL BUCLE DE VIDA YA AVISA CUANDO CAMBIA EL ESTADO
                while(true){
                    WM_WS_M.escribeSocket(socket_motor, "SALUD");
                    cadena = WM_WS_M.leeSocket(socket_motor);

                    if(cadena.equals("OK")){
                        WM_WS_M.engine_ok = true;
                    }
                    else if(cadena.equals("KO")){
                        WM_WS_M.engine_ok = false;
                    }
                    else{
                        System.out.println("RESPUESTA INESPERADA DEL MOTOR: " + cadena);
                        WM_WS_M.engine_ok = false;
                    }

                    Thread.sleep(1000);
                }

            }catch(SocketTimeoutException e){
                System.out.println("EL MOTOR LLEVA 3 SEGUNDOS SIN RESPONDER A SALUD!!");
            }catch(IOException e){
                System.out.println("SE HA PERDIDO LA CONEXION CON EL MOTOR!!");
            }catch(InterruptedException e){
                return;
            }finally{
                //SIN MOTOR, LA SIGUIENTE VIDA SALE CON KO Y CENTRAL VE LA ESTACION AVERIADA
                WM_WS_M.engine_ok = false;
            }
        }
    }
}