package com.watermanagement.ws.monitor;

import java.io.*;
import java.net.*;

public class WM_WS_M{

    //ESTADO DEL MOTOR: TRUE SI ESTA CONECTADO Y SU ULTIMA RESPUESTA A SALUD FUE OK
    //LO ACTUALIZA Hilo_Motor Y LO LEE EL BUCLE DE VIDA
    //ES volatile PORQUE LO ESCRIBE UN HILO Y LO LEE OTRO
    //MIENTRAS NO HAYA MOTOR ES FALSE: SE ENVIA VIDA CON KO Y CENTRAL VE LA ESTACION AVERIADA (ROJO), COMO PIDE LA GUIA
    public static volatile boolean engine_ok = false;

    //CADA MENSAJE VIAJA EN UNA TRAMA <STX><DATOS><ETX><LRC> Y SE CONFIRMA CON ACK O NACK (CLASE Trama)
    //LOS USAN EL BUCLE DE VIDA (CON CENTRAL) Y Hilo_Motor (CON EL MOTOR)
    public static void escribeSocket (Socket socket, String cadena) throws IOException{
        Trama.enviar(socket, cadena);
    }

    public static String leeSocket(Socket socket) throws IOException{
        return Trama.recibir(socket);
    }

    public static void main(String args[]){

        //--------------------------------------------------------------------------------------------------------------------------------
        //VALIDAR ARGUMENTOS
        if(args.length < 4 || args.length > 5){
            System.err.println("ARGUMENTOS INCORRECTOS!!");
            System.err.println("USO: /puerto_escucha_motor /IP_central /puerto_escucha_central /ID_estacion /ubicacion(necesario para registrar la estacion y entre comillas)");
            System.exit(1);
        }

        //VALIDACION DE PUERTOS
        int puerto_motor;
        int puerto_central;
        try{
            puerto_motor = Integer.parseInt(args[0]);
            puerto_central = Integer.parseInt(args[2]);
        }catch(NumberFormatException e){
            System.err.println("LOS PUERTOS TIENEN QUE SER NUMEROS!!");
            System.exit(1);
            return;
        }

        //VALIDACION CARACTER #
        if(args[3].contains("#") || (args.length == 5 && args[4].contains("#"))){
            System.err.println("NI EL ID NI LA UBICACION PUEDEN CONTENER #!!");
            System.exit(1);
        }

        System.out.println(("El monitor escucha al motor por el puerto: " + puerto_motor));
        System.out.println("El sistema central se encuentra en: " + args[1] + ":" + puerto_central);
        System.out.println("El identificador de la estacion es: " + args[3]);

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO DEL MOTOR: SE LANZA UNA SOLA VEZ, AQUI, ANTES DEL BUCLE DE CONEXION CON CENTRAL
        //ASI EL MOTOR SE PUEDE CONECTAR AUNQUE CENTRAL NO ESTE, Y AL RECONECTAR CON CENTRAL NO SE VUELVE A ABRIR EL PUERTO
        Thread hilo_motor = new Hilo_Motor(puerto_motor, args[3]);
        hilo_motor.start();

        //--------------------------------------------------------------------------------------------------------------------------------
        //CONEXION CON CENTRAL
        //SI CENTRAL NO ESTA O SE CAE, SE AVISA Y SE REINTENTA. LOS KO DE CENTRAL TERMINAN EL PROGRAMA
        String cadena;
        String[] split;

        while(true){
            //try-with-resources: EL SOCKET SE CIERRA SOLO AL SALIR DEL try, PASE LO QUE PASE
            try(Socket socket_cliente_central = new Socket(args[1], puerto_central)){

                //SI CENTRAL TARDA MAS DE 3 SEGUNDOS EN RESPONDER, leeSocket LANZA SocketTimeoutException
                //(VALE PARA EL ENQ, EL ALTA, LA AUTENTICACION Y LOS OK DE CADA VIDA)
                socket_cliente_central.setSoTimeout(3000);

                //EL MONITOR ES EL CLIENTE: ABRE LA COMUNICACION CON ENQ Y CENTRAL LE CONTESTA ACK
                Trama.enviarEnq(socket_cliente_central);

                //ALTA (SOLO SI SE HA PASADO LA UBICACION)
                if(args.length == 5){
                    escribeSocket(socket_cliente_central, "ALTA#" + args[3] + "#" + args[4]);
                    cadena = leeSocket(socket_cliente_central);
                    if(cadena.equals("OK")){
                        System.out.println("ALTA CORRECTA!!");
                    }
                    else if(cadena.equals("KO#YA_REGISTRADA")){
                        System.out.println("LA ESTACION YA ESTABA DADA DE ALTA");
                    }
                    else{
                        split = cadena.split("#");
                        System.out.println("ERROR EN EL ALTA POR: " + split[1]);
                        //FIN ORDENADO DE LA COMUNICACION ANTES DE TERMINAR
                        Trama.enviarEot(socket_cliente_central);
                        System.exit(1);
                    }
                }

                //AUTENTICACION (SIEMPRE)
                escribeSocket(socket_cliente_central, "AUTENTICAR#" + args[3]);
                cadena = leeSocket(socket_cliente_central);
                if(cadena.equals("OK")){
                    System.out.println("AUTENTICACION CORRECTA!!");
                }
                else{
                    split = cadena.split("#");
                    System.out.println("ERROR EN LA AUTENTICACION POR: " + split[1]);
                    //FIN ORDENADO DE LA COMUNICACION ANTES DE TERMINAR
                    Trama.enviarEot(socket_cliente_central);
                    System.exit(1);
                }

                //BUCLE DE VIDA: CADA SEGUNDO SE ENVIA VIDA#ID#OK O VIDA#ID#KO SEGUN EL MOTOR Y SE ESPERA EL OK DE CENTRAL
                //EL ESTADO SOLO SE MUESTRA POR PANTALLA CUANDO CAMBIA, PARA NO LLENARLA CADA SEGUNDO
                String ultimo_estado = "";
                String estado;

                while(true){
                    if(engine_ok){
                        estado = "OK";
                    }else{
                        estado = "KO";
                    }

                    escribeSocket(socket_cliente_central, "VIDA#" + args[3] + "#" + estado);
                    cadena = leeSocket(socket_cliente_central);

                    if(!cadena.equals("OK")){
                        System.out.println("RESPUESTA INESPERADA DE CENTRAL: " + cadena);
                    }

                    if(!estado.equals(ultimo_estado)){
                        if(estado.equals("OK")){
                            System.out.println("ESTADO ENVIADO A CENTRAL: OK (MOTOR FUNCIONANDO)");
                        }else{
                            System.out.println("ESTADO ENVIADO A CENTRAL: KO (MOTOR CAIDO O AVERIADO)");
                        }
                        ultimo_estado = estado;
                    }

                    Thread.sleep(1000);
                }

            }catch(UnknownHostException e){
                System.err.println("EL NOMBRE DEL HOST NO EXISTE!!");
                System.err.println("Error: " + e.toString());
                System.exit(1);
            }catch(IOException e){
                System.err.println("IMPOSIBLE CONECTAR CON WM_CENTRAL!!");
                System.err.println("Error: " + e.toString());
            }catch(InterruptedException e){
                return;
            }

            //ESPERAR 3 SEGUNDOS ANTES DE REINTENTAR: CENTRAL TARDA HASTA 3 SEGUNDOS EN DARSE CUENTA
            //DE QUE NOS HEMOS IDO, Y SI VOLVEMOS ANTES RESPONDERIA KO#YA_CONECTADA
            System.out.println("REINTENTANDO EN 3 SEGUNDOS...");
            try{
                Thread.sleep(3000);
            }catch(InterruptedException e){
                return;
            }
        }
    }

}