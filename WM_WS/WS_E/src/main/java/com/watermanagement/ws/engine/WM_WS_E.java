package com.watermanagement.ws.engine;

import java.io.*;
import java.net.*;

public class WM_WS_E{

    //------------------------------------------------------------------------------------------------------------------------------------
    //ESTADO DEL MOTOR, COMPARTIDO ENTRE HILOS (POR ESO SON volatile)

    //ID DE LA ESTACION: NULL HASTA QUE LO ENVIA EL MONITOR. SIN ID NO SE ARRANCA KAFKA NI SE RIEGA
    public static volatile String id_estacion = null;

    //TRUE MIENTRAS NO HAY MONITOR (AL ARRANCAR Y TRAS 3 SEGUNDOS SIN SALUD): ESTACION "AVERIADA", NO SE ADMITEN RIEGOS
    public static volatile boolean sin_monitor = true;

    //AVERIA SIMULADA: LA CAMBIARA UNA TECLA DEL MENU (SIGUIENTE PASO). CON TRUE SE RESPONDE KO A SALUD
    public static volatile boolean averia = false;

    //PENDIENTE (KAFKA): bloqueada (LA CAMBIAN BLOQUEAR Y ACTIVAR) Y riego_actual (NULL = VALVULA CERRADA)
    //public static volatile boolean bloqueada = false;
    //public static Riego riego_actual = null;

    //PENDIENTE (KAFKA): HILO CONSUMIDOR DE ordenes-ws. LO USA kafkaListo()
    //public static volatile Hilo_Kafka_Engine hilo_kafka = null;

    //------------------------------------------------------------------------------------------------------------------------------------
    //TRUE CUANDO EL MOTOR YA PUEDE RECIBIR ORDENES DE CENTRAL
    //HASTA ENTONCES SE RESPONDE KO A SALUD: SI NO, CENTRAL PODRIA ENVIAR UN INICIAR QUE NUNCA SE LEERIA
    public static boolean kafkaListo(){
        //PENDIENTE (KAFKA): return hilo_kafka != null && hilo_kafka.estaListo();
        return true; //PROVISIONAL: MIENTRAS NO HAYA KAFKA, PARA PROBAR LOS SOCKETS
    }

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

    public static void validacionArgumentos(String args[], BufferedReader buffer, String argumentos[]){

        if(args.length != 4){
            try{
                System.out.println("Introduce la direccion de Kafka: ");
                argumentos[0] = buffer.readLine();
                System.out.println("Introduce el puerto de escucha de Kafka: ");
                argumentos[1] = buffer.readLine();
                System.out.println("Introduce la direccion del Monitor: ");
                argumentos[2] = buffer.readLine();
                System.out.println("Introduce el puerto de escucha del Monitor: ");
                argumentos[3] = buffer.readLine();
            }catch(Exception e){
                System.err.println("Error: " + e.toString());
            }
        }else{
            argumentos[0]=args[0];
            argumentos[1]=args[1];
            argumentos[2]=args[2];
            argumentos[3]=args[3];
        }
    }

    public static void main(String args[]){

        InputStreamReader reader = new InputStreamReader(System.in);
        BufferedReader buffer = new BufferedReader(reader);

        String[] argumentos = new String[4];

        //--------------------------------------------------------------------------------------------------------------------------------
        //VALIDAR ARGUMENTOS
        validacionArgumentos(args, buffer, argumentos);

        //VALIDACION DE PUERTOS
        int puerto_kafka;
        int puerto_monitor;
        try{
            puerto_kafka = Integer.parseInt(argumentos[1]);
            puerto_monitor = Integer.parseInt(argumentos[3]);
        }catch(NumberFormatException e){
            System.err.println("LOS PUERTOS TIENEN QUE SER NUMEROS!!");
            System.exit(1);
            return;
        }

        System.out.println("Kafka se encuentra en: " + argumentos[0] + ":" + puerto_kafka);
        System.out.println("El monitor se encuentra en: " + argumentos[2] + ":" + puerto_monitor);

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO DEL MONITOR: SE LANZA LO PRIMERO Y DURA TODO EL PROGRAMA
        //SE CONECTA AL MONITOR, RECIBE EL ID Y RESPONDE A SALUD. SI EL MONITOR SE CAE, SE VUELVE A CONECTAR
        //TIENE QUE ESTAR RESPONDIENDO A SALUD MIENTRAS SE PREPARA KAFKA: SI NO, EL MONITOR NOS DARIA POR CAIDOS
        Thread hilo_monitor = new Hilo_Monitor(argumentos[2], puerto_monitor);
        hilo_monitor.start();

        //--------------------------------------------------------------------------------------------------------------------------------
        //ESPERAR EL ID: SIN EL NO SE SABE QUE ORDENES SON NUESTRAS NI COMO SE LLAMA EL FICHERO DEL RIEGO
        try{
            while(id_estacion == null){
                Thread.sleep(200);
            }
        }catch(InterruptedException e){
            return;
        }

        //--------------------------------------------------------------------------------------------------------------------------------
        //PENDIENTE (KAFKA): CREAR UN PRODUCTOR DE KAFKA COMPARTIDO
        //String servidor_kafka = argumentos[0] + ":" + puerto_kafka;
        //ProductorKafka productor = new ProductorKafka(servidor_kafka);

        //--------------------------------------------------------------------------------------------------------------------------------
        //PENDIENTE (KAFKA): REVISAR SI QUEDO UN RIEGO A MEDIAS
        //SI EXISTE riego-<ID_ESTACION>.txt (idPeticion#idOperario#segundosRegados#volumen):
        //  productor.enviar("telemetria-ws", id_estacion, "FIN#" + id_estacion + "#ID_PETICION#ID_OPERARIO#VOLUMEN#SEGUNDOS#CAIDA");
        //  Y BORRAR EL FICHERO

        //--------------------------------------------------------------------------------------------------------------------------------
        //PENDIENTE (KAFKA): HILO CONSUMIDOR DE ORDENES (ordenes-ws)
        //MIENTRAS NO TENGA PARTICIONES ASIGNADAS, kafkaListo() DEVUELVE FALSE Y SE RESPONDE KO A SALUD
        //hilo_kafka = new Hilo_Kafka_Engine(servidor_kafka, id_estacion, productor);
        //hilo_kafka.start();

        //--------------------------------------------------------------------------------------------------------------------------------
        //MENU (SIGUIENTE PASO): AVERIA SI/NO, RIEGO MANUAL Y PARAR EL RIEGO
    }
}