package com.watermanagement.ws.engine;

import java.io.*;
import java.net.*;
import java.util.*;

import com.watermanagement.ws.engine.kafka.ProductorKafka;

public class WM_WS_E{

    //------------------------------------------------------------------------------------------------------------------------------------
    //ESTADO DEL MOTOR, COMPARTIDO ENTRE HILOS (POR ESO SON volatile)

    //ID DE LA ESTACION: NULL HASTA QUE LO ENVIA EL MONITOR. SIN ID NO SE ARRANCA KAFKA NI SE RIEGA
    public static volatile String id_estacion = null;

    //TRUE MIENTRAS NO HAY MONITOR (AL ARRANCAR Y TRAS 3 SEGUNDOS SIN SALUD): ESTACION "AVERIADA", NO SE ADMITEN RIEGOS
    public static volatile boolean sin_monitor = true;

    //AVERIA SIMULADA (FUGA): LA CAMBIA EL MENU. CON TRUE SE RESPONDE KO A SALUD
    public static volatile boolean averia = false;

    //LA CAMBIAN LAS ORDENES BLOQUEAR Y ACTIVAR DE CENTRAL. CON TRUE LA ESTACION ESTA "FUERA DE SERVICIO"
    public static volatile boolean bloqueada = false;

    //RIEGO EN MARCHA (NULL = VALVULA CERRADA). SOLO SE TOCA DENTRO DE LOS METODOS synchronized DE ESTA CLASE,
    //PORQUE LO USAN VARIOS HILOS: EL CONSUMIDOR DE ORDENES, EL HILO DEL RIEGO, EL MENU Y EL HILO DEL MONITOR
    public static Riego riego_actual = null;

    //PRODUCTOR DE KAFKA COMPARTIDO Y HILO CONSUMIDOR DE ordenes-ws
    public static ProductorKafka productor = null;
    public static volatile Hilo_Kafka_Engine hilo_kafka = null;

    //CARPETA DEL FICHERO riego-<ID_ESTACION>.txt (VARIABLE DE ENTORNO WM_RIEGO_DIR; EN DOCKER, EL VOLUMEN /data)
    public static String carpeta_riegos = ".";

    //------------------------------------------------------------------------------------------------------------------------------------
    //TRUE CUANDO EL MOTOR YA PUEDE RECIBIR ORDENES DE CENTRAL
    //HASTA ENTONCES SE RESPONDE KO A SALUD: SI NO, CENTRAL PODRIA ENVIAR UN INICIAR QUE NUNCA SE LEERIA
    public static boolean kafkaListo(){
        Hilo_Kafka_Engine h = hilo_kafka;
        return h != null && h.estaListo();
    }

    //CADA MENSAJE VIAJA EN UNA TRAMA <STX><DATOS><ETX><LRC> Y SE CONFIRMA CON ACK O NACK (CLASE Trama)
    //LOS USA Hilo_Monitor PARA EL ID Y LOS SALUD
    public static void escribeSocket(Socket socket, String cadena) throws IOException{
        Trama.enviar(socket, cadena);
    }

    public static String leeSocket(Socket socket) throws IOException{
        return Trama.recibir(socket);
    }

    //DECIMALES SIEMPRE CON PUNTO (CENTRAL LOS LEE CON Double.parseDouble): CON EL FORMATO POR DEFECTO, UN WINDOWS EN ESPAÑOL PONDRIA COMA
    public static String numero(double valor){
        return String.format(Locale.ROOT, "%.2f", valor);
    }

    //====================================================================================================================================
    //RIEGOS. TODOS LOS METODOS QUE TOCAN riego_actual SON static synchronized: SE EJECUTAN DE UNO EN UNO

    //MOTIVO POR EL QUE NO SE PUEDE REGAR AHORA, O NULL SI SE PUEDE
    public static synchronized String motivoNoRegar(){
        if(sin_monitor){
            return "ESTACION_SIN_MONITOR";
        }
        if(averia){
            return "ESTACION_AVERIADA";
        }
        if(bloqueada){
            return "ESTACION_FUERA_DE_SERVICIO";
        }
        if(riego_actual != null){
            return "ESTACION_OCUPADA";
        }
        return null;
    }

    //INICIAR: SI SE PUEDE REGAR, ABRE LA VALVULA, ENVIA INICIADO Y ARRANCA EL HILO DEL RIEGO. SI NO, ENVIA RECHAZADO
    public static synchronized void iniciarRiego(String id_peticion, String id_operario, int duracion){
        String motivo = motivoNoRegar();
        if(motivo == null && duracion <= 0){
            motivo = "DURACION_INCORRECTA";
        }

        if(motivo != null){
            System.out.println("RIEGO RECHAZADO POR: " + motivo);
            //RECHAZADO#ID_ESTACION#ID_PETICION#MOTIVO
            productor.enviar("telemetria-ws", id_estacion, "RECHAZADO#" + id_estacion + "#" + id_peticion + "#" + motivo);
            return;
        }

        riego_actual = new Riego(id_peticion, id_operario, duracion);
        System.out.println("VALVULA ABIERTA: RIEGO DE " + duracion + " SEGUNDOS PARA " + id_operario);
        //INICIADO#ID_ESTACION#ID_PETICION
        productor.enviar("telemetria-ws", id_estacion, "INICIADO#" + id_estacion + "#" + id_peticion);
        guardarFichero(riego_actual);

        Thread hilo_riego = new Hilo_Riego(riego_actual);
        hilo_riego.start();
    }

    //UN SEGUNDO DE RIEGO (LO LLAMA Hilo_Riego): CAUDAL Y VOLUMEN, DATOS Y FICHERO. SI SE LLEGA AL TIEMPO, FIN
    //DEVUELVE FALSE CUANDO ESE RIEGO YA NO ESTA EN MARCHA, PARA QUE EL HILO TERMINE
    public static synchronized boolean segundoDeRiego(Riego riego){
        //LO HAN PARADO MIENTRAS EL HILO DORMIA (CENTRAL, EL MENU, UNA AVERIA, SIN MONITOR...)
        if(riego_actual != riego){
            return false;
        }

        riego.segundosRegados++;
        riego.caudal = 8 + Math.random() * 4;
        riego.volumen = riego.volumen + riego.caudal / 60;

        System.out.println("REGANDO: " + riego.segundosRegados + " DE " + riego.duracionMaxima + " SEGUNDOS, "
                           + numero(riego.caudal) + " L/MIN, " + numero(riego.volumen) + " LITROS");

        //DATOS#ID_ESTACION#ID_PETICION#ID_OPERARIO#CAUDAL#VOLUMEN
        productor.enviar("telemetria-ws", id_estacion, "DATOS#" + id_estacion + "#" + riego.idPeticion + "#" + riego.idOperario
                                                       + "#" + numero(riego.caudal) + "#" + numero(riego.volumen));
        guardarFichero(riego);

        if(riego.segundosRegados >= riego.duracionMaxima){
            terminarRiego("TIEMPO");
            return false;
        }
        return true;
    }

    //CIERRA LA VALVULA Y ENVIA EL FIN CON EL MOTIVO. SI EL FIN SE HA ENVIADO, BORRA EL FICHERO
    //DEVUELVE FALSE SI NO HABIA NINGUN RIEGO EN MARCHA
    public static synchronized boolean terminarRiego(String motivo){
        if(riego_actual == null){
            return false;
        }

        Riego riego = riego_actual;
        riego_actual = null;
        System.out.println("VALVULA CERRADA (" + motivo + "): " + numero(riego.volumen) + " LITROS EN " + riego.segundosRegados + " SEGUNDOS");

        //FIN#ID_ESTACION#ID_PETICION#ID_OPERARIO#VOLUMEN#DURACION#MOTIVO
        if(productor.enviar("telemetria-ws", id_estacion, "FIN#" + id_estacion + "#" + riego.idPeticion + "#" + riego.idOperario
                                                          + "#" + numero(riego.volumen) + "#" + riego.segundosRegados + "#" + motivo)){
            borrarFichero();
        }else{
            System.out.println("NO SE HA PODIDO ENVIAR EL FIN: SE QUEDA EN EL FICHERO Y SE ENVIARA AL VOLVER A ARRANCAR");
        }
        return true;
    }

    //BLOQUEAR (ORDEN DE CENTRAL): FUERA DE SERVICIO. SI ESTABA REGANDO, PARA CON MOTIVO BLOQUEO
    public static synchronized void bloquear(){
        bloqueada = true;
        System.out.println("ESTACION FUERA DE SERVICIO");
        terminarRiego("BLOQUEO");
    }

    //ACTIVAR (ORDEN DE CENTRAL): VUELVE A ESTAR DISPONIBLE
    public static synchronized void activar(){
        bloqueada = false;
        System.out.println("ESTACION ACTIVADA");
    }

    //MENU: SIMULA O REPARA UNA AVERIA (FUGA). SI ESTABA REGANDO, PARA EN EL ACTO CON MOTIVO FUGA
    //EL MONITOR LO NOTA EN EL SIGUIENTE SALUD (KO) Y AVISA A CENTRAL
    public static synchronized void cambiarAveria(){
        averia = !averia;
        if(averia){
            System.out.println("AVERIA (FUGA) SIMULADA!!");
            terminarRiego("FUGA");
        }else{
            System.out.println("AVERIA REPARADA");
        }
    }

    //HILO DEL MONITOR: SIN MONITOR NO SE PUEDE REGAR. SI ESTABA REGANDO, PARA CON MOTIVO SIN_MONITOR
    public static synchronized void monitorCaido(){
        terminarRiego("SIN_MONITOR");
    }

    //ESTADO PARA MOSTRAR EN PANTALLA
    public static synchronized String estado(){
        if(sin_monitor){
            return "AVERIADA (SIN MONITOR)";
        }
        if(averia){
            return "AVERIADA (FUGA)";
        }
        if(bloqueada){
            return "FUERA DE SERVICIO";
        }
        if(riego_actual != null){
            return "REGANDO (" + riego_actual.segundosRegados + " DE " + riego_actual.duracionMaxima + " SEGUNDOS, "
                   + numero(riego_actual.volumen) + " LITROS, " + riego_actual.idOperario + ")";
        }
        return "DISPONIBLE";
    }

    //TRUE SI ESTA EN MARCHA EL RIEGO DE ESA PETICION (PARA EL RIEGO MANUAL)
    public static synchronized boolean regando(String id_peticion){
        return riego_actual != null && riego_actual.idPeticion.equals(id_peticion);
    }

    //====================================================================================================================================
    //FICHERO riego-<ID_ESTACION>.txt: UNA LINEA ID_PETICION#ID_OPERARIO#SEGUNDOS_REGADOS#VOLUMEN
    //SE REESCRIBE CADA SEGUNDO MIENTRAS RIEGA Y SE BORRA AL ENVIAR EL FIN. SI EL MOTOR SE CAE, AL VOLVER SE ENVIA EL FIN (CAIDA)
    //EL ID VA EN EL NOMBRE PORQUE PUEDE HABER VARIAS ESTACIONES EN EL MISMO PC (O EN EL MISMO VOLUMEN DE DOCKER)

    public static File fichero(){
        return new File(carpeta_riegos, "riego-" + id_estacion + ".txt");
    }

    public static void guardarFichero(Riego riego){
        //false: SE SOBREESCRIBE ENTERO
        try(PrintWriter fichero = new PrintWriter(new FileWriter(fichero(), false))){
            fichero.println(riego.idPeticion + "#" + riego.idOperario + "#" + riego.segundosRegados + "#" + numero(riego.volumen));
        }catch(IOException e){
            System.err.println("NO SE HA PODIDO GUARDAR EL FICHERO DEL RIEGO!!");
            System.err.println("Error: " + e.toString());
        }
    }

    public static void borrarFichero(){
        File f = fichero();
        if(f.exists() && !f.delete()){
            System.err.println("NO SE HA PODIDO BORRAR EL FICHERO DEL RIEGO: " + f.getPath());
        }
    }

    //AL ARRANCAR: SI QUEDO UN RIEGO A MEDIAS (EL MOTOR SE CAYO REGANDO), SE ENVIA SU FIN CON MOTIVO CAIDA Y SE BORRA EL FICHERO
    //ASI LA ESTACION NO PIERDE LO QUE YA HABIA REGADO, COMO PIDE LA GUIA
    public static void recuperarRiego(){
        File f = fichero();
        if(!f.exists()){
            return;
        }

        String linea = null;
        try(BufferedReader fichero = new BufferedReader(new FileReader(f))){
            linea = fichero.readLine();
        }catch(IOException e){
            System.err.println("NO SE HA PODIDO LEER EL FICHERO DEL RIEGO!!");
            System.err.println("Error: " + e.toString());
            return;
        }
        //EL FICHERO YA ESTA CERRADO: EN WINDOWS NO SE PUEDE BORRAR UN FICHERO ABIERTO

        String[] split;
        if(linea == null || (split = linea.split("#")).length != 4){
            System.out.println("EL FICHERO DEL RIEGO ESTA MAL, SE BORRA: " + linea);
            borrarFichero();
            return;
        }

        System.out.println("QUEDO UN RIEGO A MEDIAS: " + split[3] + " LITROS EN " + split[2] + " SEGUNDOS PARA " + split[1] + ". SE ENVIA SU FIN");
        //FIN#ID_ESTACION#ID_PETICION#ID_OPERARIO#VOLUMEN#DURACION#CAIDA
        if(productor.enviar("telemetria-ws", id_estacion, "FIN#" + id_estacion + "#" + split[0] + "#" + split[1] + "#" + split[3] + "#" + split[2] + "#CAIDA")){
            borrarFichero();
        }else{
            System.out.println("NO SE HA PODIDO ENVIAR EL FIN: SE INTENTARA EN EL PROXIMO ARRANQUE");
        }
    }

    //====================================================================================================================================
    //MENU (TECLADO): AVERIA, RIEGO MANUAL Y PARAR. SI NO HAY TECLADO (DOCKER SIN -it), EL MOTOR SIGUE FUNCIONANDO SIN MENU
    public static void menu(BufferedReader buffer){
        String opcion;

        try{
            while(true){
                System.out.println();
                System.out.println("----- ESTACION " + id_estacion + ": " + estado() + " -----");
                if(averia){
                    System.out.println("1. REPARAR LA AVERIA");
                }else{
                    System.out.println("1. SIMULAR UNA AVERIA (FUGA)");
                }
                System.out.println("2. PEDIR UN RIEGO MANUAL");
                System.out.println("3. PARAR EL RIEGO");
                System.out.println("4. VER EL ESTADO");
                System.out.println("OPCION: ");

                opcion = buffer.readLine();
                if(opcion == null){
                    System.out.println("NO HAY TECLADO: EL MOTOR SIGUE FUNCIONANDO SIN MENU");
                    return;
                }

                switch(opcion.trim()){
                    case "1":
                        cambiarAveria();
                    break;

                    case "2":
                        riegoManual(buffer);
                    break;

                    case "3":
                        //PARADA DESDE EL MENU: LA HACE EL MOTOR DIRECTAMENTE
                        if(!terminarRiego("PARADA_MANUAL")){
                            System.out.println("NO HAY NINGUN RIEGO EN MARCHA");
                        }
                    break;

                    case "4":
                        //EL ESTADO YA SE MUESTRA AL VOLVER AL MENU
                    break;

                    default:
                        System.out.println("OPCION INCORRECTA!!");
                }
            }
        }catch(IOException e){
            System.err.println("Error: " + e.toString());
        }
    }

    //RIEGO MANUAL: TAMBIEN PASA POR CENTRAL, QUE ES QUIEN DECIDE. SE PUBLICA UNA PETICION CON WS COMO OPERARIO
    //Y SE ESPERA HASTA 10 SEGUNDOS A QUE LLEGUE EL INICIAR (CENTRAL NO RESPONDE A LA ESTACION SI DENIEGA)
    public static void riegoManual(BufferedReader buffer) throws IOException{
        //SIN MONITOR NO SE ADMITEN RIEGOS, NI SIQUIERA MANUALES. TAMPOCO AVERIADA, BLOQUEADA O YA REGANDO
        String motivo = motivoNoRegar();
        if(motivo != null){
            System.out.println("NO SE PUEDE REGAR AHORA: " + motivo);
            return;
        }

        System.out.println("DURACION EN SEGUNDOS: ");
        String texto = buffer.readLine();
        if(texto == null){
            return;
        }
        int duracion;
        try{
            duracion = Integer.parseInt(texto.trim());
        }catch(NumberFormatException e){
            System.out.println("LA DURACION TIENE QUE SER UN NUMERO!!");
            return;
        }
        if(duracion <= 0){
            System.out.println("LA DURACION TIENE QUE SER MAYOR QUE 0!!");
            return;
        }

        String id_peticion = UUID.randomUUID().toString();
        //PETICION#ID_PETICION#WS#ID_ESTACION#DURACION
        if(!productor.enviar("peticiones-riego", null, "PETICION#" + id_peticion + "#WS#" + id_estacion + "#" + duracion)){
            System.out.println("IMPOSIBLE CONECTAR CON WM_CENTRAL!! (NO SE HA PODIDO ENVIAR LA PETICION A KAFKA)");
            return;
        }
        System.out.println("PETICION ENVIADA A CENTRAL, ESPERANDO AUTORIZACION...");

        try{
            for(int i = 0; i < 50; i++){
                if(regando(id_peticion)){
                    System.out.println("RIEGO MANUAL AUTORIZADO!!");
                    return;
                }
                Thread.sleep(200);
            }
        }catch(InterruptedException e){
            return;
        }
        System.out.println("CENTRAL NO HA AUTORIZADO EL RIEGO (ESTACION NO DISPONIBLE O IMPOSIBLE CONECTAR CON WM_CENTRAL)");
    }

    public static void main(String args[]){

        //--------------------------------------------------------------------------------------------------------------------------------
        //VALIDAR ARGUMENTOS
        if(args.length != 4){
            System.err.println("ARGUMENTOS INCORRECTOS!!");
            System.err.println("USO: /IP_kafka /puerto_kafka /IP_monitor /puerto_monitor");
            System.exit(1);
        }

        //VALIDACION DE PUERTOS
        int puerto_kafka;
        int puerto_monitor;
        try{
            puerto_kafka = Integer.parseInt(args[1]);
            puerto_monitor = Integer.parseInt(args[3]);
        }catch(NumberFormatException e){
            System.err.println("LOS PUERTOS TIENEN QUE SER NUMEROS!!");
            System.exit(1);
            return;
        }

        System.out.println("Kafka se encuentra en: " + args[0] + ":" + puerto_kafka);
        System.out.println("El monitor se encuentra en: " + args[2] + ":" + puerto_monitor);

        //CARPETA DEL FICHERO DEL RIEGO (EN DOCKER LA FIJA EL Dockerfile: /data)
        carpeta_riegos = System.getenv().getOrDefault("WM_RIEGO_DIR", ".");

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO DEL MONITOR: SE LANZA LO PRIMERO Y DURA TODO EL PROGRAMA
        //SE CONECTA AL MONITOR, RECIBE EL ID Y RESPONDE A SALUD. SI EL MONITOR SE CAE, SE VUELVE A CONECTAR
        //TIENE QUE ESTAR RESPONDIENDO A SALUD MIENTRAS SE PREPARA KAFKA: SI NO, EL MONITOR NOS DARIA POR CAIDOS
        Thread hilo_monitor = new Hilo_Monitor(args[2], puerto_monitor);
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
        //CREAR UN PRODUCTOR DE KAFKA COMPARTIDO
        String servidor_kafka = args[0] + ":" + puerto_kafka;
        productor = new ProductorKafka(servidor_kafka);

        //--------------------------------------------------------------------------------------------------------------------------------
        //REVISAR SI QUEDO UN RIEGO A MEDIAS (ANTES DE ACEPTAR ORDENES NUEVAS)
        recuperarRiego();

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO CONSUMIDOR DE ORDENES (ordenes-ws)
        //MIENTRAS NO TENGA PARTICIONES ASIGNADAS, kafkaListo() DEVUELVE FALSE Y SE RESPONDE KO A SALUD
        hilo_kafka = new Hilo_Kafka_Engine(servidor_kafka, id_estacion);
        hilo_kafka.start();

        //--------------------------------------------------------------------------------------------------------------------------------
        //MENU: AVERIA, RIEGO MANUAL Y PARAR EL RIEGO
        menu(new BufferedReader(new InputStreamReader(System.in)));
    }
}
