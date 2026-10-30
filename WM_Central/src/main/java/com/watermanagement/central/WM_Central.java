package com.watermanagement.central;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.watermanagement.central.db.DatabaseManager;
import com.watermanagement.central.kafka.ProductorKafka;

public class WM_Central{

    public static void main(String args[]){

        //--------------------------------------------------------------------------------------------------------------------------------
        //VALIDAR ARGUMENTOS
        if(args.length != 3){
            System.err.println("ARGUMENTOS INCORRECTOS!!");
            System.err.println("USO: /puerto_escucha_monitor /IP_kafka /puerto_escucha_kafka");
            System.exit(1);
        }

        //VALIDACION DE PUERTOS
        try{
            Integer.parseInt(args[0]);
            Integer.parseInt(args[2]);
        }catch(NumberFormatException e){
            System.err.println("LOS PUERTOS TIENEN QUE SER NUMEROS!!");
            System.exit(1);
        }
        
        System.out.println(("El sistema central escucha al monitor por el puerto: " + args[0]));
        System.out.println("El Kafka se encuentra en: " + args[1] + ":" + args[2]);

        String puerto = args[0];
        
        //--------------------------------------------------------------------------------------------------------------------------------
        //ABRIR LA BASE DE DATOS, CREAR TABLAS SI NO EXISTEN
        //(LOS OPERARIOS NO SE INSERTAN AQUI: CADA UNO SE DA DE ALTA CON SU PRIMERA PETICION)
        String rutaDB = System.getenv().getOrDefault("WM_DB_PATH", "watermanagement.db");
        DatabaseManager dbmg = new DatabaseManager(rutaDB);

        //--------------------------------------------------------------------------------------------------------------------------------
        //CARGAR LAS ESTACIONES DE LA BBDD A MEMORIA
        Map<String, Estacion> registro = new ConcurrentHashMap<>();
        List<Estacion> estaciones = new ArrayList<>();

        try{
            dbmg.crearTablas();
            estaciones = dbmg.cargarEstaciones();
            
            for (Estacion e : estaciones){
                registro.put(e.idEstacion, e);
            }
            System.out.println("SE HA CARGADO LA BASE DE DATOS CORRECTAMENTE!!");
            //LAS ESTACIONES YA REGISTRADAS, CON SU UBICACION. HASTA QUE CONECTE SU MONITOR, TODAS ESTAN DESCONECTADAS
            mostrarEstaciones(registro);
        }catch(Exception e){
            System.err.println("ERROR AL INICIAR LA BASE DE DATOS!!");
            System.err.println("Error: " + e.toString());
            System.exit(1);
        }
        
        //--------------------------------------------------------------------------------------------------------------------------------
        //ANOTAR HORA DE ARRANQUE, COMPROBAR PARA CADA RIEGO QUE EL TIEMPO ES POSTERIOR
        long horaArranque = System.currentTimeMillis();

        //--------------------------------------------------------------------------------------------------------------------------------
        //CREAR UN PRODUCTOR DE KAFKA COMPARTIDO (LO USAN TODOS LOS HILOS)
        String servidor_kafka = args[1] + ":" + args[2];
        ProductorKafka productor = new ProductorKafka(servidor_kafka);

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO QUE ACEPTA CONEXIONES DE ESTACIONES
        Thread aceptador = new Hilo_Acept_Sock(puerto, dbmg, registro, productor);
        System.out.println("SE ACABA DE DELEGAR LA ACEPTACIÓN DE ESTACIONES A OTRO HILO!!");
        aceptador.start();

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO CONSUMIDOR DE KAFKA: PETICIONES DE RIEGO Y TELEMETRIA DE LAS ESTACIONES
        Thread consumidor = new Hilo_Kafka_Central(servidor_kafka, dbmg, registro, productor, horaArranque);
        consumidor.start();

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO DEL LATIDO: ESTADO DE LAS ESTACIONES PARA LOS FO CADA 2 SEGUNDOS Y RESERVAS SIN RESPUESTA DEL ENGINE
        Thread latido = new Hilo_Latido(registro, productor);
        latido.start();

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO PRINCIPAL: MENU DE CENTRAL PARA ENVIAR ORDENES A UNA ESTACION O A TODAS (INICIAR RIEGO, BLOQUEAR Y ACTIVAR)
        Ordenes ordenes = new Ordenes(registro, dbmg, productor);
        menu(ordenes, registro);
    }

    //====================================================================================================================================
    //MENU (TECLADO). SI NO HAY TECLADO (DOCKER SIN -it), CENTRAL SIGUE FUNCIONANDO SIN MENU
    //PARA USARLO EN DOCKER: docker attach central (SALIR SIN PARARLO: Ctrl+P Y DESPUES Ctrl+Q)
    public static void menu(Ordenes ordenes, Map<String, Estacion> registro){
        InputStreamReader reader = new InputStreamReader(System.in);
        BufferedReader buffer = new BufferedReader(reader);

        String opcion;
        String id_estacion;
        String texto;
        int duracion;

        try{
            while(true){
                System.out.println();
                System.out.println("----- WM_CENTRAL -----");
                System.out.println("1. VER ESTACIONES");
                System.out.println("2. INICIAR UN RIEGO");
                System.out.println("3. BLOQUEAR (FUERA DE SERVICIO)");
                System.out.println("4. ACTIVAR");
                System.out.println("OPCION: ");

                opcion = buffer.readLine();
                if(opcion == null){
                    System.out.println("NO HAY TECLADO: CENTRAL SIGUE FUNCIONANDO SIN MENU");
                    return;
                }

                switch(opcion.trim()){
                    case "1":
                        mostrarEstaciones(registro);
                    break;

                    case "2":
                        id_estacion = pedirEstacion(buffer);
                        if(id_estacion == null){
                            break;
                        }
                        System.out.println("DURACION EN SEGUNDOS: ");
                        texto = buffer.readLine();
                        if(texto == null){
                            break;
                        }
                        try{
                            duracion = Integer.parseInt(texto.trim());
                        }catch(NumberFormatException e){
                            System.out.println("LA DURACION TIENE QUE SER UN NUMERO!!");
                            break;
                        }
                        ordenes.iniciarRiego(id_estacion, duracion);
                    break;

                    case "3":
                        id_estacion = pedirEstacion(buffer);
                        if(id_estacion != null){
                            ordenes.bloquear(id_estacion);
                        }
                    break;

                    case "4":
                        id_estacion = pedirEstacion(buffer);
                        if(id_estacion != null){
                            ordenes.activar(id_estacion);
                        }
                    break;

                    default:
                        System.out.println("OPCION INCORRECTA!!");
                }
            }
        }catch(IOException e){
            System.err.println("Error: " + e.toString());
        }
    }

    //PIDE EL ID DE LA ESTACION, O TODAS. DEVUELVE NULL SI NO SE HA ESCRITO NADA VALIDO
    public static String pedirEstacion(BufferedReader buffer) throws IOException{
        System.out.println("ID DE LA ESTACION (" + Ordenes.TODAS + " PARA TODAS): ");
        String id_estacion = buffer.readLine();
        if(id_estacion == null){
            return null;
        }
        id_estacion = id_estacion.trim();
        if(id_estacion.isEmpty() || id_estacion.contains("#")){
            System.out.println("EL ID DE LA ESTACION NO PUEDE ESTAR VACIO NI CONTENER #!!");
            return null;
        }
        return id_estacion;
    }

    //MUESTRA TODAS LAS ESTACIONES ORDENADAS POR ID: UBICACION, ESTADO Y, SI ESTA REGANDO, OPERARIO, CAUDAL Y VOLUMEN
    public static void mostrarEstaciones(Map<String, Estacion> registro){
        if(registro.isEmpty()){
            System.out.println("NO HAY NINGUNA ESTACION REGISTRADA TODAVIA");
            return;
        }

        Estacion e;
        EstadoWS estado;
        Riego riego;
        String linea;

        System.out.println("ESTACIONES REGISTRADAS: " + registro.size());
        for(String id : new TreeSet<>(registro.keySet())){
            e = registro.get(id);
            estado = e.getEstado();
            riego = e.getRiego();

            linea = "  " + e.idEstacion + " - " + e.ubicacion + " - " + estado;
            if(estado == EstadoWS.REGANDO && riego != null){
                linea = linea + " (" + riego.idOperario + ": " + String.format(Locale.ROOT, "%.2f", riego.caudal) + " L/MIN, "
                        + String.format(Locale.ROOT, "%.2f", riego.volumen) + " LITROS)";
            }
            //BLOQUEADA PERO TODAVIA EN FUGA O DESCONECTADA: PASARA A FUERA_DE_SERVICIO AL RESOLVERSE
            if(e.estaBloqueada() && estado != EstadoWS.FUERA_DE_SERVICIO){
                linea = linea + " (BLOQUEADA)";
            }
            System.out.println(linea);
        }
    }
}