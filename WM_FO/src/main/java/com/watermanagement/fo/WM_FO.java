package com.watermanagement.fo;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.common.errors.GroupIdNotFoundException;

import com.watermanagement.fo.kafka.ProductorKafka;

public class WM_FO{

    //------------------------------------------------------------------------------------------------------------------------------------
    //TIEMPOS MAXIMOS (EN MILISEGUNDOS)

    //SI PASAN MAS DE 6 SEGUNDOS SIN NINGUN ESTADO DE CENTRAL (estado-estaciones), CENTRAL HA CAIDO
    public static final long MAX_SIN_LATIDO = 6000;
    //TIEMPO MAXIMO HASTA QUE CENTRAL AUTORIZA O DENIEGA UNA PETICION
    public static final long MAX_SIN_RESPUESTA = 15000;
    //UNA VEZ AUTORIZADO, EL RESUMEN SE ESPERA LA DURACION DEL RIEGO MAS ESTE MARGEN
    public static final long MARGEN_RESUMEN = 15000;
    //ESPERA ENTRE DOS PETICIONES DEL FICHERO (LA PIDE LA GUIA)
    public static final long ESPERA_FICHERO = 4000;

    //------------------------------------------------------------------------------------------------------------------------------------
    //LO QUE USAN TODOS LOS METODOS
    public static String id_operario;
    public static ProductorKafka productor;
    public static Hilo_Kafka_FO hilo_kafka;
    public static boolean cerrado = false;

    public static void validacionArgumentos(String args[], BufferedReader buffer, String argumentos[]){

        //3 ARGUMENTOS: CON MENU. 4 ARGUMENTOS: EL CUARTO ES EL FICHERO DE PETICIONES (SIN MENU)
        if(args.length < 3 || args.length > 4){
            try{
                System.out.println("Introduce la direccion de Kafka: ");
                argumentos[0] = buffer.readLine();
                System.out.println("Introduce el puerto para conectarte a Kafka: ");
                argumentos[1] = buffer.readLine();
                System.out.println("Introduce el ID del operador: ");
                argumentos[2] = buffer.readLine();
            }catch(Exception e){
                System.err.println("Error: " + e.toString());
            }
            argumentos[3] = null;
        }else{
            argumentos[0]=args[0];
            argumentos[1]=args[1];
            argumentos[2]=args[2];
            if(args.length == 4){
                argumentos[3]=args[3];
            }else{
                argumentos[3]=null;
            }
        }
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //TRUE SI HACE MAS DE 6 SEGUNDOS QUE NO LLEGA NINGUN ESTADO DE CENTRAL
    public static boolean centralCaida(){
        return System.currentTimeMillis() - hilo_kafka.ultimo_latido > MAX_SIN_LATIDO;
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //ESPERA A QUE EL CONSUMIDOR DE KAFKA TENGA PARTICIONES ASIGNADAS (COMO MUCHO 15 SEGUNDOS)
    //SI SE PIDE UN RIEGO ANTES, LA RESPUESTA DE CENTRAL PODRIA LLEGAR CUANDO TODAVIA NO ESTAMOS ESCUCHANDO
    public static boolean esperarKafka(){
        long inicio = System.currentTimeMillis();
        try{
            while(!hilo_kafka.estaListo()){
                if(System.currentTimeMillis() - inicio > MAX_SIN_RESPUESTA){
                    return false;
                }
                Thread.sleep(200);
            }
        }catch(InterruptedException e){
            return false;
        }
        return true;
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //COMPRUEBA QUE NO HAYA YA OTRO FO FUNCIONANDO CON ESTE ID. SI LO HAY, TERMINA EL PROGRAMA
    //CADA FO LEE CON EL GRUPO DE KAFKA fo-<ID>, Y KAFKA REPARTE LOS MENSAJES ENTRE LOS MIEMBROS DE UN GRUPO:
    //DOS FO CON EL MISMO ID SE REPARTIRIAN LAS RESPUESTAS Y CADA UNO VERIA SOLO UNA PARTE
    //SE PREGUNTA A KAFKA (CON UN Admin) SI ESE GRUPO YA TIENE MIEMBROS. SE HACE ANTES DE CREAR NUESTRO CONSUMIDOR,
    //PARA NO CONTARNOS A NOSOTROS MISMOS
    //(SI SE LANZAN DOS FO CON EL MISMO ID EXACTAMENTE A LA VEZ, LOS DOS PODRIAN PASAR LA COMPROBACION)
    public static void comprobarIdLibre(String servidor_kafka){
        String grupo = "fo-" + id_operario;

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servidor_kafka);
        //SI KAFKA NO RESPONDE, SE DEJA DE ESPERAR A LOS 15 SEGUNDOS
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) MAX_SIN_RESPUESTA);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        System.out.println("COMPROBANDO QUE NO HAYA OTRO OPERARIO " + id_operario + " CONECTADO...");

        //try-with-resources: EL Admin SE CIERRA SOLO AL SALIR DEL try
        try(Admin admin = Admin.create(props)){
            ConsumerGroupDescription descripcion = admin.describeConsumerGroups(List.of(grupo)).describedGroups().get(grupo).get();

            if(!descripcion.members().isEmpty()){
                System.err.println("YA HAY UN OPERARIO " + id_operario + " CONECTADO!! (EL GRUPO " + grupo + " DE KAFKA YA TIENE MIEMBROS)");
                System.err.println("SI LO ACABAS DE CERRAR DE GOLPE (SIN SALIR NI Ctrl+C), KAFKA TARDA HASTA 45 SEGUNDOS EN DARLO POR CAIDO: ESPERA Y VUELVE A LANZARLO");
                System.exit(1);
            }

        }catch(ExecutionException e){
            //EL GRUPO NO EXISTE: ES LA PRIMERA VEZ QUE SE USA ESTE ID, ASI QUE NO HAY NADIE MAS
            if(e.getCause() instanceof GroupIdNotFoundException){
                return;
            }
            System.err.println("IMPOSIBLE CONECTAR CON KAFKA!! (COMPRUEBA LA DIRECCION Y QUE ESTE ARRANCADO)");
            System.err.println("Error: " + e.getCause().toString());
            System.exit(1);
        }catch(InterruptedException e){
            System.exit(1);
        }
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //PIDE UN RIEGO Y ESPERA A QUE TERMINE
    //DEVUELVE TRUE SI CENTRAL HA CONTESTADO HASTA EL FINAL (DENEGADO O RESUMEN) Y FALSE SI NO SE HA PODIDO COMPLETAR
    public static boolean pedirRiego(String id_estacion, int duracion){
        if(centralCaida()){
            System.out.println("IMPOSIBLE CONECTAR CON WM_CENTRAL!!");
            return false;
        }

        String id_peticion = UUID.randomUUID().toString();

        //SE APUNTA LA PETICION ANTES DE ENVIARLA: LA RESPUESTA PUEDE LLEGAR ENSEGUIDA
        hilo_kafka.respuestas.clear();
        hilo_kafka.id_estacion_actual = id_estacion;
        hilo_kafka.id_peticion_actual = id_peticion;

        System.out.println("PIDIENDO UN RIEGO DE " + duracion + " SEGUNDOS EN " + id_estacion + " (PETICION " + id_peticion + ")");

        boolean terminada;
        //PETICION#ID_PETICION#ID_OPERARIO#ID_ESTACION#DURACION
        if(!productor.enviar("peticiones-riego", null, "PETICION#" + id_peticion + "#" + id_operario + "#" + id_estacion + "#" + duracion)){
            System.out.println("IMPOSIBLE CONECTAR CON WM_CENTRAL!! (NO SE HA PODIDO ENVIAR LA PETICION A KAFKA)");
            terminada = false;
        }else{
            terminada = esperarRespuesta(duracion);
        }

        //YA NO HAY PETICION EN CURSO: DEJAN DE MOSTRARSE LOS DATOS DE ESA ESTACION
        hilo_kafka.id_peticion_actual = null;
        hilo_kafka.id_estacion_actual = null;
        return terminada;
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //ESPERA LAS RESPUESTAS DE LA PETICION EN CURSO. LO QUE LLEGA YA LO MUESTRA EL HILO DE KAFKA: AQUI SOLO SE DECIDE CUANDO PARAR
    //  - DENEGADO O RESUMEN: LA PETICION HA TERMINADO
    //  - 6 SEGUNDOS SIN LATIDO: CENTRAL HA CAIDO
    //  - 15 SEGUNDOS SIN AUTORIZADO NI DENEGADO: CENTRAL NO CONTESTA
    //  - YA AUTORIZADO, SIN RESUMEN AL ACABAR EL TIEMPO DEL RIEGO MAS 15 SEGUNDOS (POR EJEMPLO, SE HA CAIDO EL ENGINE)
    public static boolean esperarRespuesta(int duracion){
        long inicio = System.currentTimeMillis();
        long autorizado = 0; //HORA DEL AUTORIZADO (0 MIENTRAS NO LLEGUE)
        String cadena;

        try{
            while(true){
                cadena = hilo_kafka.respuestas.poll(1, TimeUnit.SECONDS);

                if(cadena != null){
                    if(cadena.startsWith("AUTORIZADO#")){
                        autorizado = System.currentTimeMillis();
                    }else{
                        return true;
                    }
                }

                if(centralCaida()){
                    System.out.println("IMPOSIBLE CONECTAR CON WM_CENTRAL!!");
                    return false;
                }

                if(autorizado == 0 && System.currentTimeMillis() - inicio > MAX_SIN_RESPUESTA){
                    System.out.println("CENTRAL NO HA RESPONDIDO A LA PETICION: IMPOSIBLE CONECTAR CON WM_CENTRAL!!");
                    return false;
                }

                if(autorizado != 0 && System.currentTimeMillis() - autorizado > duracion * 1000L + MARGEN_RESUMEN){
                    System.out.println("EL RIEGO NO HA TERMINADO A SU HORA. EL RESUMEN SE MOSTRARA CUANDO LLEGUE");
                    return false;
                }
            }
        }catch(InterruptedException e){
            return false;
        }
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //MUESTRA TODAS LAS ESTACIONES CON SU ULTIMO ESTADO (EL QUE ENVIA CENTRAL CADA 2 SEGUNDOS)
    public static void verEstaciones(){
        if(centralCaida()){
            System.out.println("IMPOSIBLE CONECTAR CON WM_CENTRAL!!");
            return;
        }
        if(hilo_kafka.estaciones.isEmpty()){
            System.out.println("TODAVIA NO HA LLEGADO EL ESTADO DE NINGUNA ESTACION");
            return;
        }

        //TreeMap: ORDENADAS POR ID
        Map<String, String> ordenadas = new TreeMap<>(hilo_kafka.estaciones);
        String[] split;
        int disponibles = 0;

        System.out.println("ESTACIONES:");
        for(Map.Entry<String, String> e : ordenadas.entrySet()){
            //VALOR: UBICACION#ESTADO
            split = e.getValue().split("#");
            System.out.println("  " + e.getKey() + " - " + split[0] + " - " + split[1]);
            if(split[1].equals("DISPONIBLE")){
                disponibles++;
            }
        }
        System.out.println("DISPONIBLES: " + disponibles + " DE " + ordenadas.size());
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //MENU (SIN FICHERO)
    public static void menu(BufferedReader buffer){
        String opcion;
        String id_estacion;
        String texto;
        int duracion;

        try{
            while(true){
                System.out.println();
                System.out.println("----- OPERARIO " + id_operario + " -----");
                System.out.println("1. PEDIR UN RIEGO");
                System.out.println("2. VER ESTACIONES");
                System.out.println("3. SALIR");
                System.out.println("OPCION: ");

                opcion = buffer.readLine();
                //NULL: SE HA CERRADO LA ENTRADA (POR EJEMPLO, LANZADO EN DOCKER SIN -it)
                if(opcion == null){
                    return;
                }

                switch(opcion.trim()){
                    case "1":
                        System.out.println("ID DE LA ESTACION: ");
                        id_estacion = buffer.readLine();
                        System.out.println("DURACION EN SEGUNDOS: ");
                        texto = buffer.readLine();
                        if(id_estacion == null || texto == null){
                            return;
                        }

                        id_estacion = id_estacion.trim();
                        if(id_estacion.isEmpty() || id_estacion.contains("#")){
                            System.out.println("EL ID DE LA ESTACION NO PUEDE ESTAR VACIO NI CONTENER #!!");
                            break;
                        }
                        try{
                            duracion = Integer.parseInt(texto.trim());
                        }catch(NumberFormatException e){
                            System.out.println("LA DURACION TIENE QUE SER UN NUMERO!!");
                            break;
                        }
                        if(duracion <= 0){
                            System.out.println("LA DURACION TIENE QUE SER MAYOR QUE 0!!");
                            break;
                        }

                        pedirRiego(id_estacion, duracion);
                    break;

                    case "2":
                        verEstaciones();
                    break;

                    case "3":
                        return;

                    default:
                        System.out.println("OPCION INCORRECTA!!");
                }
            }
        }catch(IOException e){
            System.err.println("Error: " + e.toString());
        }
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //MODO FICHERO (PRUEBA BASE, SIN INTERACCION): UNA LINEA ID_ESTACION#DURACION POR PETICION
    //SE PIDE LA PRIMERA, SE ESPERA A QUE TERMINE (BIEN O MAL), SE ESPERAN 4 SEGUNDOS Y SE PASA A LA SIGUIENTE
    public static void procesarFichero(String ruta){
        String linea;
        String[] split;
        int duracion;
        int numero = 0;

        try(BufferedReader fichero = new BufferedReader(new FileReader(ruta))){
            while((linea = fichero.readLine()) != null){
                numero++;
                linea = linea.trim();

                //LAS LINEAS VACIAS SE SALTAN
                if(linea.isEmpty()){
                    continue;
                }

                split = linea.split("#");
                if(split.length != 2 || split[0].trim().isEmpty()){
                    System.out.println("LINEA " + numero + " INCORRECTA (TIENE QUE SER ID_ESTACION#DURACION): " + linea);
                    continue;
                }
                try{
                    duracion = Integer.parseInt(split[1].trim());
                }catch(NumberFormatException e){
                    System.out.println("LINEA " + numero + " INCORRECTA (LA DURACION TIENE QUE SER UN NUMERO): " + linea);
                    continue;
                }
                if(duracion <= 0){
                    System.out.println("LINEA " + numero + " INCORRECTA (LA DURACION TIENE QUE SER MAYOR QUE 0): " + linea);
                    continue;
                }

                System.out.println();
                System.out.println("----- PETICION DE LA LINEA " + numero + " -----");
                pedirRiego(split[0].trim(), duracion);

                System.out.println("SIGUIENTE PETICION EN 4 SEGUNDOS...");
                Thread.sleep(ESPERA_FICHERO);
            }
            System.out.println("NO QUEDAN MAS PETICIONES EN EL FICHERO");
        }catch(FileNotFoundException e){
            System.err.println("NO EXISTE EL FICHERO " + ruta + "!!");
        }catch(IOException e){
            System.err.println("NO SE PUEDE LEER EL FICHERO " + ruta + "!!");
            System.err.println("Error: " + e.toString());
        }catch(InterruptedException e){
            return;
        }
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //CIERRA EL CONSUMIDOR Y EL PRODUCTOR. SE LLAMA AL SALIR Y TAMBIEN CON Ctrl+C O docker stop:
    //ASI KAFKA SABE AL MOMENTO QUE ESTE FO SE HA IDO Y, SI SE RELANZA, ENTRA EN SU GRUPO SIN ESPERAR
    public static synchronized void cerrar(){
        if(cerrado){
            return;
        }
        cerrado = true;

        hilo_kafka.activo = false;
        try{
            hilo_kafka.join(5000);
        }catch(InterruptedException e){
            //SE SIGUE CERRANDO
        }
        productor.cerrar();
        System.out.println("FO CERRADO");
    }

    public static void main(String args[]){

        InputStreamReader reader = new InputStreamReader(System.in);
        BufferedReader buffer = new BufferedReader(reader);

        String[] argumentos = new String[4];

        //--------------------------------------------------------------------------------------------------------------------------------
        //VALIDAR ARGUMENTOS
        validacionArgumentos(args, buffer, argumentos);

        //VALIDACION DEL PUERTO
        int puerto_kafka;
        try{
            puerto_kafka = Integer.parseInt(argumentos[1]);
        }catch(NumberFormatException e){
            System.err.println("EL PUERTO TIENE QUE SER UN NUMERO!!");
            System.exit(1);
            return;
        }

        //VALIDACION DEL ID: CENTRAL Y WS ESTAN RESERVADOS PARA LOS RIEGOS PEDIDOS DESDE CENTRAL Y DESDE LA ESTACION
        id_operario = argumentos[2];
        if(id_operario == null || id_operario.isEmpty() || id_operario.contains("#")){
            System.err.println("EL ID DEL OPERARIO NO PUEDE ESTAR VACIO NI CONTENER #!!");
            System.exit(1);
        }
        if(id_operario.equals("CENTRAL") || id_operario.equals("WS")){
            System.err.println("CENTRAL Y WS SON IDS RESERVADOS, NO PUEDEN SER OPERARIOS!!");
            System.exit(1);
        }

        System.out.println("Kafka se encuentra en: " + argumentos[0] + ":" + puerto_kafka);
        System.out.println("El identificador del operario es: " + id_operario);

        String servidor_kafka = argumentos[0] + ":" + puerto_kafka;

        //--------------------------------------------------------------------------------------------------------------------------------
        //DOS FO CON EL MISMO ID SE REPARTIRIAN LOS MENSAJES: SI YA HAY OTRO CONECTADO, ESTE TERMINA
        comprobarIdLibre(servidor_kafka);

        //--------------------------------------------------------------------------------------------------------------------------------
        //PRODUCTOR DE KAFKA (PARA ENVIAR LAS PETICIONES)
        productor = new ProductorKafka(servidor_kafka);

        //--------------------------------------------------------------------------------------------------------------------------------
        //HILO CONSUMIDOR DE KAFKA (RESPUESTAS DE CENTRAL, DATOS DEL RIEGO Y ESTADO DE LAS ESTACIONES)
        hilo_kafka = new Hilo_Kafka_FO(servidor_kafka, id_operario);
        hilo_kafka.start();

        //CON Ctrl+C O docker stop TAMBIEN SE CIERRAN EL CONSUMIDOR Y EL PRODUCTOR
        Runtime.getRuntime().addShutdownHook(new Thread(() -> cerrar()));

        //--------------------------------------------------------------------------------------------------------------------------------
        //ANTES DE PEDIR NADA, ESPERAR A QUE EL CONSUMIDOR ESTE ESCUCHANDO
        System.out.println("CONECTANDO CON KAFKA...");
        if(!esperarKafka()){
            System.out.println("IMPOSIBLE CONECTAR CON KAFKA!! (COMPRUEBA LA DIRECCION Y QUE ESTE ARRANCADO)");
            System.exit(1);
        }
        System.out.println("CONECTADO A KAFKA!!");

        //--------------------------------------------------------------------------------------------------------------------------------
        //CON FICHERO SE PROCESA SIN MENU; SIN FICHERO, MENU
        if(argumentos[3] != null){
            procesarFichero(argumentos[3]);
        }else{
            menu(buffer);
        }

        cerrar();
        System.exit(0);
    }
}