package com.watermanagement.central;

import java.sql.SQLException;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.watermanagement.central.db.DatabaseManager;
import com.watermanagement.central.kafka.ConsumidorKafka;
import com.watermanagement.central.kafka.ProductorKafka;

//HILO QUE LEE DE KAFKA LAS PETICIONES DE RIEGO (peticiones-riego) Y LO QUE PASA EN LAS ESTACIONES (telemetria-ws)
//CONTESTA AL OPERARIO POR respuestas-fo Y DA LAS ORDENES A LOS ENGINES POR ordenes-ws
public class Hilo_Kafka_Central extends Thread{

    public String servidor_kafka;
    public DatabaseManager dbmg;
    public Map<String, Estacion> registro;
    public ProductorKafka productor;
    public long horaArranque;

    public Hilo_Kafka_Central(String servidor_kafka, DatabaseManager dbmg, Map<String, Estacion> registro, ProductorKafka productor, long horaArranque){
        this.servidor_kafka = servidor_kafka;
        this.dbmg = dbmg;
        this.registro = registro;
        this.productor = productor;
        this.horaArranque = horaArranque;
    }

    public void run(){
        //EL CONSUMIDOR SE CREA Y SE USA SOLO EN ESTE HILO
        //GRUPO FIJO: TRAS UNA CAIDA, CENTRAL SIGUE DONDE LO DEJO Y NO PIERDE LOS FIN QUE LLEGARON MIENTRAS TANTO
        ConsumidorKafka consumidor = new ConsumidorKafka(servidor_kafka, "central", List.of("peticiones-riego", "telemetria-ws"));
        System.out.println("SE HA CREADO EL HILO CONSUMIDOR DE KAFKA CORRECTAMENTE");

        String cadena;
        String[] split;

        while(true){
            for(ConsumerRecord<String, String> mensaje : consumidor.recibir()){
                cadena = mensaje.value();
                split = cadena.split("#");

                //UN MENSAJE MAL FORMADO NO PUEDE MATAR EL HILO
                try{
                    switch(split[0]){
                        case "PETICION":
                            peticion(split, mensaje.timestamp());
                        break;

                        case "INICIADO":
                            iniciado(split);
                        break;

                        case "RECHAZADO":
                            rechazado(split);
                        break;

                        case "DATOS":
                            datos(split);
                        break;

                        case "FIN":
                            fin(split);
                        break;

                        default:
                            System.out.println("MENSAJE DE KAFKA DESCONOCIDO: " + cadena);
                    }
                }catch(Exception e){
                    System.err.println("ERROR AL PROCESAR EL MENSAJE DE KAFKA: " + cadena);
                    System.err.println("Error: " + e.toString());
                }
            }
        }
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //PETICION#ID_PETICION#ID_OPERARIO#ID_ESTACION#DURACION
    public void peticion(String[] split, long hora){
        if(split.length != 5){
            System.out.println("PETICION MAL FORMADA: " + String.join("#", split));
            return;
        }
        String id_peticion = split[1];
        String id_operario = split[2];
        String id_estacion = split[3];

        //ENVIADA ANTES DE QUE ARRANCARA CENTRAL: LLEGO CON CENTRAL CAIDA Y EL OPERARIO YA DEJO DE ESPERAR
        if(hora < horaArranque){
            System.out.println("PETICION ANTERIOR AL ARRANQUE DE CENTRAL IGNORADA: " + String.join("#", split));
            return;
        }

        System.out.println();
        System.out.println("NUEVA PETICION " + corto(id_peticion) + ": " + id_operario + " PIDE " + split[4] + " SEGUNDOS EN " + id_estacion);

        int duracion;
        try{
            duracion = Integer.parseInt(split[4]);
        }catch(NumberFormatException e){
            duracion = 0;
        }
        if(duracion <= 0){
            avisar(productor, "DENEGADO", id_peticion, id_operario, "DURACION_INCORRECTA");
            return;
        }

        //ALTA DEL OPERARIO CON SU PRIMERA PETICION (CENTRAL Y WS NO SON OPERARIOS)
        //UN FALLO DE LA BBDD AQUI NO IMPIDE EL RIEGO
        if(!id_operario.equals("CENTRAL") && !id_operario.equals("WS")){
            try{
                if(dbmg.registrarOperario(id_operario)){
                    System.out.println("NUEVO OPERARIO DADO DE ALTA: " + id_operario);
                }
            }catch(SQLException e){
                System.err.println("NO SE HA PODIDO DAR DE ALTA AL OPERARIO " + id_operario + " EN LA BBDD!!");
                System.err.println("Error: " + e.toString());
            }
        }

        avisar(productor, "PASO", id_peticion, id_operario, "COMPROBANDO LA ESTACION " + id_estacion);

        Estacion e = registro.get(id_estacion);
        if(e == null){
            avisar(productor, "DENEGADO", id_peticion, id_operario, "ESTACION_NO_REGISTRADA");
            return;
        }

        //reservar() COMPRUEBA Y RESERVA A LA VEZ: DEVUELVE NULL SI LA HA RESERVADO O EL MOTIVO SI NO
        Riego riego = new Riego(id_peticion, id_operario, duracion);
        String motivo = e.reservar(riego);
        if(motivo != null){
            avisar(productor, "DENEGADO", id_peticion, id_operario, motivo);
            return;
        }

        avisar(productor, "PASO", id_peticion, id_operario, "ESTACION " + id_estacion + " DISPONIBLE: SOLICITANDO AUTORIZACION A LA ESTACION");

        //INICIAR#ID_ESTACION#ID_PETICION#ID_OPERARIO#DURACION (CLAVE: LA ESTACION, PARA QUE SUS ORDENES LLEGUEN EN ORDEN)
        if(!productor.enviar("ordenes-ws", id_estacion, "INICIAR#" + id_estacion + "#" + id_peticion + "#" + id_operario + "#" + duracion)){
            e.terminarRiego(id_peticion);
            avisar(productor, "DENEGADO", id_peticion, id_operario, "ERROR_KAFKA");
            return;
        }
        System.out.println("ORDEN INICIAR ENVIADA A " + id_estacion);

        //EL AUTORIZADO SE ENVIA CUANDO LLEGUE EL INICIADO. SI EN 5 SEGUNDOS NO LLEGA, Hilo_Latido DENIEGA LA PETICION
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //INICIADO#ID_ESTACION#ID_PETICION
    public void iniciado(String[] split){
        Estacion e = registro.get(split[1]);
        if(e == null){
            System.out.println("INICIADO DE UNA ESTACION QUE NO EXISTE: " + split[1]);
            return;
        }

        Riego riego = e.confirmarRiego(split[2]);
        if(riego == null){
            //POR EJEMPLO, YA SE HABIA DENEGADO POR TARDAR MAS DE 5 SEGUNDOS
            //SI EL ENGINE RIEGA DE TODAS FORMAS, SUS DATOS RECONSTRUYEN EL RIEGO Y SU FIN ENVIA EL RESUMEN
            System.out.println("INICIADO DE " + split[1] + " PARA UNA PETICION QUE NO ESTABA RESERVADA: " + corto(split[2]));
            return;
        }

        guardarEstado(e);
        System.out.println("RIEGO INICIADO EN " + split[1] + " (" + riego.idOperario + ", " + riego.duracion + " SEGUNDOS)");
        avisar(productor, "AUTORIZADO", riego.idPeticion, riego.idOperario, null);
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //RECHAZADO#ID_ESTACION#ID_PETICION#MOTIVO
    public void rechazado(String[] split){
        Estacion e = registro.get(split[1]);
        if(e == null){
            System.out.println("RECHAZADO DE UNA ESTACION QUE NO EXISTE: " + split[1]);
            return;
        }

        Riego riego = e.terminarRiego(split[2]);
        if(riego == null){
            System.out.println("RECHAZADO DE " + split[1] + " PARA UNA PETICION QUE NO ESTABA RESERVADA: " + corto(split[2]));
            return;
        }

        System.out.println("LA ESTACION " + split[1] + " HA RECHAZADO EL RIEGO POR: " + split[3]);
        avisar(productor, "DENEGADO", riego.idPeticion, riego.idOperario, split[3]);
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //DATOS#ID_ESTACION#ID_PETICION#ID_OPERARIO#CAUDAL#VOLUMEN (CADA SEGUNDO MIENTRAS RIEGA)
    public void datos(String[] split){
        Estacion e = registro.get(split[1]);
        if(e == null){
            return;
        }

        double caudal = Double.parseDouble(split[4]);
        double volumen = Double.parseDouble(split[5]);

        //SI CENTRAL NO CONOCIA EL RIEGO (SE HA REINICIADO EN MITAD), actualizarTelemetria LO RECONSTRUYE
        if(e.actualizarTelemetria(split[2], split[3], caudal, volumen)){
            guardarEstado(e);
        }

        //EL CAUDAL Y EL VOLUMEN NO SE ESCRIBEN EN LA TERMINAL (CON VARIAS ESTACIONES REGANDO LA LLENARIAN CADA SEGUNDO):
        //LOS ENSEÑA EL PANEL WEB, QUE LOS LEE DEL RIEGO DE LA ESTACION
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //FIN#ID_ESTACION#ID_PETICION#ID_OPERARIO#VOLUMEN#DURACION#MOTIVO
    //EL RESUMEN SALE DE LOS DATOS DEL PROPIO FIN: FUNCIONA AUNQUE CENTRAL SE HAYA REINICIADO EN MITAD DEL RIEGO
    public void fin(String[] split){
        if(split.length != 7){
            System.out.println("FIN MAL FORMADO: " + String.join("#", split));
            return;
        }

        Estacion e = registro.get(split[1]);
        if(e != null && e.terminarRiego(split[2]) != null){
            guardarEstado(e);
        }

        //LOS LITROS DE ESTE RIEGO SE SUMAN AL TOTAL DE LA ESTACION (LO ENSEÑA EL PANEL)
        if(e != null){
            e.sumarLitros(Double.parseDouble(split[4]));
        }

        System.out.println("RIEGO TERMINADO EN " + split[1] + " (" + split[3] + "): " + split[4] + " LITROS EN " + split[5] + " SEGUNDOS (" + split[6] + ")");
        avisar(productor, "RESUMEN", split[2], split[3], split[4] + "#" + split[5] + "#" + split[6]);
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //GUARDA EN LA BBDD EL ESTADO DE UNA ESTACION. UN FALLO AL GUARDAR NO PARA NADA
    public void guardarEstado(Estacion e){
        try{
            dbmg.actualizarEstado(e.idEstacion, e.estadoParaGuardar());
        }catch(SQLException ex){
            System.err.println("NO SE HA PODIDO GUARDAR EL ESTADO DE LA ESTACION " + e.idEstacion + " EN LA BBDD!!");
            System.err.println("Error: " + ex.toString());
        }
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //ENVIA UNA RESPUESTA AL OPERARIO POR respuestas-fo Y LA MUESTRA EN CENTRAL (LA GUIA PIDE VER LOS PASOS EN LOS DOS SITIOS)
    //  tipo:  PASO, AUTORIZADO, DENEGADO O RESUMEN
    //  resto: EL TEXTO DEL PASO, EL MOTIVO DEL DENEGADO O "VOLUMEN#DURACION#MOTIVO" DEL RESUMEN (NULL EN EL AUTORIZADO)
    //ES static PARA QUE LO USEN TAMBIEN Hilo_Central Y Hilo_Latido
    public static void avisar(ProductorKafka productor, String tipo, String id_peticion, String id_operario, String resto){
        String cadena = tipo + "#" + id_peticion + "#" + id_operario;
        if(resto != null){
            cadena = cadena + "#" + resto;
        }

        if(resto == null){
            System.out.println("  -> " + id_operario + " [" + corto(id_peticion) + "] " + tipo);
        }else{
            System.out.println("  -> " + id_operario + " [" + corto(id_peticion) + "] " + tipo + ": " + resto);
        }

        productor.enviar("respuestas-fo", null, cadena);
    }

    //LOS 8 PRIMEROS CARACTERES DEL ID DE LA PETICION, PARA QUE LOS MENSAJES DE CENTRAL SE LEAN MEJOR
    public static String corto(String id_peticion){
        if(id_peticion.length() > 8){
            return id_peticion.substring(0, 8);
        }
        return id_peticion;
    }
}