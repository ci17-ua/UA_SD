package com.watermanagement.central;

import java.sql.SQLException;
import java.util.*;

import com.watermanagement.central.db.DatabaseManager;
import com.watermanagement.central.kafka.ProductorKafka;

//ORDENES QUE CENTRAL ENVIA POR SU CUENTA A UNA ESTACION O A TODAS (PUNTO 11 DEL ENUNCIADO):
//  - INICIAR UN RIEGO: IGUAL QUE SI LO PIDIERA UN OPERARIO
//  - BLOQUEAR: LA ESTACION PARA CUALQUIER RIEGO Y QUEDA FUERA DE SERVICIO
//  - ACTIVAR: LA ESTACION VUELVE A ESTAR DISPONIBLE
//LAS USAN EL MENU DE CENTRAL Y EL PANEL WEB
//CADA METODO MUESTRA EN LA PANTALLA DE CENTRAL LO QUE HA HECHO Y LO DEVUELVE COMO TEXTO (PARA ENSEÑARSELO AL USUARIO DEL PANEL)
public class Ordenes{

    //EN VEZ DEL ID DE UNA ESTACION, PARA ENVIAR LA ORDEN A TODAS
    public static final String TODAS = "TODAS";

    public Map<String, Estacion> registro;
    public DatabaseManager dbmg;
    public ProductorKafka productor;

    public Ordenes(Map<String, Estacion> registro, DatabaseManager dbmg, ProductorKafka productor){
        this.registro = registro;
        this.dbmg = dbmg;
        this.productor = productor;
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //INICIAR UN RIEGO
    //CENTRAL PUBLICA UNA PETICION CON CENTRAL COMO OPERARIO Y SU PROPIO HILO DE KAFKA LA ATIENDE IGUAL QUE LAS DE LOS FO:
    //COMPRUEBA LA ESTACION, ENVIA INICIAR, ESPERA EL INICIADO... (LOS PASOS SE VEN EN LA PANTALLA DE CENTRAL CON "-> CENTRAL")
    //CON TODAS SE ENVIA UNA PETICION A CADA ESTACION REGISTRADA: LAS QUE NO ESTEN DISPONIBLES LA DENEGARAN
    public String iniciarRiego(String id_estacion, int duracion){
        if(duracion <= 0){
            return linea("", "LA DURACION TIENE QUE SER MAYOR QUE 0!!");
        }

        //UNA ESTACION AUNQUE NO EXISTA (CENTRAL LA DENEGARA CON ESTACION_NO_REGISTRADA, COMO A UN OPERARIO) O TODAS
        List<String> ids = new ArrayList<>();
        if(id_estacion.equalsIgnoreCase(TODAS)){
            ids.addAll(new TreeSet<>(registro.keySet()));
        }else{
            ids.add(id_estacion);
        }
        if(ids.isEmpty()){
            return linea("", "NO HAY NINGUNA ESTACION REGISTRADA");
        }

        String resultado = "";
        String id_peticion;
        for(String id : ids){
            id_peticion = UUID.randomUUID().toString();
            //PETICION#ID_PETICION#CENTRAL#ID_ESTACION#DURACION
            if(productor.enviar("peticiones-riego", null, "PETICION#" + id_peticion + "#CENTRAL#" + id + "#" + duracion)){
                resultado = linea(resultado, "PETICION DE RIEGO DE " + duracion + " SEGUNDOS ENVIADA A " + id + " (" + Hilo_Kafka_Central.corto(id_peticion) + ")");
            }else{
                resultado = linea(resultado, "NO SE HA PODIDO ENVIAR LA PETICION DE RIEGO A " + id + " (KAFKA NO RESPONDE)");
            }
        }
        return resultado;
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //BLOQUEAR: FUERA DE SERVICIO EN CENTRAL (Y EN LA BBDD) Y BLOQUEAR#ID AL ENGINE, QUE PARA EL RIEGO SI LO HAY (FIN CON MOTIVO BLOQUEO)
    //UN ENGINE CAIDO NO LEE LAS ORDENES ANTIGUAS: CENTRAL LE REENVIA EL BLOQUEAR CUANDO VUELVE
    //(AL RESOLVERSE LA FUGA O AL AUTENTICARSE SU MONITOR, EN Hilo_Central)
    public String bloquear(String id_estacion){
        List<Estacion> lista = destino(id_estacion);
        if(lista.isEmpty()){
            return noExiste(id_estacion);
        }

        String resultado = "";
        Riego riego;
        for(Estacion e : lista){
            if(!e.bloquear()){
                resultado = linea(resultado, "LA ESTACION " + e.idEstacion + " YA ESTABA FUERA DE SERVICIO");
                continue;
            }
            guardarEstado(e);

            //BLOQUEAR#ID_ESTACION (CLAVE: LA ESTACION, PARA QUE LLEGUE DETRAS DE SUS ORDENES ANTERIORES)
            if(productor.enviar("ordenes-ws", e.idEstacion, "BLOQUEAR#" + e.idEstacion)){
                resultado = linea(resultado, "ESTACION " + e.idEstacion + " BLOQUEADA: FUERA DE SERVICIO");
            }else{
                resultado = linea(resultado, "ESTACION " + e.idEstacion + " FUERA DE SERVICIO EN CENTRAL, PERO NO SE HA PODIDO ENVIAR LA ORDEN A SU ENGINE");
            }

            //SI ESTABA REGANDO, SE AVISA AL OPERARIO. EL RESUMEN LE LLEGA CON EL FIN DEL ENGINE
            riego = e.getRiego();
            if(riego != null){
                Hilo_Kafka_Central.avisar(productor, "PASO", riego.idPeticion, riego.idOperario,
                                          "CENTRAL HA BLOQUEADO LA ESTACION " + e.idEstacion + ": SE PARA EL RIEGO");
            }
        }
        return resultado;
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //ACTIVAR: QUITA EL BLOQUEO EN CENTRAL (Y EN LA BBDD) Y ENVIA ACTIVAR#ID AL ENGINE
    public String activar(String id_estacion){
        List<Estacion> lista = destino(id_estacion);
        if(lista.isEmpty()){
            return noExiste(id_estacion);
        }

        String resultado = "";
        for(Estacion e : lista){
            if(!e.activar()){
                resultado = linea(resultado, "LA ESTACION " + e.idEstacion + " NO ESTABA FUERA DE SERVICIO");
                continue;
            }
            guardarEstado(e);

            //ACTIVAR#ID_ESTACION
            if(productor.enviar("ordenes-ws", e.idEstacion, "ACTIVAR#" + e.idEstacion)){
                resultado = linea(resultado, "ESTACION " + e.idEstacion + " ACTIVADA: " + e.getEstado());
            }else{
                resultado = linea(resultado, "ESTACION " + e.idEstacion + " ACTIVADA EN CENTRAL, PERO NO SE HA PODIDO ENVIAR LA ORDEN A SU ENGINE");
            }
        }
        return resultado;
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //AUXILIARES

    //LAS ESTACIONES A LAS QUE VA UNA ORDEN: LA DE ESE ID (SI EXISTE) O TODAS, ORDENADAS POR ID
    public List<Estacion> destino(String id_estacion){
        List<Estacion> lista = new ArrayList<>();
        if(id_estacion.equalsIgnoreCase(TODAS)){
            for(String id : new TreeSet<>(registro.keySet())){
                lista.add(registro.get(id));
            }
        }else if(registro.containsKey(id_estacion)){
            lista.add(registro.get(id_estacion));
        }
        return lista;
    }

    public String noExiste(String id_estacion){
        if(id_estacion.equalsIgnoreCase(TODAS)){
            return linea("", "NO HAY NINGUNA ESTACION REGISTRADA");
        }
        return linea("", "LA ESTACION " + id_estacion + " NO ESTA REGISTRADA");
    }

    //MUESTRA UNA LINEA EN LA PANTALLA DE CENTRAL Y LA AÑADE AL TEXTO QUE SE DEVUELVE
    public String linea(String resultado, String texto){
        System.out.println(texto);
        return resultado + texto + "\n";
    }

    //GUARDA EN LA BBDD EL ESTADO DE LA ESTACION (BLOQUEADA SE GUARDA COMO FUERA_DE_SERVICIO). UN FALLO AL GUARDAR NO PARA LA ORDEN
    public void guardarEstado(Estacion e){
        try{
            dbmg.actualizarEstado(e.idEstacion, e.estadoParaGuardar());
        }catch(SQLException ex){
            System.err.println("NO SE HA PODIDO GUARDAR EL ESTADO DE LA ESTACION " + e.idEstacion + " EN LA BBDD!!");
            System.err.println("Error: " + ex.toString());
        }
    }
}
