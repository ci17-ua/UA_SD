package com.watermanagement.central;

public class Estacion {
    
    public final String idEstacion;
    public final String ubicacion;
    public EstadoWS estado;
    public boolean bloqueada;
    public Riego riegoActual;

    public Estacion(String idEstacion, String ubicacion, boolean bloqueada, EstadoWS estado){
        this.idEstacion = idEstacion;
        this.ubicacion = ubicacion;
        this.bloqueada = bloqueada;
        this.estado = estado;
    }
    
    //COMPRUEBA SI LA ESTACION YA ESTA CONECTADA, EN CASO DE QUE NO LA CONECTA
    //true: autenticacion correcta
    //false: autenticacion fallida
    public synchronized boolean autenticar(){
        if(estado != EstadoWS.DESCONECTADA){
            return false;
        }
        if(bloqueada){
            estado = EstadoWS.FUERA_DE_SERVICIO;
        }else{
            estado = EstadoWS.DISPONIBLE;
        }
        return true;
    }

    public synchronized void desconectar(){
        estado = EstadoWS.DESCONECTADA;
    }

    //Cambia el estado a fuga
    //true: si se cambia exitosamente
    //false: si ya esta marcada la fuga
    public synchronized boolean marcarFuga(){
        if(estado == EstadoWS.FUGA){
            return false;
        }
        estado = EstadoWS.FUGA;
        return true;
    }

    //Cambia el estado de fuga a fuera de servicio o disponible
    //true: se cambia el estado correctamente
    //false: no habia fuga que resolver
    public synchronized boolean fugaResuelta(){
        if(estado != EstadoWS.FUGA){
            return false;
        }
        if(bloqueada){
            estado = EstadoWS.FUERA_DE_SERVICIO;
        }else{
            estado = EstadoWS.DISPONIBLE;
        }
        return true;
    }

    //Estado que se guarda en le BBDD
    //  - BLOQUEADA: SIEMPRE FUERA_DE_SERVICIO, AUNQUE EN PANTALLA SE VEA FUGA
    //  - DESCONECTADA (SOLO PASA AL ACTIVARLA DESDE EL MENU): DISPONIBLE
    //  - EN OTRO CASO: EL ESTADO REAL
    public synchronized EstadoWS estadoParaGuardar(){
        if(bloqueada){
            return EstadoWS.FUERA_DE_SERVICIO;
        }
        if(estado == EstadoWS.DESCONECTADA){
            return EstadoWS.DISPONIBLE;
        }
        return estado;
    }

    public synchronized EstadoWS getEstado(){
        return estado;
    }

}
