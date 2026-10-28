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
        this.riegoActual = null;
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

    public synchronized boolean estaBloqueada(){
        return bloqueada;
    }

    public synchronized Riego getRiego(){
        return riegoActual;
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    //RIEGOS. EL ESTADO DE RIEGO (REGANDO) LO DECIDE EL ENGINE: SOLO CAMBIA CON INICIADO, DATOS Y FIN

    //PETICION: RESERVA LA ESTACION PARA UN RIEGO SI ESTA LIBRE (DISPONIBLE Y SIN OTRO RIEGO)
    //COMPRUEBA Y RESERVA A LA VEZ, ASI DOS PETICIONES NO PUEDEN QUEDARSE CON LA MISMA ESTACION
    //null: reservada
    //texto: el motivo por el que no se puede regar (va en el DENEGADO)
    public synchronized String reservar(Riego riego){
        switch(estado){
            case DESCONECTADA:
                return "ESTACION_DESCONECTADA";
            case FUGA:
                return "ESTACION_AVERIADA";
            case FUERA_DE_SERVICIO:
                return "ESTACION_FUERA_DE_SERVICIO";
            case REGANDO:
                return "ESTACION_OCUPADA";
            default:
                break;
        }
        if(riegoActual != null){
            return "ESTACION_OCUPADA";
        }
        riegoActual = riego;
        return null;
    }

    //INICIADO: EL RIEGO RESERVADO HA EMPEZADO
    //DEVUELVE EL RIEGO, O NULL SI ESA PETICION NO ERA LA RESERVADA (POR EJEMPLO, YA SE HABIA DENEGADO POR TARDAR)
    public synchronized Riego confirmarRiego(String idPeticion){
        if(riegoActual == null || riegoActual.confirmado || !riegoActual.idPeticion.equals(idPeticion)){
            return null;
        }
        riegoActual.confirmado = true;
        if(estado == EstadoWS.DISPONIBLE){
            estado = EstadoWS.REGANDO;
        }
        return riegoActual;
    }

    //DATOS: GUARDA EL CAUDAL Y EL VOLUMEN DEL RIEGO
    //SI CENTRAL NO CONOCIA EL RIEGO (SE HA REINICIADO EN MITAD), LO RECONSTRUYE CON LOS DATOS: POR ESO DATOS LLEVA PETICION Y OPERARIO
    //SOLO PASA A REGANDO SI ESTABA DISPONIBLE: UNA ESTACION DESCONECTADA SIGUE DESCONECTADA HASTA QUE SE AUTENTIQUE SU MONITOR
    //DEVUELVE TRUE SI HA CAMBIADO EL ESTADO (HAY QUE GUARDARLO)
    public synchronized boolean actualizarTelemetria(String idPeticion, String idOperario, double caudal, double volumen){
        if(riegoActual == null){
            riegoActual = new Riego(idPeticion, idOperario, 0);
        }else if(!riegoActual.idPeticion.equals(idPeticion)){
            //DATOS DE OTRO RIEGO: SE IGNORAN
            return false;
        }
        riegoActual.confirmado = true;
        riegoActual.caudal = caudal;
        riegoActual.volumen = volumen;
        if(estado == EstadoWS.DISPONIBLE){
            estado = EstadoWS.REGANDO;
            return true;
        }
        return false;
    }

    //FIN O RECHAZADO: QUITA EL RIEGO SI ES ESA PETICION (UN FIN ANTIGUO NO BORRA UN RIEGO NUEVO)
    //SOLO SI ESTABA REGANDO VUELVE A DISPONIBLE (O A FUERA_DE_SERVICIO SI ESTA BLOQUEADA). LA FUGA LA DECIDE EL MONITOR
    //DEVUELVE EL RIEGO QUITADO, O NULL SI NO ERA EL DE ESTA ESTACION
    public synchronized Riego terminarRiego(String idPeticion){
        if(riegoActual == null || !riegoActual.idPeticion.equals(idPeticion)){
            return null;
        }
        Riego riego = riegoActual;
        riegoActual = null;
        if(estado == EstadoWS.REGANDO){
            if(bloqueada){
                estado = EstadoWS.FUERA_DE_SERVICIO;
            }else{
                estado = EstadoWS.DISPONIBLE;
            }
        }
        return riego;
    }

    //RESERVA QUE LLEVA MAS DE max_ms SIN INICIADO: SE QUITA Y SE DEVUELVE PARA DENEGARLA (NULL SI NO HAY NINGUNA)
    public synchronized Riego reservaCaducada(long max_ms){
        if(riegoActual == null || riegoActual.confirmado){
            return null;
        }
        if(System.currentTimeMillis() - riegoActual.horaOrden <= max_ms){
            return null;
        }
        Riego riego = riegoActual;
        riegoActual = null;
        return riego;
    }

}