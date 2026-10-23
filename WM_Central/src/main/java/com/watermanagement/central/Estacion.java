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
}
