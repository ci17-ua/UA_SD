package com.watermanagement.central;

public class Estacion {
    
    public String idEstacion;
    public String ubicacion;
    public Estado estado;
    public float caudal;
    public float volumen;
    public String idOperario;

    public Estacion(String idEstacion, String ubicacion, Estado estado, float caudal, float volumen, String idOperario){
        this.idEstacion = idEstacion;
        this.ubicacion = ubicacion;
        this.estado = estado;
        this.caudal = caudal;
        this.volumen = volumen;
        this.idOperario = idOperario;
    }

    public Estacion(String idEstacion, String ubicacion, Estado estado){
        this.idEstacion = idEstacion;
        this.ubicacion = ubicacion;
        this.estado = estado;
        this.caudal = 0;
        this.volumen = 0;
        this.idOperario = "";
    }
}
