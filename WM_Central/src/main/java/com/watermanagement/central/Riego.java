package com.watermanagement.central;

//RIEGO DE UNA ESTACION, VISTO DESDE CENTRAL: LO QUE HACE FALTA PARA CONTESTAR AL OPERARIO Y PARA EL PANEL
public class Riego {
    public String idPeticion;
    public String idOperario;
    public int duracion;        //SEGUNDOS PEDIDOS (0 SI SE HA RECONSTRUIDO A PARTIR DE LOS DATOS)
    public long horaOrden;      //CUANDO SE ENVIO INICIAR: SI PASAN 5 SEGUNDOS SIN INICIADO, SE DENIEGA
    public boolean confirmado;  //FALSE HASTA QUE LLEGA INICIADO (O EL PRIMER DATOS)
    public double caudal;       //L/MIN DEL ULTIMO DATOS
    public double volumen;      //LITROS ACUMULADOS DEL ULTIMO DATOS

    public Riego(String idPeticion, String idOperario, int duracion){
        this.idPeticion = idPeticion;
        this.idOperario = idOperario;
        this.duracion = duracion;
        this.horaOrden = System.currentTimeMillis();
        this.confirmado = false;
        this.caudal = 0;
        this.volumen = 0;
    }
}