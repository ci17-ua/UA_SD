package com.watermanagement.ws.engine;

//RIEGO EN MARCHA EN EL MOTOR. SOLO SE TOCA DESDE LOS METODOS synchronized DE WM_WS_E
public class Riego {
    public String idPeticion;
    public String idOperario;
    public int duracionMaxima;   //SEGUNDOS PEDIDOS (LOS TRAE INICIAR)
    public int segundosRegados;  //SUMA 1 CADA SEGUNDO. AL LLEGAR A duracionMaxima, FIN CON MOTIVO TIEMPO
    public double caudal;        //L/MIN SIMULADO (ALEATORIO ENTRE 8 Y 12)
    public double volumen;       //LITROS: SUMA caudal / 60 CADA SEGUNDO

    public Riego(String idPeticion, String idOperario, int duracionMaxima){
        this.idPeticion = idPeticion;
        this.idOperario = idOperario;
        this.duracionMaxima = duracionMaxima;
        this.segundosRegados = 0;
        this.caudal = 0;
        this.volumen = 0;
    }
}