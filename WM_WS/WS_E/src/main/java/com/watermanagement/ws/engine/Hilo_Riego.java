package com.watermanagement.ws.engine;

//HILO QUE SOLO EXISTE MIENTRAS SE RIEGA: CADA SEGUNDO LLAMA A WM_WS_E.segundoDeRiego()
//(CALCULA EL CAUDAL Y EL VOLUMEN, ENVIA DATOS Y GUARDA EL FICHERO)
//TERMINA CUANDO ESE RIEGO YA NO ES EL ACTUAL: HA LLEGADO AL TIEMPO O LO HAN PARADO (CENTRAL, EL MENU, UNA AVERIA...)
public class Hilo_Riego extends Thread{

    public Riego riego;

    public Hilo_Riego(Riego riego){
        this.riego = riego;
    }

    public void run(){
        try{
            do{
                Thread.sleep(1000);
            }while(WM_WS_E.segundoDeRiego(riego));
        }catch(InterruptedException e){
            return;
        }
    }
}