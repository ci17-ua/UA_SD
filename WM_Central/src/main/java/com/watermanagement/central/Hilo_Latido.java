package com.watermanagement.central;

import java.util.*;

import com.watermanagement.central.kafka.ProductorKafka;

//HILO QUE CADA 2 SEGUNDOS PUBLICA EN estado-estaciones:
//  - LATIDO: PARA QUE LOS FO SEPAN QUE CENTRAL SIGUE VIVA (AUNQUE NO HAYA NINGUNA ESTACION)
//  - ESTADO#ID_ESTACION#UBICACION#ESTADO: UNO POR ESTACION, PARA LA LISTA DE ESTACIONES DE LOS FO
//Y CADA MEDIO SEGUNDO DENIEGA LOS RIEGOS RESERVADOS QUE LLEVAN MAS DE 5 SEGUNDOS SIN INICIADO (ENGINE CAIDO O LENTO)
public class Hilo_Latido extends Thread{

    public static final long CADA_LATIDO = 2000;
    public static final long MAX_SIN_INICIADO = 5000;

    public Map<String, Estacion> registro;
    public ProductorKafka productor;

    public Hilo_Latido(Map<String, Estacion> registro, ProductorKafka productor){
        this.registro = registro;
        this.productor = productor;
    }

    public void run(){
        System.out.println("SE HA CREADO EL HILO DEL LATIDO CORRECTAMENTE");

        long ultimo_latido = 0;
        Riego riego;

        while(true){
            //UN FALLO (POR EJEMPLO, KAFKA CAIDO) NO PUEDE MATAR EL HILO
            try{
                //------------------------------------------------------------------------------------------------------------------------
                //RESERVAS SIN INICIADO EN 5 SEGUNDOS: SE DENIEGAN PARA QUE EL OPERARIO NO ESPERE PARA SIEMPRE
                for(Estacion e : registro.values()){
                    riego = e.reservaCaducada(MAX_SIN_INICIADO);
                    if(riego != null){
                        System.out.println("LA ESTACION " + e.idEstacion + " NO HA RESPONDIDO A LA ORDEN DE RIEGO EN 5 SEGUNDOS");
                        Hilo_Kafka_Central.avisar(productor, "DENEGADO", riego.idPeticion, riego.idOperario, "ESTACION_NO_RESPONDE");
                    }
                }

                //------------------------------------------------------------------------------------------------------------------------
                //LATIDO Y ESTADO DE CADA ESTACION
                if(System.currentTimeMillis() - ultimo_latido >= CADA_LATIDO){
                    ultimo_latido = System.currentTimeMillis();
                    productor.enviar("estado-estaciones", null, "LATIDO");
                    for(Estacion e : registro.values()){
                        productor.enviar("estado-estaciones", null, "ESTADO#" + e.idEstacion + "#" + e.ubicacion + "#" + e.getEstado());
                    }
                }

                Thread.sleep(500);
            }catch(InterruptedException e){
                return;
            }catch(Exception e){
                System.err.println("ERROR EN EL HILO DEL LATIDO!!");
                System.err.println("Error: " + e.toString());
            }
        }
    }
}