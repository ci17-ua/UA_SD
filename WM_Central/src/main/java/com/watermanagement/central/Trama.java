package com.watermanagement.central;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

//PROTOCOLO DE TRAMAS DE LOS SOCKETS: <STX><DATOS><ETX><LRC>
//  - CADA MENSAJE (VIDA#WS-01#OK, SALUD...) VA ENTRE STX Y ETX, SEGUIDO DEL LRC: EL XOR DE TODOS LOS BYTES DE LOS DATOS
//  - QUIEN RECIBE UNA TRAMA CALCULA EL LRC: SI COINCIDE CONTESTA ACK, Y SI NO, NACK
//  - CON NACK, QUIEN ENVIA REPITE LA TRAMA (COMO MUCHO 3 INTENTOS EN TOTAL)
//  - AL CONECTARSE, EL CLIENTE ENVIA ENQ Y EL SERVIDOR CONTESTA ACK. QUIEN TERMINA DE FORMA ORDENADA ENVIA EOT
//LOS DATOS VAN EN UTF-8, Y EN UTF-8 NINGUN BYTE DE UN CARACTER (NI SIQUIERA DE UNA LETRA CON TILDE) VALE 0x02 O 0x03:
//STX Y ETX NUNCA APARECEN DENTRO DE LOS DATOS
//ESTA CLASE ES IGUAL EN CENTRAL, MONITOR Y ENGINE: SOLO CAMBIA LA LINEA package
public class Trama{

    //CARACTERES DE CONTROL ASCII
    public static final int STX = 0x02;
    public static final int ETX = 0x03;
    public static final int EOT = 0x04;
    public static final int ENQ = 0x05;
    public static final int ACK = 0x06;
    public static final int NACK = 0x15;

    public static final int MAX_INTENTOS = 3;
    public static final int MAX_DATOS = 1024;

    //PARA LA DEMOSTRACION: PROBABILIDAD (DE 0 A 1) DE ENVIAR UNA TRAMA CON EL LRC MAL A PROPOSITO (VARIABLE DE ENTORNO WM_TRAMAS_ERROR)
    //TCP YA ENTREGA LOS BYTES SIN ERRORES, ASI QUE SIN ESTO NUNCA SE VERIA UN NACK.
    //SOLO SE ESTROPEA EL PRIMER ENVIO DE CADA TRAMA: LA REPETICION SALE BIEN Y LA CONEXION NO SE CORTA
    public static double probabilidad_error = leerProbabilidad();

    //CON WM_TRAMAS_TRAZA=1 SE MUESTRA CADA TRAMA QUE SE ENVIA Y SE RECIBE
    public static boolean traza = "1".equals(System.getenv("WM_TRAMAS_TRAZA"));

    public static double leerProbabilidad(){
        String valor = System.getenv("WM_TRAMAS_ERROR");
        if(valor == null){
            return 0;
        }
        try{
            double p = Double.parseDouble(valor.trim());
            return Math.max(0, Math.min(1, p));
        }catch(NumberFormatException e){
            System.err.println("WM_TRAMAS_ERROR TIENE QUE SER UN NUMERO ENTRE 0 Y 1: SE IGNORA");
            return 0;
        }
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //LRC: XOR DE TODOS LOS BYTES DE LOS DATOS (UN VALOR DE 0 A 255)
    public static int lrc(byte[] datos){
        int resultado = 0;
        for(byte b : datos){
            resultado = resultado ^ (b & 0xFF);
        }
        return resultado;
    }

    //LEE UN BYTE. SI EL OTRO EXTREMO HA CERRADO, EOFException. SI PASA EL TIEMPO DE setSoTimeout, SocketTimeoutException
    public static int leerByte(InputStream entrada) throws IOException{
        int b = entrada.read();
        if(b == -1){
            throw new EOFException("EL OTRO EXTREMO HA CERRADO LA CONEXION");
        }
        return b;
    }

    //TEXTO DE UNA TRAMA PARA LA TRAZA: <STX>VIDA#WS-01#OK<ETX><LRC=0x36>
    public static String mostrar(String cadena, int valor_lrc){
        return "<STX>" + cadena + "<ETX><LRC=0x" + String.format("%02X", valor_lrc) + ">";
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //ENVIAR UN MENSAJE: <STX><DATOS><ETX><LRC> Y ESPERAR ACK. CON NACK SE REPITE (MAX_INTENTOS EN TOTAL)
    //LAS EXCEPCIONES NO SE CAPTURAN AQUI: SI EL OTRO EXTREMO SE CAE, QUIEN LLAMA TIENE QUE ENTERARSE
    public static void enviar(Socket socket, String cadena) throws IOException{
        OutputStream salida = socket.getOutputStream();
        InputStream entrada = socket.getInputStream();

        byte[] datos = cadena.getBytes(StandardCharsets.UTF_8);
        int valor_lrc = lrc(datos);

        for(int intento = 1; intento <= MAX_INTENTOS; intento++){
            //LRC QUE SE ENVIA: EL BUENO, SALVO EN LA DEMOSTRACION DE ERRORES (SOLO EN EL PRIMER INTENTO)
            int lrc_enviado = valor_lrc;
            if(intento == 1 && Math.random() < probabilidad_error){
                lrc_enviado = valor_lrc ^ 0xFF;
                System.out.println("[TRAMA] SE ENVIA A PROPOSITO CON EL LRC MAL: " + mostrar(cadena, lrc_enviado));
            }

            //LA TRAMA ENTERA EN UN SOLO write
            ByteArrayOutputStream trama = new ByteArrayOutputStream();
            trama.write(STX);
            trama.write(datos);
            trama.write(ETX);
            trama.write(lrc_enviado);
            salida.write(trama.toByteArray());
            salida.flush();
            if(traza){
                System.out.println("[TRAMA] ENVIADA: " + mostrar(cadena, lrc_enviado));
            }

            int respuesta = leerByte(entrada);
            if(respuesta == ACK){
                if(traza){
                    System.out.println("[TRAMA] ACK RECIBIDO");
                }
                return;
            }
            if(respuesta == EOT){
                throw new EOFException("EL OTRO EXTREMO HA TERMINADO LA COMUNICACION (EOT)");
            }
            if(respuesta != NACK){
                throw new IOException("RESPUESTA INESPERADA A UNA TRAMA: 0x" + String.format("%02X", respuesta));
            }
            System.out.println("[TRAMA] NACK RECIBIDO: " + cadena + " HA LLEGADO MAL (INTENTO " + intento + " DE " + MAX_INTENTOS + ")");
        }
        throw new IOException("TRAMA RECHAZADA " + MAX_INTENTOS + " VECES (NACK): " + cadena);
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //RECIBIR UN MENSAJE: ESPERA <STX><DATOS><ETX><LRC>, COMPRUEBA EL LRC Y CONTESTA ACK (Y DEVUELVE LOS DATOS) O NACK (Y ESPERA LA REPETICION)
    public static String recibir(Socket socket) throws IOException{
        OutputStream salida = socket.getOutputStream();
        InputStream entrada = socket.getInputStream();
        int errores = 0;

        while(true){
            //LO QUE LLEGUE ANTES DEL STX NO ES UNA TRAMA: SE DESCARTA. UN EOT ES EL FIN ORDENADO DE LA COMUNICACION
            int b = leerByte(entrada);
            if(b == EOT){
                throw new EOFException("EL OTRO EXTREMO HA TERMINADO LA COMUNICACION (EOT)");
            }
            if(b != STX){
                continue;
            }

            //DATOS HASTA EL ETX
            ByteArrayOutputStream datos = new ByteArrayOutputStream();
            while((b = leerByte(entrada)) != ETX){
                if(datos.size() >= MAX_DATOS){
                    throw new IOException("TRAMA DEMASIADO LARGA (SIN ETX EN " + MAX_DATOS + " BYTES)");
                }
                datos.write(b);
            }

            //EL BYTE QUE SIGUE AL ETX ES EL LRC
            int lrc_recibido = leerByte(entrada);
            byte[] bytes = datos.toByteArray();
            String cadena = new String(bytes, StandardCharsets.UTF_8);
            int lrc_calculado = lrc(bytes);

            if(lrc_recibido == lrc_calculado){
                salida.write(ACK);
                salida.flush();
                if(traza){
                    System.out.println("[TRAMA] RECIBIDA: " + mostrar(cadena, lrc_recibido) + " -> ACK");
                }
                return cadena;
            }

            //LRC INCORRECTO: NACK Y SE ESPERA LA REPETICION
            salida.write(NACK);
            salida.flush();
            errores++;
            System.out.println("[TRAMA] LRC INCORRECTO EN " + cadena + " (LLEGA 0x" + String.format("%02X", lrc_recibido)
                               + " Y SALE 0x" + String.format("%02X", lrc_calculado) + "): SE RESPONDE NACK");
            if(errores >= MAX_INTENTOS){
                throw new IOException(MAX_INTENTOS + " TRAMAS SEGUIDAS CON EL LRC INCORRECTO");
            }
        }
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //INICIO DE LA COMUNICACION
    //CLIENTE (QUIEN SE CONECTA): ENVIA ENQ Y ESPERA EL ACK DEL SERVIDOR
    public static void enviarEnq(Socket socket) throws IOException{
        OutputStream salida = socket.getOutputStream();
        salida.write(ENQ);
        salida.flush();

        int respuesta = leerByte(socket.getInputStream());
        if(respuesta != ACK){
            throw new IOException("EL SERVIDOR NO HA ACEPTADO LA COMUNICACION (ENQ SIN ACK)");
        }
        if(traza){
            System.out.println("[TRAMA] ENQ ENVIADO Y ACK RECIBIDO: COMUNICACION ABIERTA");
        }
    }

    //SERVIDOR (QUIEN HA HECHO accept): ESPERA EL ENQ DEL CLIENTE Y LE CONTESTA ACK
    public static void esperarEnq(Socket socket) throws IOException{
        int b = leerByte(socket.getInputStream());
        if(b != ENQ){
            throw new IOException("SE ESPERABA ENQ AL ABRIR LA COMUNICACION Y HA LLEGADO 0x" + String.format("%02X", b));
        }
        OutputStream salida = socket.getOutputStream();
        salida.write(ACK);
        salida.flush();
        if(traza){
            System.out.println("[TRAMA] ENQ RECIBIDO Y ACK ENVIADO: COMUNICACION ABIERTA");
        }
    }

    //FIN ORDENADO DE LA COMUNICACION: QUIEN TERMINA ENVIA EOT
    //SI EL OTRO EXTREMO YA HA CERRADO EL SOCKET, DA IGUAL: POR ESO NO SE MUESTRA EL ERROR
    public static void enviarEot(Socket socket){
        try{
            OutputStream salida = socket.getOutputStream();
            salida.write(EOT);
            salida.flush();
            if(traza){
                System.out.println("[TRAMA] EOT ENVIADO: FIN DE LA COMUNICACION");
            }
        }catch(IOException e){
            //EL OTRO EXTREMO YA NO ESTA
        }
    }
}

