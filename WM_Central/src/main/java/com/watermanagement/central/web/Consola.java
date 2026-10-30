package com.watermanagement.central.web;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

//COPIA DE LO QUE CENTRAL ESCRIBE EN PANTALLA, PARA ENSEÑARLO EN EL PANEL WEB ("ACTIVIDAD DE CENTRAL")
//SE INSTALA AL PRINCIPIO DEL main: A PARTIR DE AHI, CADA System.out.println Y System.err.println
//SIGUE SALIENDO EN LA TERMINAL COMO SIEMPRE Y ADEMAS SE GUARDA AQUI (LAS ULTIMAS MAX_LINEAS LINEAS)
//ASI NO HAY QUE TOCAR LOS MENSAJES DE LOS DEMAS HILOS: EL PANEL VE LOS MISMOS QUE LA TERMINAL
public class Consola extends OutputStream{

    public static final int MAX_LINEAS = 300;

    //UNA LINEA GUARDADA: SU NUMERO (CRECE SIEMPRE), EL TEXTO Y SI SALIO POR System.err
    public static class Linea{
        public long numero;
        public String texto;
        public boolean error;

        public Linea(long numero, String texto, boolean error){
            this.numero = numero;
            this.texto = texto;
            this.error = error;
        }
    }

    //LINEAS GUARDADAS (COMPARTIDAS POR System.out Y System.err) Y NUMERO DE LA ULTIMA
    public static final ArrayDeque<Linea> lineas = new ArrayDeque<>();
    public static long ultima = 0;

    public PrintStream original;      //LA TERMINAL DE VERDAD
    public Charset juego;             //CODIFICACION DE LA TERMINAL (EN WINDOWS NO ES UTF-8)
    public boolean error;             //TRUE PARA System.err
    public ByteArrayOutputStream linea_actual;

    public Consola(PrintStream original, boolean error){
        this.original = original;
        this.juego = original.charset();
        this.error = error;
        this.linea_actual = new ByteArrayOutputStream();
    }

    //CAMBIA System.out Y System.err POR COPIAS QUE ESCRIBEN EN LA TERMINAL Y GUARDAN LAS LINEAS
    public static void instalar(){
        System.setOut(new PrintStream(new Consola(System.out, false), true, System.out.charset()));
        System.setErr(new PrintStream(new Consola(System.err, true), true, System.err.charset()));
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //LO QUE LLAMA PrintStream AL ESCRIBIR: SE PASA A LA TERMINAL Y SE VA JUNTANDO LA LINEA HASTA EL SALTO DE LINEA

    public void write(int b) throws IOException{
        original.write(b);
        anadir(b);
    }

    public void write(byte[] b, int inicio, int cantidad) throws IOException{
        original.write(b, inicio, cantidad);
        for(int i = inicio; i < inicio + cantidad; i++){
            anadir(b[i]);
        }
    }

    public void flush() throws IOException{
        original.flush();
    }

    public void anadir(int b){
        if(b == '\n'){
            guardar(linea_actual.toString(juego), error);
            linea_actual.reset();
        }else if(b != '\r'){
            linea_actual.write(b);
        }
    }

    //------------------------------------------------------------------------------------------------------------------------------------
    //LINEAS GUARDADAS. synchronized PORQUE ESCRIBEN TODOS LOS HILOS DE CENTRAL Y LEE EL PANEL WEB

    public static synchronized void guardar(String texto, boolean error){
        ultima++;
        lineas.addLast(new Linea(ultima, texto, error));
        if(lineas.size() > MAX_LINEAS){
            lineas.removeFirst();
        }
    }

    //LAS LINEAS POSTERIORES A LA NUMERO desde (CON 0, TODAS LAS QUE QUEDAN). EL PANEL PIDE SOLO LAS NUEVAS CADA SEGUNDO
    public static synchronized List<Linea> desde(long desde){
        List<Linea> nuevas = new ArrayList<>();
        for(Linea l : lineas){
            if(l.numero > desde){
                nuevas.add(l);
            }
        }
        return nuevas;
    }

    public static synchronized long getUltima(){
        return ultima;
    }
}
