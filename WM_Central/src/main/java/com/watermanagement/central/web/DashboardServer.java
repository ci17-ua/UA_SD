package com.watermanagement.central.web;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import com.watermanagement.central.*;

//PANEL WEB DE CENTRAL. USA EL SERVIDOR HTTP QUE TRAE JAVA (com.sun.net.httpserver): NO HACE FALTA NINGUNA DEPENDENCIA
//  GET  /             LA PAGINA DEL PANEL (resources/static/dashboard.html)
//  GET  /api/estado   LAS ESTACIONES Y LA ACTIVIDAD DE CENTRAL EN JSON. LA PAGINA LO PIDE CADA SEGUNDO
//  POST /api/orden    UNA ORDEN DEL PANEL: INICIAR UN RIEGO, BLOQUEAR O ACTIVAR (UNA ESTACION O TODAS). LA HACE Ordenes
//EL SERVIDOR ATIENDE CADA PETICION EN UNO DE SUS PROPIOS HILOS: NO BLOQUEA NI EL MENU NI LOS DEMAS HILOS DE CENTRAL
public class DashboardServer{

    public int puerto;
    public Map<String, Estacion> registro;
    public Ordenes ordenes;
    public HttpServer servidor;
    public byte[] pagina;   //EL HTML SE LEE UNA VEZ AL ARRANCAR

    public DashboardServer(int puerto, Map<String, Estacion> registro, Ordenes ordenes){
        this.puerto = puerto;
        this.registro = registro;
        this.ordenes = ordenes;
    }

    //ARRANCA EL SERVIDOR. DEVUELVE FALSE SI NO SE HA PODIDO (POR EJEMPLO, EL PUERTO YA ESTA EN USO)
    //SIN PANEL, CENTRAL SIGUE FUNCIONANDO CON SU MENU DE CONSOLA
    public boolean arrancar(){
        try(InputStream html = DashboardServer.class.getResourceAsStream("/static/dashboard.html")){
            if(html == null){
                System.err.println("NO SE ENCUENTRA static/dashboard.html DENTRO DEL JAR!!");
                return false;
            }
            pagina = html.readAllBytes();

            servidor = HttpServer.create(new InetSocketAddress(puerto), 0);
            servidor.createContext("/", this::verPagina);
            servidor.createContext("/api/estado", this::verEstado);
            servidor.createContext("/api/orden", this::recibirOrden);
            //4 HILOS PARA ATENDER A LOS NAVEGADORES (CADA PETICION SE RESUELVE EN MILISEGUNDOS)
            servidor.setExecutor(Executors.newFixedThreadPool(4));
            servidor.start();
        }catch(IOException e){
            System.err.println("NO SE HA PODIDO ARRANCAR EL PANEL WEB EN EL PUERTO " + puerto + "!!");
            System.err.println("Error: " + e.toString());
            return false;
        }

        System.out.println("PANEL WEB EN EL PUERTO " + puerto + ": http://<IP_DE_CENTRAL>:" + puerto);
        return true;
    }

    //====================================================================================================================================
    //GET /: LA PAGINA. CUALQUIER OTRA RUTA QUE NO SEA DE /api DA 404
    public void verPagina(HttpExchange peticion) throws IOException{
        String ruta = peticion.getRequestURI().getPath();
        if(!ruta.equals("/") && !ruta.equals("/index.html")){
            responder(peticion, 404, "text/plain", "NO EXISTE: " + ruta);
            return;
        }
        responder(peticion, 200, "text/html", pagina);
    }

    //====================================================================================================================================
    //GET /api/estado?desde=N
    //{"estaciones":[{"id":..,"ubicacion":..,"estado":..,"bloqueada":..,"total":..,"riego":{..} o null}, ...],
    // "consola":{"ultima":N,"lineas":[{"texto":..,"error":..}, ...]}}
    //desde ES LA ULTIMA LINEA DE LA CONSOLA QUE YA TIENE LA PAGINA: SOLO SE LE ENVIAN LAS NUEVAS
    public void verEstado(HttpExchange peticion) throws IOException{
        if(!peticion.getRequestMethod().equals("GET")){
            responder(peticion, 405, "text/plain", "SOLO GET");
            return;
        }

        long desde = 0;
        Map<String, String> parametros = parametros(peticion.getRequestURI().getRawQuery());
        try{
            desde = Long.parseLong(parametros.getOrDefault("desde", "0"));
        }catch(NumberFormatException e){
            desde = 0;
        }

        //StringBuilder EN VEZ DE IR SUMANDO STRINGS: EL JSON SE MONTA CADA SEGUNDO Y PUEDE TENER CIENTOS DE LINEAS
        StringBuilder json = new StringBuilder("{\"estaciones\":[");

        //ESTACIONES ORDENADAS POR ID
        Estacion e;
        EstadoWS estado;
        Riego riego;
        boolean primera = true;
        for(String id : new TreeSet<>(registro.keySet())){
            e = registro.get(id);
            estado = e.getEstado();
            riego = e.getRiego();

            if(!primera){
                json.append(",");
            }
            primera = false;

            json.append("{\"id\":").append(texto(e.idEstacion))
                .append(",\"ubicacion\":").append(texto(e.ubicacion))
                .append(",\"estado\":").append(texto(estado.name()))
                .append(",\"bloqueada\":").append(e.estaBloqueada())
                .append(",\"total\":").append(numero(e.getLitrosTotales()))
                .append(",\"riego\":");
            //EL RIEGO (O LA RESERVA, SI TODAVIA NO HA LLEGADO EL INICIADO). DURANTE UN BLOQUEO, UNA FUGA O UNA DESCONEXION NO SE ENSEÑA
            if(riego != null && (estado == EstadoWS.REGANDO || estado == EstadoWS.DISPONIBLE)){
                json.append("{\"operario\":").append(texto(riego.idOperario))
                    .append(",\"confirmado\":").append(riego.confirmado)
                    .append(",\"duracion\":").append(riego.duracion)
                    .append(",\"caudal\":").append(numero(riego.caudal))
                    .append(",\"volumen\":").append(numero(riego.volumen)).append("}");
            }else{
                json.append("null");
            }
            json.append("}");
        }

        //ACTIVIDAD DE CENTRAL: LAS LINEAS NUEVAS DE LA CONSOLA
        json.append("],\"consola\":{\"ultima\":").append(Consola.getUltima()).append(",\"lineas\":[");
        primera = true;
        for(Consola.Linea l : Consola.desde(desde)){
            if(!primera){
                json.append(",");
            }
            primera = false;
            json.append("{\"texto\":").append(texto(l.texto)).append(",\"error\":").append(l.error).append("}");
        }
        json.append("]}}");

        responder(peticion, 200, "application/json", json.toString());
    }

    //====================================================================================================================================
    //POST /api/orden CON accion=iniciar|bloquear|activar, estacion=ID|TODAS Y, PARA INICIAR, duracion=SEGUNDOS
    //RESPONDE CON EL TEXTO QUE DEVUELVE Ordenes (LO MISMO QUE SALE EN LA TERMINAL DE CENTRAL)
    public void recibirOrden(HttpExchange peticion) throws IOException{
        if(!peticion.getRequestMethod().equals("POST")){
            responder(peticion, 405, "text/plain", "SOLO POST");
            return;
        }

        String cuerpo = new String(peticion.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, String> parametros = parametros(cuerpo);
        String accion = parametros.getOrDefault("accion", "");
        String id_estacion = parametros.getOrDefault("estacion", "").trim();

        if(id_estacion.isEmpty() || id_estacion.contains("#")){
            responder(peticion, 400, "text/plain", "EL ID DE LA ESTACION NO PUEDE ESTAR VACIO NI CONTENER #!!");
            return;
        }

        System.out.println("ORDEN DESDE EL PANEL WEB: " + accion.toUpperCase() + " " + id_estacion);

        String resultado;
        switch(accion){
            case "iniciar":
                int duracion;
                try{
                    duracion = Integer.parseInt(parametros.getOrDefault("duracion", "").trim());
                }catch(NumberFormatException e){
                    responder(peticion, 400, "text/plain", "LA DURACION TIENE QUE SER UN NUMERO!!");
                    return;
                }
                resultado = ordenes.iniciarRiego(id_estacion, duracion);
            break;

            case "bloquear":
                resultado = ordenes.bloquear(id_estacion);
            break;

            case "activar":
                resultado = ordenes.activar(id_estacion);
            break;

            default:
                responder(peticion, 400, "text/plain", "ORDEN DESCONOCIDA: " + accion);
                return;
        }

        responder(peticion, 200, "text/plain", resultado);
    }

    //====================================================================================================================================
    //AUXILIARES

    public void responder(HttpExchange peticion, int codigo, String tipo, String texto) throws IOException{
        responder(peticion, codigo, tipo, texto.getBytes(StandardCharsets.UTF_8));
    }

    public void responder(HttpExchange peticion, int codigo, String tipo, byte[] cuerpo) throws IOException{
        peticion.getResponseHeaders().set("Content-Type", tipo + "; charset=utf-8");
        //EL NAVEGADOR NO DEBE GUARDAR NADA: EL ESTADO CAMBIA CADA SEGUNDO
        peticion.getResponseHeaders().set("Cache-Control", "no-store");
        peticion.sendResponseHeaders(codigo, cuerpo.length);
        try(OutputStream salida = peticion.getResponseBody()){
            salida.write(cuerpo);
        }
    }

    //"a=1&b=dos" -> {a=1, b=dos} (FORMATO DE LOS FORMULARIOS Y DE LA URL)
    public Map<String, String> parametros(String texto){
        Map<String, String> mapa = new HashMap<>();
        if(texto == null || texto.isEmpty()){
            return mapa;
        }
        String[] partes;
        for(String par : texto.split("&")){
            partes = par.split("=", 2);
            if(partes.length == 2){
                mapa.put(URLDecoder.decode(partes[0], StandardCharsets.UTF_8), URLDecoder.decode(partes[1], StandardCharsets.UTF_8));
            }
        }
        return mapa;
    }

    //TEXTO EN JSON: ENTRE COMILLAS Y CON LAS COMILLAS, BARRAS Y CARACTERES DE CONTROL ESCAPADOS
    public String texto(String s){
        StringBuilder resultado = new StringBuilder("\"");
        char c;
        for(int i = 0; i < s.length(); i++){
            c = s.charAt(i);
            switch(c){
                case '"':  resultado.append("\\\""); break;
                case '\\': resultado.append("\\\\"); break;
                case '\n': resultado.append("\\n"); break;
                case '\t': resultado.append("\\t"); break;
                default:
                    if(c < 0x20){
                        resultado.append(String.format("\\u%04x", (int) c));
                    }else{
                        resultado.append(c);
                    }
            }
        }
        return resultado.append("\"").toString();
    }

    //NUMERO EN JSON: SIEMPRE CON PUNTO, AUNQUE EL SISTEMA ESTE EN ESPAÑOL
    public String numero(double valor){
        return String.format(Locale.ROOT, "%.2f", valor);
    }
}
