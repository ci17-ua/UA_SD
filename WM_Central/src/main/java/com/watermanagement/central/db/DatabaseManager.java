package com.watermanagement.central.db;

import java.sql.*;
import java.util.*;

import com.watermanagement.central.*;

//BASE DE DATOS SQLITE DE CENTRAL: TABLAS estaciones Y operarios (EXPLICADAS EN docs/DB.md)
//CADA METODO ABRE SU PROPIA CONEXION Y LA CIERRA AL TERMINAR (try-with-resources): ASI LA PUEDEN USAR VARIOS HILOS A LA VEZ
//LOS VALORES SIEMPRE VAN EN UN PreparedStatement, NUNCA PEGADOS AL TEXTO DE LA CONSULTA
public class DatabaseManager{

    private final String url;

    //ruta: EL FICHERO DE LA BASE DE DATOS (SI NO EXISTE, SQLITE LO CREA)
    public DatabaseManager(String ruta){
        this.url = "jdbc:sqlite:" + ruta;
    }

    private Connection conectar() throws SQLException{
        return DriverManager.getConnection(url);
    }

    //CREA LAS TABLAS SI NO EXISTEN (LA PRIMERA VEZ QUE ARRANCA CENTRAL)
    public void crearTablas() throws SQLException{
        String consulta_estaciones = "CREATE TABLE IF NOT EXISTS estaciones(" +
                                     "id TEXT PRIMARY KEY," +
                                     "ubicacion TEXT NOT NULL," +
                                     "estado TEXT NOT NULL) STRICT;";
        String consulta_operarios = "CREATE TABLE IF NOT EXISTS operarios(" +
                                    "id TEXT PRIMARY KEY) STRICT;";

        try(Connection conn = conectar();
            PreparedStatement ps = conn.prepareStatement(consulta_estaciones);
            PreparedStatement ps2 = conn.prepareStatement(consulta_operarios)){
            ps.executeUpdate();
            ps2.executeUpdate();
        }
    }

    //ALTA DE UNA ESTACION NUEVA (EMPIEZA DISPONIBLE)
    public void insertarEstacion(Estacion estacion) throws SQLException{
        String consulta = "INSERT INTO estaciones (id, ubicacion, estado) VALUES (?, ?, ?)";
        try(Connection conn = conectar();
            PreparedStatement ps = conn.prepareStatement(consulta)){
            ps.setString(1, estacion.idEstacion);
            ps.setString(2, estacion.ubicacion);
            ps.setString(3, EstadoWS.DISPONIBLE.name());
            ps.executeUpdate();
        }
    }

    //GUARDA EL ULTIMO ESTADO CONOCIDO DE UNA ESTACION
    public void actualizarEstado(String id, EstadoWS estado) throws SQLException{
        String consulta = "UPDATE estaciones SET estado = ? WHERE id = ?";
        try(Connection conn = conectar();
            PreparedStatement ps = conn.prepareStatement(consulta)){
            ps.setString(1, estado.name());
            ps.setString(2, id);
            ps.executeUpdate();
        }
    }

    //DA DE ALTA AL OPERARIO SI NO EXISTIA (INSERT OR IGNORE: SI YA ESTA, NO HACE NADA)
    //TRUE: ES UN OPERARIO NUEVO
    //FALSE: YA ESTABA DADO DE ALTA
    public boolean registrarOperario(String id) throws SQLException{
        String consulta = "INSERT OR IGNORE INTO operarios (id) VALUES (?)";
        try(Connection conn = conectar();
            PreparedStatement ps = conn.prepareStatement(consulta)){
            ps.setString(1, id);
            return ps.executeUpdate() > 0;
        }
    }

    //CARGA LAS ESTACIONES AL ARRANCAR CENTRAL
    //TODAS EMPIEZAN DESCONECTADAS HASTA QUE SE AUTENTIQUE SU MONITOR. LAS GUARDADAS COMO FUERA_DE_SERVICIO SIGUEN BLOQUEADAS
    public List<Estacion> cargarEstaciones() throws SQLException{
        List<Estacion> lista = new ArrayList<>();
        String consulta = "SELECT * FROM estaciones";
        try(Connection conn = conectar();
            PreparedStatement ps = conn.prepareStatement(consulta);
            ResultSet rs = ps.executeQuery()){
            while(rs.next()){
                boolean bloqueada = EstadoWS.valueOf(rs.getString("estado")) == EstadoWS.FUERA_DE_SERVICIO;
                lista.add(new Estacion(rs.getString("id"), rs.getString("ubicacion"), bloqueada, EstadoWS.DESCONECTADA));
            }
        }
        return lista;
    }
}
