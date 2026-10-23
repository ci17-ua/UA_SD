package com.watermanagement.central.db;

import java.sql.*;
import java.util.*;
import java.lang.*;
import com.watermanagement.central.*;

public class DatabaseManager{

    private final String url;

    public DatabaseManager(String url){
        this.url = "jdbc:sqlite:" + url;
    }

    private Connection conectar() throws SQLException{
        return DriverManager.getConnection(url);
    }

    public void crearTablas() throws SQLException{
        String querry_est = "CREATE TABLE IF NOT EXISTS estaciones(" +
                            "id TEXT PRIMARY KEY," + 
                            "ubicacion TEXT NOT NULL," + 
                            "estado TEXT NOT NULL) STRICT;";
        String querry_op = " CREATE TABLE IF NOT EXISTS operarios(" +
                            "id TEXT PRIMARY KEY) STRICT;";

        try (Connection conn = conectar(); 
        PreparedStatement ps = conn.prepareStatement(querry_est);
        PreparedStatement ps2 = conn.prepareStatement(querry_op)){
            ps.executeUpdate();
            ps2.executeUpdate();
        }
    }

    public void insertarEstacion(Estacion estacion) throws SQLException{
        String querry = "INSERT INTO estaciones (id, ubicacion, estado) VALUES (?, ?, ?)";
        try(Connection conn = conectar(); 
            PreparedStatement ps = conn.prepareStatement(querry)){
                ps.setString(1, estacion.idEstacion);
                ps.setString(2, estacion.ubicacion);
                ps.setString(3, EstadoWS.DISPONIBLE.name());
                ps.executeUpdate();
            }
    }

    public void actualizarEstado(String id, EstadoWS estado) throws SQLException{
        String querry = "UPDATE estaciones SET estado = ? WHERE id = ?";
        try(Connection conn = conectar();
            PreparedStatement ps = conn.prepareStatement(querry)){
                ps.setString(1, estado.name());
                ps.setString(2, id);
                ps.executeUpdate();
            }
    }

    public List<Estacion> cargarEstaciones() throws SQLException{
    
        List<Estacion> lista = new ArrayList<>();
        String querry = "SELECT * FROM estaciones";
        try(Connection conn = conectar();
            PreparedStatement ps = conn.prepareStatement(querry);
            ResultSet rs = ps.executeQuery()){
                
                while(rs.next()){
                    Estacion e = new Estacion(rs.getString("id"), rs.getString("ubicacion"), false, EstadoWS.valueOf(rs.getString("estado")));
                    if(e.estado == EstadoWS.FUERA_DE_SERVICIO){
                        e.bloqueada = true;
                    }
                    e.estado = EstadoWS.DESCONECTADA;
                    lista.add(e);
                }
            }    
        return lista;
    }
}