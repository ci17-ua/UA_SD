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

    public void crearTabla() throws SQLException{
        String sql = "CREATE TABLE IF NOT EXISTS estaciones_riego (" +
                    "id TEXT PRIMARY KEY" + 
                    "ubicacin TEXT NOT NULL" + 
                    "estado TEXT NOT NULL";
        try (Connection conn = conectar(); 
        PreparedStatement ps = conn.prepareStatement(sql)){
            ps.executeUpdate();
        }
    }

    public void insertar(Estacion estacion) throws SQLException{
        String sql = "INSERT INTO estaciones_riego (id) VALUES (?)";
        try(Connection conn = conectar(); 
            PreparedStatement ps = conn.prepareStatement(sql)){
                ps.setString(1, estacion.idEstacion);
                ps.setString(2, estacion.ubicacion);
                ps.setString(3, estacion.estado.name());
                ps.executeUpdate();
            }
    }

    public void listarTodos() throws SQLException{
        String sql = "SELECT * FROM estaciones_riego";
        try(Connection conn =  conectar();
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()){
                while(rs.next()){
                    System.out.println(rs.getString("id"));
                }
            }
    }

    public List<Estacion> cargarEstaciones(){
    
        List<Estacion> lista = new ArrayList<>();
        String sql = "SELECT * FORM estaciones_riego";
        try(Connection conn = conectar();
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()){
                while(rs.next()){
                    Estado estado = Estado.valueOf(rs.getString("estado"));
                    Estacion e = new Estacion(rs.getString("id"), rs.getString("ubicacion"), estado);
                    lista.add(e);
                }
        }catch(Exception e){
            System.err.println("Error: " + e.toString());
        }
        return lista;
    }
}