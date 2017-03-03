/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author mauricio
 */
public class Executor2 {

    private static final String SQL_1 = "Select co_id, co_plaintext from tb_content limit %d offset %d";
    private static final String SQL_2 = "Select wo_id from tb_word where wo_word = ?";
    private static final String SQL_3 = "Update rl_content_word Set wo_count = wo_count + 1 where co_id = ? and wo_id = ?";
    private static final String SQL_4 = "Insert rl_content_word(co_id, wo_id, wo_count) values(?,?,?)";
    private static final String SQL_5 = "Insert tb_word(wo_word) values(?)";
    private static final String SQL_6 = "Insert rl_content_word(co_id, wo_id, wo_count) values(?,?,?)";
    private static final String SQL_7 = "Update tb_content Set co_count = ? where co_id = ?";
    
    /**
     * @param args the command line arguments
     */
    private static Connection getConnection(String database, String port, String user, String senha) throws ClassNotFoundException, SQLException {
        String driverName = "com.mysql.jdbc.Driver";
        Class.forName(driverName);
        String url = "jdbc:mysql://localhost:" + port + "/" + database;
        return DriverManager.getConnection(url, user, senha);
    }

    private static Iterator<String> preprocessamento(String conteudo) {
        conteudo = conteudo.trim().toLowerCase();
        return new LinkedList<String>(Arrays.asList(conteudo.split("\\s+"))).iterator();
    }
    private static LinkedList<String> preprocessamento2(String conteudo) {
        conteudo = conteudo.trim().toLowerCase();
        return new LinkedList<String>(Arrays.asList(conteudo.split("\\s+")));
    }

    private static long countElementos(Connection con) throws SQLException {
        String sql = "Select count(*) from tb_content";
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        long result;
        while (rs.next()) {
            result = rs.getLong(1);
            stmt.close();
            return result;
        }
        return 0;
    }

    public static void processar(String port, String database, String user, String senha) throws SQLException, ClassNotFoundException {
        Connection con = getConnection(database, port, user, senha);
        Statement stmt;
        ResultSet rs, rs2;
        PreparedStatement ps;
        String text;
        Iterator<String> it;
        String palavra, sql, p;
        //Scanner scan = new Scanner(System.in);
        //System.out.println("Informe o limite: ");
        int co_id;
        long total = countElementos(con), lim = 100, ini = 0;
        long x = ini;
        long contador = 0, wo_id;
        while (ini < total) {
            sql = String.format(SQL_1, lim, ini);
            stmt = con.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                text = rs.getString(2);
                co_id = rs.getInt(1); 
                if(text == null)continue;
                it = preprocessamento(text);
                System.out.println(x);
                
                while(it.hasNext()){
                    p = it.next();
                    palavra = p.replaceAll("\\p{Punct}", "");
                    ps = con.prepareStatement(SQL_2);
                    ps.setBytes(1, palavra.getBytes());
                    rs2 = ps.executeQuery();
                    if (rs2.next()) {
                        
                        wo_id = rs2.getInt(1);
                        rs2.close();
                        ps.close();
                        
                        ps = con.prepareStatement(SQL_3);
                        ps.setInt(1, co_id);
                        ps.setLong(2, wo_id);
                        if (ps.executeUpdate() != 1) {
                            ps.close();
                            ps = con.prepareStatement(SQL_4);
                            ps.setInt(1, co_id);
                            ps.setLong(2, wo_id);
                            ps.setInt(3, 1);
                            ps.executeUpdate();
                        }
                        ps.close();

                    } else {
                        rs2.close();
                        ps.close();
                        
                        ps = con.prepareStatement(SQL_5);
                        ps.setBytes(1, palavra.getBytes());
                        ps.executeUpdate();
                        ps.close();
                        
                        contador++;
                        wo_id = contador;
                        
                        ps = con.prepareStatement(SQL_6);
                        ps.setInt(1, co_id);
                        ps.setLong(2, wo_id);
                        ps.setInt(3, 1);
                        ps.executeUpdate();
                        ps.close();
                    }
                    it.remove();
                }

                x++;
            }
            rs.close();
            stmt.close();
            ini += lim;
        }
        con.close();
    }
    
    
    public static void updateCountContent(Connection con, long co_id, long count) throws SQLException{
        PreparedStatement ps = con.prepareStatement(SQL_7);
        ps.setLong(1, count);
        ps.setLong(2, co_id);
        ps.executeUpdate();
        ps.close();
    }
    
    public static void contarContent(String port, String database, String user, String senha) throws SQLException, ClassNotFoundException {
        Connection con = getConnection(database, port, user, senha);
        Statement stmt;
        ResultSet rs, rs2;
        PreparedStatement ps;
        String text;
        List<String> it;
        String palavra, sql, p;
        int co_id;
        long total = countElementos(con), lim = 1000, ini = 0;
        long x = ini;
        long contador = 0, wo_id;
        while (ini < total) {
            sql = String.format(SQL_1, lim, ini);
            stmt = con.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                text = rs.getString(2);
                co_id = rs.getInt(1);
                if(text == null)continue;
                it = preprocessamento2(text);
                updateCountContent(con, co_id, it.size());
                contador++;
                System.out.println(contador);
            }
            ini += lim;
        }
        con.close();
    }
    
    
    

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        // TODO code application logic here
        //ExtraiTexto et = new ExtraiTexto();
        //et.extrair("3306", "wikipedia_pre_search", "root", "");
        //processar("3306", "wikipedia_pre_search", "root", "123456");
        contarContent("3306", "wikipedia_pre_search", "root", "123456");
    }

}
