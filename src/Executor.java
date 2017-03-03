
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author mauricio
 */
public class Executor {

    private static final String SQL_1 = "Select count(*) from tb_content";
    private static final String SQL_2 = "Update tb_word set wo_idf = ? where wo_id = ?";
    private static final String SQL_3 = "Select COUNT(DISTINCT co_id) from rl_content_word where wo_id = ?";
    private static final String SQL_4 = "Select count(*) from tb_word";
    private static final String SQL_5 = "Select wo_id from tb_word limit ? offset ?";
    private static final String SQL_6 = "Select count(*) from rl_content_word";
    private static final String SQL_7 = "Select rl.wo_id, rl.co_id, rl.wo_count, c.co_count, w.wo_idf  from rl_content_word rl, tb_content c, tb_word w"
            + " Where c.co_id = rl.co_id AND w.wo_id = rl.wo_id limit ? offset ?";
    private static final String SQL_8 = "SELECT AVG(co_count) FROM tb_content";
    private static final String SQL_9 = "Update rl_content_word set rank_d_w = ? where wo_id = ? AND co_id = ?"; 
    
    
    private static Connection getConnection(String database, String port, String user, String senha) throws ClassNotFoundException, SQLException {
        String driverName = "com.mysql.jdbc.Driver";
        Class.forName(driverName);
        String url = "jdbc:mysql://localhost:" + port + "/" + database;
        return DriverManager.getConnection(url, user, senha);
    }

    private static long nrDocumento(Connection con) throws SQLException {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(SQL_1);
        long result = 0;
        if (rs.next()) {
            result = rs.getLong(1);
        }
        rs.close();
        stmt.close();
        return result;
    }

    private static void updateIDF(Connection con, double wo_idf, long wo_id) throws SQLException {
        PreparedStatement ps = con.prepareStatement(SQL_2);
        ps.setDouble(1, wo_idf);
        ps.setLong(2, wo_id);
        ps.executeUpdate();
        ps.close();
    }

    private static long nrDocumento(Connection con, long wo_id) throws SQLException {
        PreparedStatement ps = con.prepareStatement(SQL_3);
        ps.setLong(1, wo_id);
        ResultSet rs = ps.executeQuery();
        long result = 0;
        if (rs.next()) {
            result = rs.getLong(1);
        }
        rs.close();
        ps.close();
        return result;
    }

    private static long nrPalavras(Connection con) throws SQLException {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(SQL_4);
        long result = 0;
        if (rs.next()) {
            result = rs.getLong(1);
        }
        rs.close();
        stmt.close();
        return result;
    }

    private static void gerarIDF(String database, String port, String user, String senha) throws SQLException {
        Connection con = null;
        try {
            con = getConnection(database, port, user, senha);
            long M = nrDocumento(con) + 1;
            long totalPalavra = nrPalavras(con), lim = 1000, ini = 1000000, wo_id;
            double df, wo_idf;
            String sql;
            PreparedStatement ps;
            ResultSet rs;
            long count = ini;
            while (ini < totalPalavra) {
                ps = con.prepareStatement(SQL_5);
                ps.setLong(1, lim);
                ps.setLong(2, ini);
                rs = ps.executeQuery();
                while (rs.next()) {
                    System.out.println(count);
                    wo_id = rs.getLong(1);
                    df = nrDocumento(con, wo_id);
                    wo_idf = Math.log1p(M / df);
                    updateIDF(con, wo_idf, wo_id);
                    count++;
                }
                rs.close();
                ps.close();
                ini += lim;
            }

        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Executor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(Executor.class.getName()).log(Level.SEVERE, null, ex);
        }
        con.close();

    }

    public static long nrRelacoes(Connection con) throws SQLException {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(SQL_6);
        long result = 0;
        if (rs.next()) {
            result = rs.getLong(1);
        }
        rs.close();
        stmt.close();
        return result;
    }

    public static double mediaDocumento(Connection con) throws SQLException {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(SQL_8);
        double result = 0;
        if (rs.next()) {
            result = rs.getLong(1);
        }
        rs.close();
        stmt.close();
        return result;
    }

    private static void updateRanking(Connection con, long wo_id, long co_id, double ranking) throws SQLException{
        PreparedStatement ps = con.prepareStatement(SQL_9);
        ps.setDouble(1, ranking);
        ps.setLong(2, wo_id);
        ps.setLong(3, co_id);
        ps.executeUpdate();
        ps.close();
    }
    
    
    public static void gerarRanking(String database, String port, String user, String senha) throws SQLException {
        Connection con = null;
        try {
            con = getConnection(database, port, user, senha);
            long totalRelacoes = nrRelacoes(con), lim = 10000, ini = 0, wo_id,
                    co_id, co_count, wo_count;
            double mediaD = mediaDocumento(con), wo_idf;
            double ranking = 0, b = 0.5;
            
            long count = ini;

            PreparedStatement ps;
            ResultSet rs;

            while (ini < totalRelacoes) {
                ps = con.prepareStatement(SQL_7);
                ps.setLong(1, lim);
                ps.setLong(2, ini);
                rs = ps.executeQuery();
                while(rs.next()){
                    System.out.println(count);
                    wo_id = rs.getLong(1);
                    co_id = rs.getLong(2);
                    wo_count = rs.getLong(3);
                    co_count = rs.getLong(4);
                    wo_idf = rs.getDouble(5);
                    ranking = Math.log1p(1 + Math.log1p( 1 + wo_count))/
                            (1- b + (b * (co_count/mediaD)));
                    updateRanking(con, wo_id, co_id, ranking * wo_idf);
                    count++;
                }
                rs.close();
                ps.close();
                ini += lim;
            }

        } catch (SQLException ex) {
            Logger.getLogger(Executor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Executor.class.getName()).log(Level.SEVERE, null, ex);
        }
        con.close();

    }

    public static void main(String[] args) throws SQLException {
        //gerarIDF("wikipedia_pre_search", "3306", "root", "123456");
        gerarRanking("wikipedia_pre_search", "3306", "root", "123456");
    }

}
