
import combinacao.Combinacao;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;
import javax.swing.table.DefaultTableModel;
import object.Documento;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author mauricio
 */
public class JFSearchWiki extends javax.swing.JFrame {

    private static final String SQL_1 = "Select c.co_id, c.co_title, c.co_url, r.rank_d_w from rl_content_word r, tb_word w, tb_content c "
            + "Where w.wo_id = r.wo_id AND c.co_id = r.co_id AND w.wo_word = ? ORDER BY rank_d_w DESC  limit %d ";

    /**
     * Creates new form JFSearchWiki
     */
    public JFSearchWiki() {
        initComponents();
        setLocationRelativeTo(null);
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jPanel1 = new javax.swing.JPanel();
        jtfQuery = new javax.swing.JTextField();
        jbLimpar = new javax.swing.JButton();
        jScrollPane1 = new javax.swing.JScrollPane();
        jtWiki = new javax.swing.JTable();
        jbBuscar = new javax.swing.JButton();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        jPanel1.setBorder(javax.swing.BorderFactory.createTitledBorder(javax.swing.BorderFactory.createEtchedBorder(), "Search Wiki", javax.swing.border.TitledBorder.DEFAULT_JUSTIFICATION, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Dialog", 0, 24))); // NOI18N

        jtfQuery.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jtfQueryActionPerformed(evt);
            }
        });

        jbLimpar.setText("Limpar");
        jbLimpar.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jbLimparActionPerformed(evt);
            }
        });

        jtWiki.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {

            },
            new String [] {

            }
        ));
        jScrollPane1.setViewportView(jtWiki);

        jbBuscar.setText("Buscar");
        jbBuscar.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jbBuscarActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
        jPanel1.setLayout(jPanel1Layout);
        jPanel1Layout.setHorizontalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jScrollPane1)
                    .addGroup(jPanel1Layout.createSequentialGroup()
                        .addComponent(jtfQuery, javax.swing.GroupLayout.PREFERRED_SIZE, 338, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                        .addComponent(jbLimpar)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(jbBuscar)))
                .addContainerGap())
        );
        jPanel1Layout.setVerticalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jtfQuery, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jbLimpar)
                    .addComponent(jbBuscar))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 427, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jPanel1, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addContainerGap())
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jPanel1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void jtfQueryActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jtfQueryActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jtfQueryActionPerformed

    private void jbLimparActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jbLimparActionPerformed
        // TODO add your handling code here:
        jtfQuery.setText("");
        jtWiki.setModel(new DefaultTableModel());

    }//GEN-LAST:event_jbLimparActionPerformed
    private Connection getConnection(String database, String port, String user, String senha) throws ClassNotFoundException, SQLException {
        String driverName = "com.mysql.jdbc.Driver";
        Class.forName(driverName);
        String url = "jdbc:mysql://localhost:" + port + "/" + database;
        return DriverManager.getConnection(url, user, senha);
    }

    private Map<Long, Documento> getDocsWord(Connection con, String palavra, int limit) throws SQLException {
        String sql = String.format(SQL_1, limit);
        System.out.println(sql);
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setBytes(1, palavra.getBytes());
        ResultSet rs = ps.executeQuery();
        Map<Long, Documento> result = new HashMap(limit);
        
        
        while (rs.next()) {
            try {
                result.put(rs.getLong(1), new Documento(rs.getLong(1), (new String(rs.getBytes(2),"UTF-8")), (new String(rs.getBytes(3),"UTF-8")), rs.getDouble(4)));
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(JFSearchWiki.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        rs.close();
        ps.close();
        return result;

    }

    private List<Documento> obterDocumentosRelevantes(Connection con, List<String> words) throws SQLException {
        Map<String, Set<Documento>> conjuntos = getDocumentos(con, words);
        if (conjuntos.size() == 1) {
            return new ArrayList(conjuntos.get(words.get(0)));
        }

        int i;
        List<String[]> combinacoes = new ArrayList();
        for (i = conjuntos.size(); i >= 1; i--) {
            if (conjuntos.size() < i) {
                continue;
            }
            Combinacao comb = new Combinacao(new ArrayList(conjuntos.keySet()), i);
            while (comb.hasNext()) {
                combinacoes.add(comb.next());
            }

        }
        HashMap<Long, Documento> conjuntoFinal = new HashMap<>();
        for (String[] combs : combinacoes) {
            Set[] conj = new Set[combs.length];
            int x = 0;
            for (String c : combs) {
                if (combs.length == 1) {
                    for (Documento d : conjuntos.get(c)) {
                        if (words.size() != 1) {
                            d.setRank(d.getRank() / Math.pow(1.5, words.size()));
                        }
                    }
                    conj[x] = conjuntos.get(c);
                } else {
                    for (Documento d : conjuntos.get(c)) {
                        if (words.size() != 1) {
                            d.setRank(d.getRank()*combs.length);
                        }
                    }
                    conj[x] = conjuntos.get(c);
                }
                x++;
            }
            Set<Documento> docs = interseccaoList(conj);
            for (Documento d : docs) {
                if (conjuntoFinal.containsKey(d.getId())) {
                    Documento d1 = conjuntoFinal.get(d.getId());
                    d1.setRank(d1.getRank() + d.getRank());
                } else {
                    if (d.getRank() >= 4) {
                        conjuntoFinal.put(d.getId(), d);
                    }
                }
            }
        }

        return new ArrayList(conjuntoFinal.values());
    }

    private Map<String, Set<Documento>> getDocumentos(Connection con, List<String> words) throws SQLException {
        Map<String, Set<Documento>> map = new HashMap();
        for (String w : words) {
            if (map.containsKey(w)) {
                for (Documento d : map.get(w)) {
                    d.setRank(d.getRank() * 2);
                }
            } else {
                map.put(w, new HashSet(getDocsWord(con, w, 100).values()));
            }
        }

        return map;
    }

    private Set<Documento> interseccaoList(Set<Documento>... C) {
        if (C.length == 1) {
            return C[0];
        }
        Set<Documento> A = C[0];
        for (int i = 1; i < C.length; i++) {
            A = interseccao(A, C[i]);
        }
        return A;
    }

    private Set<Documento> interseccao(Set<Documento> C1, Set<Documento> C2) {
        Set<Documento> result = new HashSet<>();
        for (Documento d : C1) {
            if (C2.contains(d)) {
                result.add(d);
            }
        }
        return result;
    }

    private void preencheTabela(List<Documento> docs) {
        DefaultTableModel dt = new DefaultTableModel();
        dt.addColumn("ID");
        dt.addColumn("Titulo");
        dt.addColumn("URL");
        for (Documento d : docs) {
            dt.addRow(new Object[]{d.getId(), d.getTitulo(), d.getUrl()});
            System.out.println(d.getRank());
        }
        jtWiki.setModel(dt);
    }


    private void jbBuscarActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jbBuscarActionPerformed
        try {
            // TODO add your handling code here:
            Connection con = getConnection("wikipedia_pre_search", "3306", "root", "123456");
            String query = jtfQuery.getText()
                    .trim().toLowerCase();
            List<String> words = new LinkedList<String>();
            for (String q : query.split("\\s+")) {
                q = q.replaceAll("\\p{Punct}", "");
                if (q.trim().length() != 0) {
                    words.add(q);
                }
            }
            if(words.size() == 0){
                JOptionPane.showMessageDialog(null, "Nada a ser buscado!");
                return;
            }
            List<Documento> documentos = obterDocumentosRelevantes(con, words);
            if (documentos.size() == 0) {
                jtWiki.setModel(new DefaultTableModel());
                JOptionPane.showMessageDialog(null, "Nenhum dado encontrado!");
                return;
            }
            Collections.sort(documentos, Collections.reverseOrder());
            preencheTabela(documentos);
            con.close();

        } catch (ClassNotFoundException ex) {
            Logger.getLogger(JFSearchWiki.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(JFSearchWiki.class.getName()).log(Level.SEVERE, null, ex);
        }


    }//GEN-LAST:event_jbBuscarActionPerformed

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(JFSearchWiki.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(JFSearchWiki.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(JFSearchWiki.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(JFSearchWiki.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new JFSearchWiki().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JPanel jPanel1;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JButton jbBuscar;
    private javax.swing.JButton jbLimpar;
    private javax.swing.JTable jtWiki;
    private javax.swing.JTextField jtfQuery;
    // End of variables declaration//GEN-END:variables
}
