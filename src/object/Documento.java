/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package object;

import java.util.Objects;

/**
 *
 * @author mauricio
 */
public class Documento implements Comparable<Documento>{
    
    private Long id;
    private String titulo;
    private String url;
    private Double rank;

    public Documento(Long id, String titulo, String url, Double rank) {
        this.id = id;
        this.rank = rank;
        this.titulo = titulo;
        this.url = url;
    }

    public Long getId() {
        return id;
    }

    public Double getRank() {
        return rank;
    }

    public void setRank(Double rank) {
        this.rank = rank;
    }

    public String getTitulo() {
        return titulo;
    }

    public void setTitulo(String titulo) {
        this.titulo = titulo;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
    
    

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Documento){
            Documento d = (Documento) obj;
            return d.id.equals(id);
        }
        return false;
    }

    @Override
    public int compareTo(Documento o) {
        return rank.compareTo(o.rank);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + Objects.hashCode(this.id);
        hash = 79 * hash + Objects.hashCode(this.titulo);
        hash = 79 * hash + Objects.hashCode(this.url);
        return hash;
    }
    
    
    
    
    
}
