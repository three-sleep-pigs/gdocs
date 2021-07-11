package com.gdocs.backend.Entity;

import lombok.Data;

import javax.persistence.*;

@Data
@Entity
@Table(name = "gfile",schema = "gdocs")
public class GFile {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Basic
    private String filename;

    @Basic
    private String creator;
}
