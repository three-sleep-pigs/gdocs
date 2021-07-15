package com.gdocs.backend.Entity;

import lombok.Data;

import javax.persistence.*;
import java.sql.Time;

@Data
@Entity
@Table(name = "edit",schema = "gdocs")
public class Edit {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Basic
    private Integer fileid;

    @Basic
    private String editor;

    @Basic
    private Time edittime;
}
