package com.gdocs.backend.Entity;

import lombok.Data;

import javax.persistence.*;
import java.sql.Time;
import java.sql.Timestamp;

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
    private Integer length;

    @Basic
    private Integer version;

    /** 0:create,1:modify data,2:delete,3:recover,4:rollback **/
    @Basic
    private Integer operation;

    @Basic
    private Timestamp edittime;
}
