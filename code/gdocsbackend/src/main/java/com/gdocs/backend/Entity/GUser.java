package com.gdocs.backend.Entity;

import lombok.Data;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Entity
@Table(name = "guser",schema = "gdocs")
public class GUser {
    @Id
    private String username;

    @Basic
    private String passwords;
}
