package com.gdocs.backend.Util;

import lombok.Data;

import java.util.List;

@Data
public class Sheet {
    private String name;
    private String index;
    private Integer order = 0;
    private Integer status = 1;
    private List<CellData> celldata;
}
