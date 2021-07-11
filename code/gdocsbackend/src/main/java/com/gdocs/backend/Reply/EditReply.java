package com.gdocs.backend.Reply;

import lombok.Data;

import java.util.Set;

@Data
public class EditReply {
    //200:成功 400:失败
    private Integer status;
    Set<String> coworkers;
}
