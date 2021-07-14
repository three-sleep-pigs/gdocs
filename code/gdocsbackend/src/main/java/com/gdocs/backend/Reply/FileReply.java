package com.gdocs.backend.Reply;

import com.gdocs.backend.Entity.GFile;
import com.gdocs.backend.Util.Sheet;
import lombok.Data;

import java.io.File;
import java.util.List;

@Data
public class FileReply {
    //200:成功 401:没有该文件 402:文件打开失败
    private Integer status;
    private List<Sheet> sheets;
}
