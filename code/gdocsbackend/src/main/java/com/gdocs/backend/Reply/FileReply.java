package com.gdocs.backend.Reply;

import com.gdocs.backend.Entity.GFile;
import lombok.Data;

@Data
public class FileReply {
    private int status;
    private GFile gfile;
}
