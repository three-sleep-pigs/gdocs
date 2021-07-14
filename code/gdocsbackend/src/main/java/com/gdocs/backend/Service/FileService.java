package com.gdocs.backend.Service;

import com.gdocs.backend.Entity.GFile;
import com.gdocs.backend.Reply.FileReply;

import java.util.List;

public interface FileService {
    List<GFile> getFiles();

    FileReply getFileByID(Integer id);

    Integer deleteFileByID(String username,Integer id);

    Integer addFile(String username,String filename);

    Integer editFile(String username, String index, String row, String column, String value);
}
