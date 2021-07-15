package com.gdocs.backend.Service;

import com.gdocs.backend.Entity.GFile;
import com.gdocs.backend.Reply.FileReply;

import java.util.List;

public interface FileService {
    List<GFile> getFiles();
    List<GFile> getBin(String creator);
    FileReply addFile(String username, String filename);
    Integer deleteFileByID(String username,Integer id);
    Integer  recoverGFileById(String username,Integer id);
    Integer editFileByID(String username,Integer fileId);

}
