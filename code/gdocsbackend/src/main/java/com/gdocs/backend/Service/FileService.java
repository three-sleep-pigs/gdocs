package com.gdocs.backend.Service;

import com.gdocs.backend.Entity.GFile;
import com.gdocs.backend.Reply.FileReply;
import com.gdocs.backend.Util.CellData;

import java.util.List;

public interface FileService {
    List<GFile> getFiles();

    FileReply getFileByID(Integer id);

    Integer deleteFileByID(String username,Integer id);

    FileReply addFile(String username,String filename);

    Integer editFile(String username,Integer fileId,List<CellData> cellDataList);
}
