package com.gdocs.backend.Dao;

import com.gdocs.backend.Entity.GFile;

import java.util.List;
import java.util.Optional;

public interface GFileDao {
    List<GFile> getGFiles();
    Optional<GFile> getGFileById(Integer id);
    GFile saveFile(GFile gFile);
    int deleteGFileById(Integer id);
}
