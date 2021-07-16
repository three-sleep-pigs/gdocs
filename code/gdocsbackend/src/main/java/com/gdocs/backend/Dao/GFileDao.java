package com.gdocs.backend.Dao;

import com.gdocs.backend.Entity.GFile;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

public interface GFileDao {
    List<GFile> getGFiles();
    List<GFile> getBin(String creator);
    Optional<GFile> getGFileById(Integer id);
    GFile saveFile(GFile gFile);
    int deleteGFileById(Integer id);
    int recoverGFileById(Integer id);
    int setRecentById(Timestamp recent, Integer id);
    int setLengthById(Integer length,Integer id);
    int setVersionById(Integer version,Integer id);
}
