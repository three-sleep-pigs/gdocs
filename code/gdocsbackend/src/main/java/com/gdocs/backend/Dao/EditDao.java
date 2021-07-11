package com.gdocs.backend.Dao;

import com.gdocs.backend.Entity.Edit;

import java.sql.Time;
import java.util.List;

public interface EditDao {
    List<Edit> getEditsByFileId(Integer fileId);
    int insertEdit(Integer fileId, String editor, Time editTime);
}
