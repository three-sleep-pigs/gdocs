package com.gdocs.backend.DaoImpl;

import com.gdocs.backend.Dao.EditDao;
import com.gdocs.backend.Entity.Edit;
import com.gdocs.backend.Repository.EditRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.sql.Time;
import java.util.List;

@Repository
public class EditDaoImpl implements EditDao {
    @Autowired
    private EditRepository editRepository;

    @Override
    public List<Edit> getEditsByFileId(Integer fileId)
    {
        return editRepository.getEditsByFileid(fileId);
    }

    @Override
    public Edit save (Edit edit)
    {
        return editRepository.save(edit);
    }

    @Override
    public Edit getById(Integer id)
    {
        return editRepository.getById(id);
    }
}
