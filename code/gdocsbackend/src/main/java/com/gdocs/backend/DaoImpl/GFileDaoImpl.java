package com.gdocs.backend.DaoImpl;

import com.gdocs.backend.Dao.GFileDao;
import com.gdocs.backend.Entity.GFile;
import com.gdocs.backend.Repository.GFileRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

@Repository
public class GFileDaoImpl implements GFileDao {
    @Autowired
    private GFileRepository gFileRepository;

    @Override
    public List<GFile> getGFiles()
    {
        return gFileRepository.getGFiles();
    }

    @Override
    public Optional<GFile> getGFileById(Integer id)
    {
        return gFileRepository.getGFileById(id);
    }

    @Override
    public GFile saveFile(GFile gFile)
    {
        return gFileRepository.save(gFile);
    }

    @Override
    public int deleteGFileById(Integer id)
    {
        return gFileRepository.deleteGFileById(id);
    }

    @Override
    public int setRecentById(Timestamp recent, Integer id)
    {
        return gFileRepository.setRecentById(recent, id);
    }
}
