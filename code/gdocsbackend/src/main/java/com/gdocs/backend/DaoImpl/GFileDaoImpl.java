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
    public List<GFile> getBin(String creator)
    {
        return gFileRepository.getBin(creator);
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
    public int recoverGFileById(Integer id)
    {
        return gFileRepository.recoverGFileById(id);
    }

    @Override
    public int setRecentById(Timestamp recent, Integer id)
    {
        return gFileRepository.setRecentById(recent, id);
    }

    @Override
    public int setLengthById(Integer length,Integer id)
    {
        return gFileRepository.setLengthById(length, id);
    }

    @Override
    public int setVersionById(Integer version,Integer id)
    {
        return gFileRepository.setVersionById(version, id);
    }
}
