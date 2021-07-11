package com.gdocs.backend.DaoImpl;

import com.gdocs.backend.Dao.GUserDao;
import com.gdocs.backend.Entity.GUser;
import com.gdocs.backend.Repository.GUserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class GUserDaoImpl implements GUserDao {
    @Autowired
    private GUserRepository gUserRepository;

    @Override
    public Optional<GUser> getGUserByUsername(String username)
    {
        return gUserRepository.getGUserByUsername(username);
    }

    @Override
    public GUser saveUser(GUser gUser)
    {
        return gUserRepository.save(gUser);
    }
}
