package com.gdocs.backend.Dao;

import com.gdocs.backend.Entity.GUser;

import java.util.Optional;

public interface GUserDao {
    Optional<GUser> getGUserByUsername(String username);

    GUser saveUser(GUser gUser);
}
