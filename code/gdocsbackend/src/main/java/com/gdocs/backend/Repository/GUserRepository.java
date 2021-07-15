package com.gdocs.backend.Repository;

import com.gdocs.backend.Entity.GUser;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface GUserRepository extends JpaRepository<GUser,String> {
    Optional<GUser> getGUserByUsername(String username);
}
