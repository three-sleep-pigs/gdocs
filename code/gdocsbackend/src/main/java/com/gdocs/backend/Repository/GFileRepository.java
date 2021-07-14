package com.gdocs.backend.Repository;

import com.gdocs.backend.Entity.GFile;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import javax.transaction.Transactional;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

public interface GFileRepository extends JpaRepository<GFile,Integer> {
    @Query("select g from GFile g")
    List<GFile> getGFiles();

    Optional<GFile> getGFileById(Integer id);

    @Transactional
    @Modifying
    int deleteGFileById(Integer id);

    @Transactional
    @Modifying
    @Query(value="update gfile set recent=?  where id=?",nativeQuery=true)
    int setRecentById(Timestamp recent,Integer id);
}
