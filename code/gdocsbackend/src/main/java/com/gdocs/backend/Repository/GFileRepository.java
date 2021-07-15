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
    @Query("select g from GFile g where g.deleted = false ")
    List<GFile> getGFiles();

    @Query(value="select * from gfile where creator=? and deleted = true",nativeQuery=true)
    List<GFile> getBin(String creator);

    Optional<GFile> getGFileById(Integer id);

    @Transactional
    @Modifying
    @Query(value="update gfile set deleted = true  where id=?",nativeQuery=true)
    int deleteGFileById(Integer id);

    @Transactional
    @Modifying
    @Query(value="update gfile set deleted = false  where id=?",nativeQuery=true)
    int recoverGFileById(Integer id);

    @Transactional
    @Modifying
    @Query(value="update gfile set recent=?  where id=?",nativeQuery=true)
    int setRecentById(Timestamp recent,Integer id);

    @Transactional
    @Modifying
    @Query(value="update gfile set length=?  where id=?",nativeQuery=true)
    int setLengthById(Integer length,Integer id);
}
