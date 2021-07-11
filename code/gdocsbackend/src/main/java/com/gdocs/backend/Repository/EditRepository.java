package com.gdocs.backend.Repository;

import com.gdocs.backend.Entity.Edit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import javax.transaction.Transactional;
import java.sql.Time;
import java.util.List;

public interface EditRepository extends JpaRepository<Edit,Integer> {
    List<Edit> getEditsByFileid(Integer fileId);

    @Transactional
    @Modifying
    @Query(value="insert into edit (fileid,editor,edittime) values (?,?,?)",nativeQuery=true)
    int insertEdit(Integer fileId, String editor, Time editTime);
}
