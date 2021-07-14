package com.gdocs.backend.Repository;

import com.gdocs.backend.Entity.Edit;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface EditRepository extends JpaRepository<Edit,Integer> {
    List<Edit> getEditsByFileid(Integer fileId);
}
