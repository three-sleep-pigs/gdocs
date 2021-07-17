package com.gdocs.backend.Service;

import com.gdocs.backend.Entity.Edit;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

public interface EditService {
    String downExcelData(Integer id,Integer version,Integer edit);

}
