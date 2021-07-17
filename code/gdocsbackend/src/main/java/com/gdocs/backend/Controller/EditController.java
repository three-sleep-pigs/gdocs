package com.gdocs.backend.Controller;

import com.gdocs.backend.Service.EditService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

@Controller
@CrossOrigin(origins = "*",maxAge = 3600)
public class EditController {
    @Autowired
    private EditService editService;

    @PostMapping("/publicApi/excel/downData")
    @ResponseBody
    //http://localhost/publicApi/excel/downData?id=1
    public String downExcelData(@RequestParam(value = "id", defaultValue = "-1") Integer id,@RequestParam(value = "version", defaultValue = "-1") Integer version,@RequestParam(value = "edit", defaultValue = "-1") Integer edit) {
        /***
         * 1.从数据库中读取id luckysheet记录
         */

        return editService.downExcelData(id,version,edit);
    }
}
