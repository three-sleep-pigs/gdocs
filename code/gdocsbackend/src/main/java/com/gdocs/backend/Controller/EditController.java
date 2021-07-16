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

    @GetMapping("/home")
    public String home() {
        return "test";
    }

    @GetMapping("/")
    //http://localhost
    public String onlineProductExcel(ModelMap modelMap, HttpServletRequest request, @RequestParam(value = "id", defaultValue = "27") int id) {
        String userName = "aa";
        modelMap.addAttribute("recordId", "-1");
        modelMap.addAttribute("userName", id);
        modelMap.addAttribute("title", "协同演示");
        return "online_excel";
    }

    @PostMapping("/publicApi/excel/downData")
    @ResponseBody
    //http://localhost/publicApi/excel/downData?id=1
    public String downExcelData(@RequestParam(value = "id", defaultValue = "-1") Integer id,@RequestParam(value = "version", defaultValue = "-1") Integer version) {
        /***
         * 1.从数据库中读取id luckysheet记录
         */
        System.out.print(id + "\r");
        return editService.downExcelData(id,version);
    }


}
