package com.gdocs.backend.Controller;

import org.springframework.ui.ModelMap;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.net.URLDecoder;
import java.util.Random;

@RestController
public class TestController {

    @GetMapping("/home")
    public String home() {
        return "test";
    }

    @GetMapping("/")
    public String onlineProductExcel(ModelMap modelMap, HttpServletRequest request, @RequestParam(value = "id", defaultValue = "27") int id) {
        String userName = "aa";
        modelMap.addAttribute("recordId", "-1");
        modelMap.addAttribute("userName", 1);
        modelMap.addAttribute("title", "协同演示");
        return "online_excel";
    }

    @PostMapping("/publicApi/excel/downData")
    @ResponseBody
    //http://localhost/publicApi/excel/downData?id=1
    public String downExcelData(ModelMap modelMap, @RequestParam(value = "id", defaultValue = "-1") int id) {
        /***
         * 1.从数据库中读取id luckysheet记录
         */

        File file = null;
        try {
            //模拟从数据库拿luckysheet数据
            file = new File(URLDecoder.decode(ResourceUtils.getURL("classpath:static/exceldata").getPath(), "utf-8"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return txtToString(new File(file.getAbsolutePath()));
    }

    private String txtToString(File file) {
        StringBuilder result = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String s = null;
            while ((s = br.readLine()) != null) {
                result.append(s);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.print(result);
        return result.toString();
    }
}
