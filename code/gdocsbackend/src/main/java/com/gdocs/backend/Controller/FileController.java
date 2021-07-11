package com.gdocs.backend.Controller;

import com.gdocs.backend.Entity.GFile;
import com.gdocs.backend.Reply.FileReply;
import com.gdocs.backend.Service.FileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.relational.core.sql.In;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@CrossOrigin(origins = "*",maxAge = 3600)
public class FileController {
    @Autowired
    private FileService fileService;

    @RequestMapping(path = "/getFiles")
    public List<GFile> getFiles()
    {
        return fileService.getFiles();
    }

    @RequestMapping("/addFile")
    public FileReply addFile(@RequestBody Map<String,String> params)
    {
        String username = params.get("username");
        String filename = params.get("filename");
        return fileService.addFile(username,filename);
    }

    @RequestMapping("/getFile")
    public FileReply getFileByID(@RequestBody Map<String,Integer> params)
    {
        Integer id = params.get("id");
        return fileService.getFileByID(id);
    }

//    @RequestMapping("/editFile")
//    public Integer editFile(@RequestBody Map<String,String> params)
//    {
//        String username = params.get("username");
//        Integer id = Integer.parseInt(params.get("id"));
//
//    }

    @RequestMapping("/deleteFile")
    public Integer deleteFile(@RequestBody Map<String,String> params)
    {
        String username = params.get("username");
        Integer id = Integer.parseInt(params.get("id"));
        return fileService.deleteFileByID(username,id);
    }

}
