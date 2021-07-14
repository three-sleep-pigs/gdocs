package com.gdocs.backend.Controller;

import com.gdocs.backend.Entity.GFile;
import com.gdocs.backend.Service.FileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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

    @RequestMapping(path = "/getBin")
    public List<GFile> getBin(@RequestBody Map<String,String> params)
    {
        String username = params.get("username");
        return fileService.getBin(username);
    }

    @RequestMapping("/addFile")
    public Integer addFile(@RequestBody Map<String,String> params)
    {
        String username = params.get("username");
        String filename = params.get("filename");
        return fileService.addFile(username,filename);
    }

    @RequestMapping("/deleteFile")
    public Integer deleteFile(@RequestBody Map<String,String> params)
    {
        String username = params.get("username");
        Integer id = Integer.parseInt(params.get("id"));
        return fileService.deleteFileByID(username,id);
    }

    @RequestMapping("/recoverFile")
    public Integer recoverFile(@RequestBody Map<String,String> params)
    {
        String username = params.get("username");
        Integer id = Integer.parseInt(params.get("id"));
        return fileService.recoverGFileById(username,id);
    }

}
