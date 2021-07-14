package com.gdocs.backend.Controller;

import com.gdocs.backend.Entity.GFile;
import com.gdocs.backend.Reply.FileReply;
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

    @RequestMapping("/addFile")
    public Integer addFile(@RequestBody Map<String,String> params)
    {
        String username = params.get("username");
        String filename = params.get("filename");
        return fileService.addFile(username,filename);
    }

    @RequestMapping("/editFile")
    public Integer editFile(@RequestParam(name = "name") String username,@RequestBody Map<String,Object> params)
    {
        String index = params.get("i").toString();
        String row = params.get("r").toString();
        String column = params.get("c").toString();
        String value = params.get("v").toString();
        fileService.editFile(username,index,row,column,value);
        return 200;
    }

    @RequestMapping("/deleteFile")
    public Integer deleteFile(@RequestBody Map<String,String> params)
    {
        String username = params.get("username");
        Integer id = Integer.parseInt(params.get("id"));
        return fileService.deleteFileByID(username,id);
    }

}
