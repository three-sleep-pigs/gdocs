package com.gdocs.backend.Controller;

import com.gdocs.backend.Entity.Edit;
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

    @RequestMapping(path = "/getBin")
    public List<GFile> getBin(@RequestBody Map<String,String> params)
    {
        String username = params.get("username");
        return fileService.getBin(username);
    }

    @RequestMapping("/addFile")
    public FileReply addFile(@RequestBody Map<String,String> params)
    {
        System.out.print("create files:" + params);
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

    @RequestMapping("/editFile")
    public Integer editFile(@RequestBody Map<String,String> params)
    {
        String username = params.get("username");
        Integer id = Integer.parseInt(params.get("id"));
        return fileService.editFileByID(username,id);
    }

    @RequestMapping("/updateFile")
    public Integer updateFile(@RequestBody Map<String,String> params)
    {
        Integer id = Integer.parseInt(params.get("id"));
        Integer append = Integer.parseInt(params.get("append"));
        return fileService.updateFileByID(id,append);
    }

    @RequestMapping("/recoverFile")
    public Integer recoverFile(@RequestBody Map<String,String> params)
    {
        String username = params.get("username");
        Integer id = Integer.parseInt(params.get("id"));
        return fileService.recoverGFileById(username,id);
    }

    @RequestMapping("/getEditRecord")
    public List<Edit> getEditsByFileId(@RequestBody Map<String,Integer> params)
    {
        Integer fileId = params.get("id");
        return fileService.getEditsByFileId(fileId);
    }

    @RequestMapping("/rollback")
    public Integer rollback(@RequestBody Map<String,Integer> params)
    {
        Integer fileId = params.get("file");
        Integer editId = params.get("edit");
        System.out.print(fileId + "," + editId);
        return fileService.rollback(fileId,editId);
    }
}
