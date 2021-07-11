package com.gdocs.backend.Controller;

import com.gdocs.backend.Reply.EditReply;
import com.gdocs.backend.Service.EditService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@CrossOrigin(origins = "*",maxAge = 3600)
public class EditController {
    @Autowired
    private EditService editService;

    @RequestMapping(path = "/startEdit")
    public Integer startEdit(@RequestBody Map<String,String> params){
        String username= params.get("username");
        Integer id= Integer.parseInt(params.get("id"));
        return editService.startEdit(username,id);
    }

    @RequestMapping(path = "/getEditors")
    public EditReply getEditors(@RequestBody Map<String,Integer> params){
        Integer id = params.get("id");
        return editService.getEditors(id);
    }

    @RequestMapping(path = "/exitEdit")
    public Integer exitEdit(@RequestBody Map<String,String> params){
        String username= params.get("username");
        Integer id= Integer.parseInt(params.get("id"));
        return editService.exitEdit(username,id);
    }
}
