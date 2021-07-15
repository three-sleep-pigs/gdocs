package com.gdocs.backend.Controller;

import com.gdocs.backend.Reply.LoginReply;
import com.gdocs.backend.Service.LoginService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@CrossOrigin(origins = "*",maxAge = 3600)
public class LoginController {
    @Autowired
    private LoginService loginService;

    @RequestMapping(path = "/login")
    public LoginReply login(@RequestBody Map<String,String> params){
        String username= params.get("username");
        String passwords= params.get("passwords");
        return loginService.login(username,passwords);
    }

    @RequestMapping(path = "/register")
    public Integer register(@RequestBody Map<String,String> params){
        String username= params.get("username");
        String passwords= params.get("passwords");
        return loginService.register(username,passwords);
    }
}
