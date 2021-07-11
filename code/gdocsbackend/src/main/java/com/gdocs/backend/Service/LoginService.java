package com.gdocs.backend.Service;

import com.gdocs.backend.Reply.LoginReply;

public interface LoginService {
    LoginReply login(String username,String passwords);

    Integer register(String username,String passwords);
}
