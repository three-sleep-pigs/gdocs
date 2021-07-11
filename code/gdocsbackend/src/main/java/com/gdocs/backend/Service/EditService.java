package com.gdocs.backend.Service;

import com.gdocs.backend.Reply.EditReply;

public interface EditService {
    Integer startEdit(String username,Integer id);

    EditReply getEditors(Integer id);

    Integer exitEdit(String username,Integer id);
}
