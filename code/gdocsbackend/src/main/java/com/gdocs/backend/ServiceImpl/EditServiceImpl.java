package com.gdocs.backend.ServiceImpl;

import com.gdocs.backend.Reply.EditReply;
import com.gdocs.backend.Service.EditService;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

import static com.gdocs.backend.Util.Constant.EditorsMap;

@Service
public class EditServiceImpl implements EditService {
    @Override
    public Integer startEdit(String username,Integer id)
    {
        Set<String> editors = EditorsMap.get(id);
        if (editors == null)
        {
            editors = new HashSet<>();
        }
        editors.add(username);
        EditorsMap.put(id,editors);
        return 200;
    }

    @Override
    public EditReply getEditors(Integer id)
    {
        EditReply reply = new EditReply();
        Set<String> editors = EditorsMap.get(id);
        if (editors != null)
        {
            reply.setStatus(200);
            reply.setCoworkers(editors);
        }
        else {
            reply.setStatus(400);
        }
        return reply;
    }

    @Override
    public Integer exitEdit(String username,Integer id)
    {
        Set<String> editors = EditorsMap.get(id);
        if (editors == null) {
            return 400;
        }
        editors.remove(username);
        EditorsMap.put(id,editors);
        return 200;
    }
}
