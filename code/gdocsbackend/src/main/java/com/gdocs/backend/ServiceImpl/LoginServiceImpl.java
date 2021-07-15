package com.gdocs.backend.ServiceImpl;

import com.gdocs.backend.Dao.GUserDao;
import com.gdocs.backend.Entity.GUser;
import com.gdocs.backend.Reply.LoginReply;
import com.gdocs.backend.Service.LoginService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class LoginServiceImpl implements LoginService {
    @Autowired
    private GUserDao gUserDao;

    @Override
    public LoginReply login(String username, String passwords)
    {
        Optional<GUser> optionalGUser = gUserDao.getGUserByUsername(username);
        LoginReply reply = new LoginReply();
        //GUser存在
        if (optionalGUser.isPresent())
        {
            GUser gUser = optionalGUser.get();
            //密码正确
            if (gUser.getPasswords().equals(passwords))
            {
                reply.setStatus(200);
                reply.setGUser(gUser);
            }
            //密码错误
            else {
                reply.setStatus(401);
            }
        }
        //GUser不存在
        else {
            reply.setStatus(402);
        }
        return reply;
    }

    @Override
    public Integer register(String username,String passwords)
    {
        Optional<GUser> optionalGUser = gUserDao.getGUserByUsername(username);
        if (optionalGUser.isPresent())
        {
            return 401;
        }
        GUser gUser = new GUser();
        gUser.setUsername(username);
        gUser.setPasswords(passwords);
        if (gUserDao.saveUser(gUser) == null)
        {
            return 402;
        }
        return 200;
    }
}
