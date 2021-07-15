package com.gdocs.backend.Reply;

import com.gdocs.backend.Entity.GUser;
import lombok.Data;

@Data
public class LoginReply {
    //200:成功 401:密码错误 402:未注册
    private Integer status;
    private GUser gUser;
}
