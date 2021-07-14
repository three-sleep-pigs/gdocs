package com.gdocs.backend.ServiceImpl;

import com.alibaba.fastjson.JSONObject;
import com.gdocs.backend.Service.EditService;
import com.gdocs.backend.Util.HTTPUtil;
import com.gdocs.backend.Util.JSONParse;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;

import static com.gdocs.backend.Util.Constant.BASIC_URL;
import static com.gdocs.backend.Util.Constant.READ_URL;

@Service
public class EditServiceImpl implements EditService {
    @Override
    public String downExcelData(int id)
    {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("Path","6");
        jsonObject.put("Offset",0);
        jsonObject.put("Length",1000);
        String s;
        try {
            s = HTTPUtil.HttpRestClient(BASIC_URL + READ_URL, HttpMethod.POST,jsonObject);
        } catch (IOException e) {
            return "连接DFS错误";
        }
        System.out.print(s);
        HashMap<String,Object> reply = null;
        try {
            reply = (HashMap<String,Object>) JSONParse.parse(s);
        } catch (Exception ex) {
            return "解析错误";
        }
        String data = "";
        if (reply != null) {
            if (reply.get("Success").equals(true))
            {
                data = reply.get("Data").toString();
                System.out.print(data);
                return generateReply(data,"1","1");
            }
        }
        return "DFS read error";
    }

    private String generateReply(String data,String filename,String index) {
        StringBuilder result = new StringBuilder();
        System.out.print(result.toString());
        String title = "[{\"name\":" + filename + ",\"index\":" + index +",\"status\":1,\"order\":\"0\",\"celldata\":[";
        return title + data +"]}]";
    }
}
