package com.gdocs.backend.ServiceImpl;

import com.alibaba.fastjson.JSONObject;
import com.gdocs.backend.Dao.GFileDao;
import com.gdocs.backend.Entity.Edit;
import com.gdocs.backend.Entity.GFile;
import com.gdocs.backend.Service.EditService;
import com.gdocs.backend.Util.HTTPUtil;
import com.gdocs.backend.Util.JSONParse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static com.gdocs.backend.Util.Constant.*;

@Service
public class EditServiceImpl implements EditService {
    @Autowired
    private GFileDao gFileDao;

    @Override
    public String downExcelData(Integer id)
    {
        Optional<GFile> optionalGFile = gFileDao.getGFileById(id);
        if (optionalGFile.isPresent())
        {
            GFile gFile = optionalGFile.get();
            String name = gFile.getFilename();
            Integer length = gFile.getLength();
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("Path",id.toString()+".txt");
            jsonObject.put("Offset",0);
            jsonObject.put("Length",length);
            String s;
            try {
                s = HTTPUtil.HttpRestClient(BASIC_URL+ READ_URL, HttpMethod.POST,jsonObject);
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
                    System.out.print("GET+DATA:"+data+"\r");

                }
            }
            return generateReply(data,name,id.toString());
        }

        return "文件不存在";
    }

    private String generateReply(String data,String filename,String index) {
        StringBuilder result = new StringBuilder();
        System.out.print(result.toString()+"\r");
        String title = "[{\"name\":\"" + filename + "\",\"index\":" + index +",\"status\":1,\"order\":\"0\",\"celldata\":[";
        return title + data +"]}]";
    }

}
