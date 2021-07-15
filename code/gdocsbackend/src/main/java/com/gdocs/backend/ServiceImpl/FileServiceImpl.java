package com.gdocs.backend.ServiceImpl;

import com.alibaba.fastjson.JSONObject;
import com.gdocs.backend.Dao.EditDao;
import com.gdocs.backend.Dao.GFileDao;
import com.gdocs.backend.Entity.Edit;
import com.gdocs.backend.Entity.GFile;
import com.gdocs.backend.Reply.FileReply;
import com.gdocs.backend.Service.FileService;
import com.gdocs.backend.Util.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;

import java.io.*;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;

import static com.gdocs.backend.Util.Constant.BASIC_URL;
import static com.gdocs.backend.Util.Constant.CREATE_URL;

@Service
public class FileServiceImpl implements FileService {
    private static final String DIR_PATH = "C:\\Users\\peach\\Desktop\\gdocs-three-sleepy-pigs\\code\\files\\";
    @Autowired
    private GFileDao gFileDao;

    @Autowired
    private EditDao editDao;

    @Override
    public List<GFile> getFiles()
    {
        return gFileDao.getGFiles();
    }

    @Override
    public List<GFile> getBin(String creator)
    {
        return gFileDao.getBin(creator);
    }

    @Override
    public FileReply addFile(String username, String filename)
    {
        FileReply fileReply = new FileReply();
        GFile gFile = new GFile();
        gFile.setFilename(filename);
        gFile.setCreator(username);
        gFile.setLength(0);
        gFile.setDeleted(false);
        gFile.setRecent(Timestamp.valueOf(LocalDateTime.now()));
        if (gFileDao.saveFile(gFile) != null)
        {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("Path",gFile.getId().toString()+".txt");
            System.out.print(gFile.getId());
            String s;
            try {
                s = HTTPUtil.HttpRestClient(BASIC_URL + CREATE_URL, HttpMethod.POST,jsonObject);
            } catch (IOException e) {
                fileReply.setStatus(400);
                return fileReply;
            }
            Map<String,Object> reply= (Map<String,Object>)JSONObject.parse(s);
            System.out.print(reply + "\r");
           if (reply.get("Success").equals(true))
           {
               Edit edit = new Edit();
               edit.setFileid(gFile.getId());
               edit.setEditor(username);
               edit.setEdittime(gFile.getRecent());
               edit.setLength(0);
               editDao.save(edit);
               fileReply.setStatus(200);
               fileReply.setGfile(gFile);
               return fileReply;
           }
        }
        fileReply.setStatus(400);
        return fileReply;
    }

    @Override
    public Integer deleteFileByID(String username,Integer id)
    {
        Optional<GFile> optionalGFile = gFileDao.getGFileById(id);
        if (optionalGFile.isPresent())
        {
            GFile gFile = optionalGFile.get();
            if (gFile.getCreator().equals(username))
            {
                if (gFileDao.deleteGFileById(id) == 1)
                {
                    return 200;//删除成功
                } else {
                    return 401;//删除失败
                }
            } else {
                return 403;//无删除权限
            }
        }
        return 402;//文件不存在
    }

    @Override
    public Integer recoverGFileById(String username,Integer id)
    {
        Optional<GFile> optionalGFile = gFileDao.getGFileById(id);
        if (optionalGFile.isPresent())
        {
            GFile gFile = optionalGFile.get();
            if (gFile.getCreator().equals(username))
            {
                if (gFileDao.recoverGFileById(id) == 1)
                {
                    return 200;//恢复成功
                } else {
                    return 401;//恢复失败
                }
            } else {
                return 403;//无恢复权限
            }
        }
        return 402;//文件不存在
    }

    @Override
    public Integer editFileByID(String username,Integer fileId)
    {
        Optional<GFile> optionalGFile = gFileDao.getGFileById(fileId);
        Edit edit = new Edit();
        if (optionalGFile.isPresent())
        {
            GFile gFile = optionalGFile.get();
            edit.setEditor(username);
            edit.setFileid(fileId);
            edit.setLength(gFile.getLength());
            edit.setEdittime(Timestamp.valueOf(LocalDateTime.now()));
            gFileDao.setRecentById(edit.getEdittime(),fileId);
        }

        if (editDao.save(edit) == null)
        {
            return 400;
        }
        return 200;
    }

    @Override
    public Integer updateFileByID(Integer fileId,Integer append)
    {
        Optional<GFile> optionalGFile = gFileDao.getGFileById(fileId);
        if (optionalGFile.isPresent())
        {
            GFile gFile = optionalGFile.get();
            Integer length = gFile.getLength();
            gFileDao.setLengthById(length+append,fileId);
            return 200;
        }
        return 400;
    }

    @Override
    public List<Edit> getEditsByFileId(Integer fileId)
    {
        return editDao.getEditsByFileId(fileId);
    }

    @Override
    public Integer rollback(Integer fileId,Integer editId)
    {
        if (gFileDao.setLengthById(editDao.getById(editId).getLength(),fileId) == 1)
            return 200;
        return 400;
    }
}
