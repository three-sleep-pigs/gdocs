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

import static com.gdocs.backend.Util.Constant.*;

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
        gFile.setVersion(0);
        gFile.setDeleted(false);
        gFile.setRecent(Timestamp.valueOf(LocalDateTime.now()));
        if (gFileDao.saveFile(gFile) != null)
        {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("Path",gFile.getId()+ "_0.txt");
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
               edit.setVersion(0);
               edit.setOperation(0);
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
                    Edit edit = new Edit();
                    edit.setFileid(id);
                    edit.setEditor(username);
                    edit.setEdittime(Timestamp.valueOf(LocalDateTime.now()));
                    edit.setOperation(2);
                    edit.setVersion(gFile.getVersion());
                    edit.setLength(gFile.getLength());
                    editDao.save(edit);
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
                    Edit edit = new Edit();
                    edit.setFileid(id);
                    edit.setEditor(username);
                    edit.setEdittime(Timestamp.valueOf(LocalDateTime.now()));
                    edit.setOperation(3);
                    edit.setVersion(gFile.getVersion());
                    edit.setLength(gFile.getLength());
                    editDao.save(edit);
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
            edit.setOperation(1);
            edit.setVersion(gFile.getVersion());
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
    public Integer rollback(Integer fileId,Integer editId,String username)
    {
        Edit previous = editDao.getById(editId);
        Integer previousLength = previous.getLength();
        Optional<GFile> optionalGFile = gFileDao.getGFileById(fileId);
        if (optionalGFile.isPresent())
        {
            GFile gFile = optionalGFile.get();
            Integer newVersion = gFile.getVersion() + 1;
            Edit edit = new Edit();
            edit.setFileid(fileId);
            edit.setEditor(username);
            edit.setEdittime(Timestamp.valueOf(LocalDateTime.now()));
            edit.setOperation(2);
            edit.setVersion(newVersion);
            edit.setLength(previousLength);
            editDao.save(edit);
            gFileDao.setRecentById(edit.getEdittime(),fileId);
            gFileDao.setVersionById(newVersion,fileId);
            gFileDao.setLengthById(previousLength,fileId);

            //从旧版本读取数据
            JSONObject readObject = new JSONObject();
            readObject.put("Path",fileId + "_" + previous.getVersion() + ".txt");
            readObject.put("Offset",0);
            readObject.put("Length",previousLength);
            String readReply;
            try {
                readReply = HTTPUtil.HttpRestClient(BASIC_URL+ READ_URL, HttpMethod.POST,readObject);
            } catch (IOException e) {
                return 402;//读取DFS错误
            }
            Map<String,Object> rReplyMap= (Map<String,Object>)JSONObject.parse(readReply);
            if (rReplyMap.get("Success").equals(false))
            {
                return 402;
            }
            String data = (String) rReplyMap.get("Data");
            //创建新文件
            JSONObject createObject = new JSONObject();
            createObject.put("Path",fileId + "_" + newVersion + ".txt");
            String createReply;
            try {
                createReply = HTTPUtil.HttpRestClient(BASIC_URL + CREATE_URL, HttpMethod.POST,createObject);
            } catch (IOException e) {
                return 403;//创建新文件错误
            }
            Map<String,Object> cReplyMap= (Map<String,Object>)JSONObject.parse(createReply);
            if (cReplyMap.get("Success").equals(false))
            {
                return 403;
            }

            //写入新文件
            JSONObject writeObject = new JSONObject();
            writeObject.put("Path",fileId + "_" + newVersion + ".txt");
            writeObject.put("Data",data);
            String writeReply;
            try {
                writeReply = HTTPUtil.HttpRestClient(BASIC_URL+ APPEND_URL, HttpMethod.POST,writeObject);
            } catch (IOException e) {
                return 404;//写入DFS错误
            }
            Map<String,Object> wReplyMap= (Map<String,Object>)JSONObject.parse(writeReply);
            if (wReplyMap.get("Success").equals(false))
            {
                return 404;
            }
            return 200;
        }
        return 401;//文件不存在
    }
}
