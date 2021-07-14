package com.gdocs.backend.ServiceImpl;

import com.gdocs.backend.Dao.EditDao;
import com.gdocs.backend.Dao.GFileDao;
import com.gdocs.backend.Entity.Edit;
import com.gdocs.backend.Entity.GFile;
import com.gdocs.backend.Reply.FileReply;
import com.gdocs.backend.Service.FileService;
import com.gdocs.backend.Util.CellData;
import com.gdocs.backend.Util.CellType;
import com.gdocs.backend.Util.Sheet;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;

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
    public FileReply getFileByID(Integer id)
    {
        FileReply reply = new FileReply();
        List<Sheet> sheets = new ArrayList<>();
        Sheet sheet = new Sheet();
        Map<Integer,Map<Integer,CellData>> CellData = new HashMap<>();
        CellType cellType = new CellType();
        Optional<GFile> optionalGFile = gFileDao.getGFileById(id);
        if(optionalGFile.isPresent()){
            GFile gFile = optionalGFile.get();
            sheet.setName(gFile.getFilename());
            sheet.setIndex(gFile.getId().toString());
            sheet.setOrder(0);
            sheet.setStatus(1);
            ArrayList<String> arrayList = new ArrayList<>();
            try {
                FileReader reader = new FileReader(DIR_PATH + id + ".txt");
                BufferedReader bufferedReader = new BufferedReader(reader);
                String string;
                while ((string = bufferedReader.readLine()) != null) {
                    arrayList.add(string);
                }
                bufferedReader.close();
                reader.close();
            } catch (IOException e) {
                reply.setStatus(401);//文件打开失败
            }
            System.out.print(arrayList);
            reply.setStatus(200);
        }
        else {
            reply.setStatus(401);//文件打开失败
        }
        reply.setSheets(sheets);
        return reply;
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
                File file = new File(DIR_PATH+id.toString()+".txt");
                if (file.exists()) {
                    if (file.delete()) {
                        if (gFileDao.deleteGFileById(id) == 1)
                        {
                            return 200;//删除成功
                        }
                    }
                    else {
                        return 401;//删除失败
                    }
                }
                else {
                    return 402;//文件不存在
                }
            }
            else {
                return 403;//无删除权限
            }
        }
        return 402;//文件不存在
    }

    @Override
    public Integer addFile(String username,String filename)
    {
        GFile gFile = new GFile();
        gFile.setFilename(filename);
        gFile.setCreator(username);
        gFile.setRecent(Timestamp.valueOf(LocalDateTime.now()));
        if (gFileDao.saveFile(gFile) != null)
        {
            File file = new File(DIR_PATH + gFile.getId() + ".txt");
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    return 400;//创建文件失败
                }
            }
            Edit edit = new Edit();
            edit.setFileid(gFile.getId());
            edit.setEditor(username);
            edit.setEdittime(gFile.getRecent());
            editDao.save(edit);
            return 200;
        }
        return 400;
    }

    @Override
    public Integer editFile(String username, String index, String row, String column, String value)
    {
        FileWriter writer;
        try {
            writer = new FileWriter(DIR_PATH + index + ".txt",true);
        } catch (IOException e) {
            return 401;//文件打开失败
        }
        try {
            writer.write(row + " " + column + " " + value.toString() + "\r\n");
            writer.flush();
            writer.close();
        } catch (IOException e) {
            return 402;//写入文件失败
        }
        Edit edit = new Edit();
        edit.setEditor(username);
        edit.setFileid(Integer.parseInt(index));
        edit.setEdittime(Timestamp.valueOf(LocalDateTime.now()));
        editDao.save(edit);
        gFileDao.setRecentById(edit.getEdittime(),Integer.parseInt(index));
        return 200;
    }
}
