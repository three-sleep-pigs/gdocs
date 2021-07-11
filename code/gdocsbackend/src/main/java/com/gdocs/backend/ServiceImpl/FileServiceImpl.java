package com.gdocs.backend.ServiceImpl;

import com.gdocs.backend.Dao.EditDao;
import com.gdocs.backend.Dao.GFileDao;
import com.gdocs.backend.Entity.GFile;
import com.gdocs.backend.Reply.FileReply;
import com.gdocs.backend.Service.FileService;
import com.gdocs.backend.Util.CellData;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

@Service
public class FileServiceImpl implements FileService {
    private static final String DIR_PATH = "C:/Users/peach/Desktop/gdocs-three-sleepy-pigs/code/files/";
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
        Optional<GFile> optionalGFile = gFileDao.getGFileById(id);
        if(optionalGFile.isPresent()){
            GFile gFile = optionalGFile.get();
            reply.setGFile(gFile);
            File file = new File(DIR_PATH+id.toString()+".xml");
            if (file.exists()) {
                reply.setStatus(200);
                reply.setFile(file);
            }
            else {
                reply.setStatus(402);
            }
        }
        else {
            reply.setStatus(401);
        }
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
                if(gFileDao.deleteGFileById(id) == 1)
                {
                    File file = new File(DIR_PATH+id.toString()+".xml");
                    if (file.exists()) {
                        if (file.delete()) {
                            return 200;//删除成功
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
                    return 401;//删除失败
                }
            }
            else {
                return 403;//无删除权限
            }
        }
        else {
            return 402;//文件不存在
        }
    }

    @Override
    public FileReply addFile(String username,String filename)
    {
        FileReply reply = new FileReply();
        GFile gFile = new GFile();
        gFile.setFilename(filename);
        gFile.setCreator(username);
        if (gFileDao.saveFile(gFile) != null)
        {
            reply.setGFile(gFile);
            File file = new File(DIR_PATH + gFile.getId() + ".xls");
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    reply.setStatus(400);//创建文件失败
                    return reply;
                }
            }
            reply.setStatus(200);
            reply.setFile(file);
        }
        else {
            reply.setStatus(400);
        }
        return reply;
    }

    @Override
    public Integer editFile(String username,Integer fileId,List<CellData> cellDataList)
    {
        XSSFWorkbook xssfWorkbook;
        try {
            xssfWorkbook = new XSSFWorkbook(new FileInputStream(new File((DIR_PATH + fileId.toString() + ".xls"))));
        } catch (IOException e) {
            return 401;//打开文件失败
        }

        XSSFSheet sheet = xssfWorkbook.getSheetAt(0);

        for (CellData cellData : cellDataList) {
            XSSFRow xssfRow = sheet.getRow(cellData.getRow());
            XSSFCell xssfCell = xssfRow.getCell(cellData.getColumn());
            xssfCell.setCellValue(cellData.getData());
        }

        try {
            xssfWorkbook.write(new FileOutputStream(new File(DIR_PATH + fileId.toString() + ".xls")));
        } catch (IOException e) {
            return 402;//写入文件失败
        }
        return 200;
    }
}
