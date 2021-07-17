package com.gdocs.backend;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gdocs.backend.Entity.Edit;
import com.gdocs.backend.Entity.GFile;
import com.gdocs.backend.Reply.FileReply;
import com.gdocs.backend.Reply.LoginReply;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.context.WebApplicationContext;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Rollback
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@Transactional
public class FileControllerTest extends BackendApplicationTests {
    private MockMvc mockMvc;
    private ObjectMapper om = new ObjectMapper();

    @Autowired
    private WebApplicationContext context;

    @Before
    public void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    public void contextLoads() {
    }

    @Test
    @Transactional
    public void checkGetFileList() throws Exception {
        MvcResult result =  mockMvc.perform(get("/getFiles")).andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        List<GFile> fileList = om.readValue(resultContent, new TypeReference<List<GFile>>() {});
        assertEquals(11,fileList.size());
    }

    @Test
    @Transactional
    public void checkGetBin() throws Exception {
        MvcResult result =  mockMvc.perform(get("/getBin").content("{\"username\":\"123\"}").contentType(MediaType.APPLICATION_JSON_VALUE)).andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        List<GFile> fileList = om.readValue(resultContent, new TypeReference<List<GFile>>() {});
        assertEquals(0,fileList.size());
    }

    @Test
    @Transactional
    public void getExcel() throws Exception {
        MvcResult result =  mockMvc.perform(post("/publicApi/excel/downData?id=10&version=0&edit=58")).andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        System.out.print(resultContent.length());
    }

    @Test
    @Transactional
    public void checkDeleteSuccess() throws Exception {
        MvcResult result =  mockMvc.perform(get("/deleteFile").content("{\"username\":\"123\",\"id\":1}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(200,reply);
    }

    @Test
    @Transactional
    public void checkDeleteEmpty() throws Exception {
        MvcResult result =  mockMvc.perform(get("/deleteFile").content("{\"username\":\"123\",\"id\":11}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(402,reply);
    }

    @Test
    @Transactional
    public void checkDeleteNoAuthority() throws Exception {
        MvcResult result =  mockMvc.perform(get("/deleteFile").content("{\"username\":\"123\",\"id\":10}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(403,reply);
    }

    @Test
    @Transactional
    public void checkRecoverSuccess() throws Exception {
        MvcResult result =  mockMvc.perform(get("/recoverFile").content("{\"username\":\"123\",\"id\":1}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(200,reply);
    }

    @Test
    @Transactional
    public void checkRecoverEmpty() throws Exception {
        MvcResult result =  mockMvc.perform(get("/recoverFile").content("{\"username\":\"123\",\"id\":11}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(402,reply);
    }

    @Test
    @Transactional
    public void checkRecoverNoAuthority() throws Exception {
        MvcResult result =  mockMvc.perform(get("/recoverFile").content("{\"username\":\"123\",\"id\":10}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(403,reply);
    }

    @Test
    @Transactional
    public void checkAddFile() throws Exception {
        MvcResult result =  mockMvc.perform(get("/addFile").content("{\"username\":\"123\",\"filename\":\"addFileTest\"}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        FileReply reply = om.readValue(resultContent, new TypeReference<FileReply>() {});
        assertEquals(200,reply.getStatus());
    }


}
