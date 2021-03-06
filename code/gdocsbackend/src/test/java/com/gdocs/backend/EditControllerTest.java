package com.gdocs.backend;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gdocs.backend.Entity.Edit;
import com.gdocs.backend.Entity.GFile;
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
public class EditControllerTest extends BackendApplicationTests {
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
    public void checkEditFileSuccess() throws Exception {
        MvcResult result =  mockMvc.perform(get("/editFile").content("{\"username\":\"123\",\"id\":10}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(200,reply);
    }

    @Test
    @Transactional
    public void checkEditFileEmpty() throws Exception {
        MvcResult result =  mockMvc.perform(get("/editFile").content("{\"username\":\"123\",\"id\":11}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(400,reply);
    }

    @Test
    @Transactional
    public void checkUpdateFileSuccess() throws Exception {
        MvcResult result =  mockMvc.perform(get("/updateFile").content("{\"id\":10,\"append\":0}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(200,reply);
    }

    @Test
    @Transactional
    public void checkUpdateFileEmpty() throws Exception {
        MvcResult result =  mockMvc.perform(get("/updateFile").content("{\"id\":11,\"append\":0}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(400,reply);
    }

    @Test
    @Transactional
    public void checkGetEditRecord() throws Exception {
        MvcResult result =  mockMvc.perform(get("/getEditRecord").content("{\"id\":10}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        List<Edit> reply = om.readValue(resultContent, new TypeReference<List<Edit>>() {});
        assertEquals(6,reply.size());
    }

    @Test
    @Transactional
    public void checkRollbackReadError() throws Exception {
        MvcResult result =  mockMvc.perform(get("/rollback").content("{\"file\":10,\"edit\":58,\"username\":\"123\"}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(402,reply);
    }

    @Test
    @Transactional
    public void checkRollbackCreateError() throws Exception {
        MvcResult result =  mockMvc.perform(get("/rollback").content("{\"file\":23,\"edit\":52,\"username\":\"123\"}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(403,reply);
    }

    @Test
    @Transactional
    public void checkRollbackSuccess() throws Exception {
        MvcResult result =  mockMvc.perform(get("/rollback").content("{\"file\":24,\"edit\":58,\"username\":\"123\"}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(200,reply);
    }

    @Test
    @Transactional
    public void checkRollbackEmpty() throws Exception {
        MvcResult result =  mockMvc.perform(get("/rollback").content("{\"file\":11,\"edit\":52,\"username\":\"123\"}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(401,reply);
    }
}
