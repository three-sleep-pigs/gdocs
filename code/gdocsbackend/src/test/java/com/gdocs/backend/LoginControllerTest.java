package com.gdocs.backend;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Rollback
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@Transactional
public class LoginControllerTest extends BackendApplicationTests {
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
    public void checkLoginSuccess() throws Exception {
        MvcResult result =  mockMvc.perform(get("/login").content("{\"username\":\"123\",\"passwords\":\"123\"}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        LoginReply loginReply = om.readValue(resultContent, new TypeReference<LoginReply>() {});
        assertEquals(200,loginReply.getStatus());
    }

    @Test
    @Transactional
    public void checkLoginWrongPwd() throws Exception {
        MvcResult result =  mockMvc.perform(get("/login").content("{\"username\":\"123\",\"passwords\":\"wrong pwd\"}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        LoginReply loginReply = om.readValue(resultContent, new TypeReference<LoginReply>() {});
        assertEquals(401,loginReply.getStatus());
    }

    @Test
    @Transactional
    public void checkLoginNoRegistered() throws Exception {
        MvcResult result =  mockMvc.perform(get("/login").content("{\"username\":\"NotRegistered\",\"passwords\":\"pwd\"}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        LoginReply loginReply = om.readValue(resultContent, new TypeReference<LoginReply>() {});
        assertEquals(402,loginReply.getStatus());
    }

    @Test
    @Transactional
    public void checkRegisterSuccess() throws Exception {
        MvcResult result =  mockMvc.perform(get("/register").content("{\"username\":\"new1\",\"passwords\":\"pwd\"}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(200,reply);
    }

    @Test
    @Transactional
    public void checkRegisterRepeated() throws Exception {
        MvcResult result =  mockMvc.perform(get("/register").content("{\"username\":\"123\",\"passwords\":\"pwd\"}").contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk()).andReturn();
        String resultContent = result.getResponse().getContentAsString();
        Integer reply = om.readValue(resultContent, new TypeReference<Integer>() {});
        assertEquals(401,reply);
    }
}
