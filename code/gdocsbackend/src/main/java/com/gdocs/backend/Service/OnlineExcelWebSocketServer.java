package com.gdocs.backend.Service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gdocs.backend.Configure.GetHttpSessionConfigurator;
import com.gdocs.backend.Dao.EditDao;
import com.gdocs.backend.Util.HTTPUtil;
import com.gdocs.backend.Util.JSONParse;
import com.gdocs.backend.Util.Pako_GzipUtils;
import com.gdocs.backend.WsResultBean;
import com.mongodb.DBObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.websocket.*;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.gdocs.backend.Util.Constant.*;

@Slf4j
@Component
@Service
@ServerEndpoint(value = "/excelSocket/{name}/{file}/{version}", configurator = GetHttpSessionConfigurator.class)
public class OnlineExcelWebSocketServer {
    /**
     * 静态变量，用来记录当前连接数
     */
    private static AtomicInteger onlineCount = new AtomicInteger();

    /**
     * concurrent线程安全set，用来存放每个客户端对应的MyWebSocketServer对象
     */
    private static ConcurrentHashMap<String, OnlineExcelWebSocketServer> tokenMap = new ConcurrentHashMap<>();
    /**
     * 与每个客户端的连接会话，需要通过它来给客户端发送数据
     */
    private Session session;
    /***
     * 唯一标识
     */
    private String userId;

    private Integer fileId;

    private boolean edited;

    private Integer version;
    /**
     * 连接成功调用的方法
     * org.springframework.boot.web.servlet.server.Session requestSession,
     *
     * @param session 可选的参数。与某个客户端的连接会话
     */
    @OnOpen
    public void onOpen(Session session, @PathParam("name") String name,@PathParam("file") Integer file,@PathParam("version") Integer v) {
//        正常情况下，可以用登录的用户名或者token来作为userId
//        如下可以获取到httpSession，与当前的session(socket)不是一样的
//        HttpSession httpSession = (HttpSession) config.getUserProperties().get(HttpSession.class.getName());
//        userId = String.valueOf(httpSession.getAttribute("你的token key"));
        userId = name;
        fileId = file;
        version = v;
        edited = false;
        if (tokenMap.get(userId) == null) {
            onlineCount.incrementAndGet();
        }
        tokenMap.put(userId, this);
        this.session = session;
        log.info("{}建立了文件{}连接！", userId,fileId);
    }

    /**
     * 连接关闭调用的方法
     */
    @OnClose
    public void onClose() {
        tokenMap.remove(userId);
        onlineCount.decrementAndGet();
        if (edited)
        {
            String s = null;
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("username", userId);
            jsonObject.put("id", fileId);
            try {
                s = HTTPUtil.HttpRestClient(LOCAL_URL + EDIT_URL, HttpMethod.POST, jsonObject);
                if (s == "400") {
                    log.error("插入编辑记录失败");
                }
            } catch (IOException e) {
                log.error("连接编辑记录失败");
            }
        }
        log.info("有一连接关闭！当前连接总数为{}", onlineCount.get());
    }

    @OnMessage
    public void onMessage(String message) {
        if (message.equals("rub")) {//rub代表心跳包
            return;
        }
        //写入文件
        String contentReal = Pako_GzipUtils.unCompressToURI(message);
        DBObject bson = null;
        try {
            bson = (DBObject) JSONParse.parse(contentReal);
        } catch (Exception ex) {
            return;
        }
        if (bson != null) {
            if (bson.get("t").equals("v")) {
                //更改数据并存储
                edited = true;
                log.info(bson.toString());
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("Path",fileId + "_" + version + ".txt");
                jsonObject.put("Data",bson.toString()+",");
                String s = null;
                try {
                    s = HTTPUtil.HttpRestClient(BASIC_URL + APPEND_URL, HttpMethod.POST,jsonObject);
                } catch (IOException e) {
                    log.error("连接dfs失败");
                }
                //System.out.print(s);
                Map<String,Object> reply= (Map<String,Object>)JSONObject.parse(s);
                if (reply.get("Success").equals(false))
                {
                    log.error(reply.toString());
                    log.error("写dfs失败");
                    return;
                }
                String to = bson.toString()+",";
                JSONObject object = new JSONObject();
                object.put("id", fileId);
                object.put("append",to.length());
                try {
                    s = HTTPUtil.HttpRestClient(LOCAL_URL + UPDATE_URL, HttpMethod.POST, object);
                    if (s == "400") {
                        log.error("更新文件失败");
                    }
                } catch (IOException e) {
                    log.error("连接文件更新失败");
                }
            }
        }

        //转发操作
        for (String key : tokenMap.keySet()) {
            if (!key.equals(userId)) {
                OnlineExcelWebSocketServer socketServer = (OnlineExcelWebSocketServer) tokenMap.get(key);
                if (socketServer.fileId.equals(this.fileId))
                {
                    WsResultBean wsResultBean = null;
                    wsResultBean = new WsResultBean();
                    wsResultBean.setData(contentReal);
                    wsResultBean.setStatus(0);
                    wsResultBean.setUsername(userId);
                    wsResultBean.setId(wsResultBean.getUsername());
                    wsResultBean.setReturnMessage("success");
                    if (bson != null) {
                        if (bson.get("t").equals("mv")) {
                            //更新选区显示
                            wsResultBean.setType(3);
                        } else {
                            //更新数据
                            wsResultBean.setType(2);
                        }
                    }
                    socketServer.sendMessage(wsResultBean, socketServer.session);
                }
            }
        }
    }

    @OnError
    public void onError(Session session, Throwable error) {
        log.error("WebSocket接收消息错误{},sessionId为{}", error.getMessage(), session.getId());
        error.printStackTrace();
    }

    /**
     * 服务端发送消息给客户端
     */
    private void sendMessage(WsResultBean wsResultBean, Session toSession) {
        try {
            log.info("服务端给客户端[{}]发送消息", toSession.getId());
            toSession.getBasicRemote().sendText(JSON.toJSONString(wsResultBean));
        } catch (Exception e) {
            log.error("服务端发送消息给客户端失败：{}", e);
        }
    }


}
