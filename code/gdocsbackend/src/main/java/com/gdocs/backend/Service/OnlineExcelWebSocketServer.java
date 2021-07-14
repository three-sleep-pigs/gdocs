package com.gdocs.backend.Service;

import com.alibaba.fastjson.JSON;
import com.gdocs.backend.Configure.GetHttpSessionConfigurator;
import com.gdocs.backend.Util.JSONParse;
import com.gdocs.backend.Util.MyStringUtil;
import com.gdocs.backend.Util.Pako_GzipUtils;
import com.gdocs.backend.WsResultBean;
import com.mongodb.DBObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.websocket.*;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@ServerEndpoint(value = "/excelSocket/{name}", configurator = GetHttpSessionConfigurator.class)
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

    /**
     * 连接成功调用的方法
     * org.springframework.boot.web.servlet.server.Session requestSession,
     *
     * @param session 可选的参数。与某个客户端的连接会话
     */
    @OnOpen
    public void onOpen(Session session, @PathParam("name") String name) {
//        正常情况下，可以用登录的用户名或者token来作为userId
//        如下可以获取到httpSession，与当前的session(socket)不是一样的
//        HttpSession httpSession = (HttpSession) config.getUserProperties().get(HttpSession.class.getName());
//        userId = String.valueOf(httpSession.getAttribute("你的token key"));
        userId = name;
        if (tokenMap.get(userId) == null) {
            onlineCount.incrementAndGet();
        }
        tokenMap.put(userId, this);
        this.session = session;
        log.info("{}建立了连接！", userId);
    }

    /**
     * 连接关闭调用的方法
     */
    @OnClose
    public void onClose() {
        tokenMap.remove(userId);

        onlineCount.decrementAndGet();
        log.info("有一连接关闭！当前连接总数为{}", onlineCount.get());
    }

    @OnMessage
    public void onMessage(String message) {
        if (message.equals("rub")) {//rub代表心跳包
            return;
        }

        for (String key : tokenMap.keySet()) {
            if (!key.equals(userId)) {
                OnlineExcelWebSocketServer socketServer = tokenMap.get(key);
                WsResultBean wsResultBean = new WsResultBean();
                log.info("消息解压前：" + MyStringUtil.getStringShow(message));
                String contentReal = Pako_GzipUtils.unCompressToURI(message);
                log.info("消息解压后：" + MyStringUtil.getStringShow(contentReal));
                wsResultBean.setData(contentReal);
                wsResultBean.setStatus(0);
                wsResultBean.setUsername(userId);
                wsResultBean.setId(wsResultBean.getUsername());
                wsResultBean.setReturnMessage("success");


                DBObject bson = null;
                try {
                    bson = (DBObject) JSONParse.parse(wsResultBean.getData());
                } catch (Exception ex) {
                    return;
                }
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

    @Override
    public int hashCode() {
        return Objects.hash(session);
    }
}
