package com.gdocs.backend.Configure;

import javax.servlet.http.HttpSession;
import javax.websocket.HandshakeResponse;
import javax.websocket.server.HandshakeRequest;
import javax.websocket.server.ServerEndpointConfig;

public class GetHttpSessionConfigurator extends ServerEndpointConfig.Configurator {

    @Override
    public void modifyHandshake(ServerEndpointConfig sec, HandshakeRequest request, HandshakeResponse response) {
        if (request.getHttpSession() !=null) {
            HttpSession httpSession = (HttpSession) request.getHttpSession();
            sec.getUserProperties().put(HttpSession.class.getName(), httpSession);
        }
    }
}
