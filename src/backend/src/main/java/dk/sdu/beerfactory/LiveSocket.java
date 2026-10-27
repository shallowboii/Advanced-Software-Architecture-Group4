package dk.sdu.beerfactory;

import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.config.annotation.*;
import org.springframework.web.socket.handler.*;

@Component
public class LiveSocket extends TextWebSocketHandler {
    private final ConcurrentHashMap<String,WebSocketSession> sessions=new ConcurrentHashMap<>();
    private final Json json;
    public LiveSocket(Json json) { this.json=json; }
    @Override public void afterConnectionEstablished(WebSocketSession session) { sessions.put(session.getId(),new ConcurrentWebSocketSessionDecorator(session,1000,1024*1024)); }
    @Override public void afterConnectionClosed(WebSocketSession session,CloseStatus status) { sessions.remove(session.getId()); }
    public void publish(String type,Object data) {
        TextMessage message=new TextMessage(json.write(java.util.Map.of("type",type,"data",data)));
        sessions.forEach((id,session)-> {
            try { if(session.isOpen()) session.sendMessage(message); else sessions.remove(id); }
            catch(Exception e) { sessions.remove(id);try{session.close();}catch(Exception ignored){} }
        });
    }
    @Configuration
    @EnableWebSocket
    public static class WebSocketConfig implements WebSocketConfigurer {
        private final LiveSocket socket;
        public WebSocketConfig(LiveSocket socket) { this.socket=socket; }
        @Override public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) { registry.addHandler(socket,"/ws"); }
    }
}
