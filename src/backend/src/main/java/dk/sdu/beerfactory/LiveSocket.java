package dk.sdu.beerfactory;

import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.config.annotation.*;
import org.springframework.web.socket.handler.*;

/**
 * Broadcasts read-only dashboard projections through bounded, thread-safe raw WebSocket sessions.
 */
@Component
public class LiveSocket extends TextWebSocketHandler {
  private final ConcurrentHashMap<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
  private final Json json;

  /**
   * Wires the collaborators for this live socket; no production transition occurs during
   * construction.
   */
  public LiveSocket(Json json) {
    this.json = json;
  }

  @Override
  /** Registers a bounded thread-safe WebSocket session for future dashboard projections. */
  public void afterConnectionEstablished(WebSocketSession session) {
    sessions.put(
        session.getId(), new ConcurrentWebSocketSessionDecorator(session, 1000, 1024 * 1024));
  }

  @Override
  /** Removes a disconnected WebSocket session from the broadcast registry. */
  public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
    sessions.remove(session.getId());
  }

  /** Broadcasts a typed JSON dashboard update, removing closed or failed sessions. */
  public void publish(String type, Object data) {
    TextMessage message = new TextMessage(json.write(java.util.Map.of("type", type, "data", data)));
    sessions.forEach(
        (id, session) -> {
          try {
            if (session.isOpen()) session.sendMessage(message);
            else sessions.remove(id);
          } catch (Exception e) {
            sessions.remove(id);
            try {
              session.close();
            } catch (Exception ignored) {
            }
          }
        });
  }

  @Configuration
  @EnableWebSocket
  public static class WebSocketConfig implements WebSocketConfigurer {
    private final LiveSocket socket;

    /** Connects the dashboard socket handler to Spring WebSocket configuration. */
    public WebSocketConfig(LiveSocket socket) {
      this.socket = socket;
    }

    @Override
    /** Exposes the same-origin raw JSON WebSocket endpoint at /ws. */
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
      registry.addHandler(socket, "/ws");
    }
  }
}
