package dk.sdu.beerfactory;

import jakarta.annotation.PreDestroy;
import java.sql.*;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Elects one coordinator using a PostgreSQL session advisory lock and monotonically increasing
 * fencing epoch. This protects backend replication, not controller process recovery.
 */
@Component
public class Leadership {
  private static final long LOCK = 4263371986L;
  private static final Logger log = LoggerFactory.getLogger(Leadership.class);
  private final DataSource source;
  private final Store store;
  private final Bootstrap bootstrap;
  private Connection connection;
  private volatile long epoch;

  /**
   * Wires the collaborators for this leadership; no production transition occurs during
   * construction.
   */
  public Leadership(DataSource source, Store store, Bootstrap bootstrap) {
    this.source = source;
    this.store = store;
    this.bootstrap = bootstrap;
  }

  /** Returns the locally held coordinator epoch, or zero when this replica is not leader. */
  public long epoch() {
    return epoch;
  }

  /** Reports whether this replica currently holds a coordinator epoch. */
  public boolean active() {
    return epoch > 0;
  }

  @Scheduled(fixedDelay = 400)
  /**
   * Acquires or validates the session advisory lock and advances the durable fencing epoch on
   * leadership handover.
   */
  public synchronized void maintain() {
    if (!bootstrap.ready) return;
    try {
      if (connection != null) {
        if (!connection.isValid(1)) close();
        return;
      }
      Connection candidate = source.getConnection();
      try (Statement stmt = candidate.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT pg_try_advisory_lock(" + LOCK + ")")) {
        rs.next();
        if (!rs.getBoolean(1)) {
          candidate.close();
          return;
        }
      } catch (Exception e) {
        candidate.close();
        throw e;
      }
      connection = candidate;
      epoch =
          store.jdbc.queryForObject(
              "UPDATE factory_state SET epoch=epoch+1 WHERE id=1 RETURNING epoch", Long.class);
      log.info("Acquired coordinator epoch {}", epoch);
    } catch (Exception e) {
      close();
      log.warn("Coordinator unavailable: {}", e.getClass().getSimpleName());
    }
  }

  @PreDestroy
  /**
   * Drops the local epoch before releasing the advisory lock and closing its dedicated connection.
   */
  public synchronized void close() {
    epoch = 0;
    if (connection == null) return;
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("SELECT pg_advisory_unlock(" + LOCK + ")");
    } catch (Exception ignored) {
      try {
        connection.abort(Runnable::run);
      } catch (Exception ignoredAgain) {
      }
    } finally {
      try {
        connection.close();
      } catch (Exception ignored) {
      }
      connection = null;
    }
  }
}
