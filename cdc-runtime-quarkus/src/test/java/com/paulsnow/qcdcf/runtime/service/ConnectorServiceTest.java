package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.model.TableId;
import com.paulsnow.qcdcf.runtime.bootstrap.ConnectorBootstrap;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ConnectorService} snapshot state transitions.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
class ConnectorServiceTest {

    private ConnectorService service;
    private ManagedExecutor executor;
    private DataSource dataSource;

    @BeforeEach
    void setUp() throws SQLException {
        service = new ConnectorService();
        service.bootstrap = mock(ConnectorBootstrap.class);
        when(service.bootstrap.connectorId()).thenReturn("test-connector");
        executor = mock(ManagedExecutor.class);
        dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenThrow(new SQLException("no database in unit test"));
        service.executor = executor;
        service.dataSource = dataSource;
        service.snapshotMonitor = new SnapshotMonitorService();
    }

    @Test
    void triggerSnapshotStartsWhenIdle() {
        Map<String, Object> result = service.triggerSnapshot("public.customer");

        assertThat(result.get("message")).isEqualTo("Snapshot started");
        assertThat(service.isSnapshotRunning()).isTrue();
        verify(executor).submit(any(Runnable.class));
    }

    @Test
    void triggerSnapshotRejectedWhileAnotherIsRunning() {
        service.triggerSnapshot("public.customer");
        Map<String, Object> second = service.triggerSnapshot("public.orders");

        assertThat(second.get("message")).isEqualTo("Snapshot already running; request ignored");
        assertThat(service.lastSnapshotTable()).isEqualTo("public.customer");
        verify(executor, times(1)).submit(any(Runnable.class));
    }

    @Test
    void runSnapshotBlockingRejectedWhileAnotherIsRunning() {
        service.triggerSnapshot("public.customer");

        boolean started = service.runSnapshotBlocking(new TableId("public", "orders"));

        assertThat(started).isFalse();
        assertThat(service.lastSnapshotTable()).isEqualTo("public.customer");
    }

    @Test
    void runSnapshotBlockingRecordsFailureAgainstItsOwnTable() {
        boolean succeeded = service.runSnapshotBlocking(new TableId("public", "customer"));

        assertThat(succeeded).isFalse();
        assertThat(service.isSnapshotRunning()).isFalse();
        assertThat(service.lastSnapshotTable()).isEqualTo("public.customer");
        assertThat(service.lastSnapshotStatus()).startsWith("FAILED");
        verify(executor, never()).submit(any(Runnable.class));
    }

    @Test
    void newSnapshotCanStartAfterPreviousFails() {
        service.runSnapshotBlocking(new TableId("public", "customer"));

        Map<String, Object> result = service.triggerSnapshot("public.orders");

        assertThat(result.get("message")).isEqualTo("Snapshot started");
    }
}
