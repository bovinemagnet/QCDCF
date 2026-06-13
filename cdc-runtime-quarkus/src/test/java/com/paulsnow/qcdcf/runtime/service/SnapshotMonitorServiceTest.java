package com.paulsnow.qcdcf.runtime.service;

import com.paulsnow.qcdcf.runtime.service.SnapshotMonitorService.SnapshotJobRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SnapshotMonitorService} snapshot history tracking.
 *
 * @author Paul Snow
 * @since 0.0.0
 */
class SnapshotMonitorServiceTest {

    private SnapshotMonitorService monitor;

    @BeforeEach
    void setUp() {
        monitor = new SnapshotMonitorService();
    }

    @Test
    void recordSnapshotStartedAddsRunningRecord() {
        monitor.recordSnapshotStarted("public.customer");

        SnapshotJobRecord active = monitor.activeSnapshot();
        assertThat(active).isNotNull();
        assertThat(active.tableName()).isEqualTo("public.customer");
        assertThat(active.status()).isEqualTo("RUNNING");
    }

    @Test
    void recordSnapshotCompletedReplacesRunningRecord() {
        monitor.recordSnapshotStarted("public.customer");
        monitor.recordSnapshotCompleted("public.customer", 42);

        assertThat(monitor.activeSnapshot()).isNull();
        List<SnapshotJobRecord> history = monitor.snapshotHistory();
        assertThat(history).hasSize(1);
        assertThat(history.getFirst().status()).isEqualTo("COMPLETE");
        assertThat(history.getFirst().rowsRead()).isEqualTo(42);
    }

    @Test
    void recordSnapshotFailedReplacesRunningRecord() {
        monitor.recordSnapshotStarted("public.customer");
        monitor.recordSnapshotFailed("public.customer", "boom");

        assertThat(monitor.activeSnapshot()).isNull();
        List<SnapshotJobRecord> history = monitor.snapshotHistory();
        assertThat(history).hasSize(1);
        assertThat(history.getFirst().status()).isEqualTo("FAILED");
    }

    @Test
    void completionForOneTableDoesNotTouchAnotherTablesRecord() {
        monitor.recordSnapshotStarted("public.orders");
        monitor.recordSnapshotStarted("public.customer");
        monitor.recordSnapshotCompleted("public.customer", 10);

        SnapshotJobRecord active = monitor.activeSnapshot();
        assertThat(active).isNotNull();
        assertThat(active.tableName()).isEqualTo("public.orders");
    }

    @Test
    void historyIsTrimmedToMaximumSize() {
        for (int i = 0; i < 30; i++) {
            monitor.recordSnapshotStarted("public.t" + i);
        }
        assertThat(monitor.snapshotHistory()).hasSize(20);
    }

    @Test
    void concurrentRecordingNeverLosesRecordsOrExposesEmptyHistory() throws Exception {
        int tables = 8;
        int rounds = 200;
        ExecutorService pool = Executors.newFixedThreadPool(tables + 1);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(tables);

        // Seed one persistent record so a reader should never see an empty history
        monitor.recordSnapshotStarted("public.seed");
        monitor.recordSnapshotCompleted("public.seed", 1);

        var emptyObserved = new java.util.concurrent.atomic.AtomicBoolean(false);
        var reading = new java.util.concurrent.atomic.AtomicBoolean(true);
        pool.submit(() -> {
            while (reading.get()) {
                if (monitor.snapshotHistory().isEmpty()) {
                    emptyObserved.set(true);
                }
            }
        });

        for (int t = 0; t < tables; t++) {
            String table = "public.t" + t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int r = 0; r < rounds; r++) {
                        monitor.recordSnapshotStarted(table);
                        monitor.recordSnapshotChunkCompleted(table, r, r * 10L);
                        monitor.recordSnapshotCompleted(table, r * 10L);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        reading.set(false);
        pool.shutdown();
        assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        // After all completions, no record may be stuck in RUNNING (a lost
        // started record causes the completion to fall back to a synthetic
        // entry, leaving the original RUNNING record orphaned).
        assertThat(monitor.activeSnapshot()).isNull();
        assertThat(emptyObserved).isFalse();
        assertThat(monitor.snapshotHistory()).hasSize(20);
    }
}
