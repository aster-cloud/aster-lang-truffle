package aster.truffle.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import aster.core.exceptions.MaxRetriesExceededException;
import io.aster.workflow.DeterminismContext;
import aster.runtime.workflow.WorkflowEvent;
import aster.runtime.workflow.WorkflowSnapshot;
import aster.runtime.workflow.WorkflowState;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * AsyncTaskRegistry 重试执行集成测试
 */
public class RetryExecutionTest {

  private AsyncTaskRegistry registry;
  private RecordingEventStore eventStore;

  @BeforeEach
  public void setUp() {
    registry = new AsyncTaskRegistry();
    eventStore = new RecordingEventStore();
    registry.setWorkflowId("wf-retry-test");
    registry.setEventStore(eventStore);
    registry.startPolling();
  }

  @AfterEach
  public void tearDown() {
    registry.stopPolling();
    registry.shutdown();
  }

  @Test
  public void testSuccessfulRetry() {
    AtomicInteger attempts = new AtomicInteger();
    AsyncTaskRegistry.RetryPolicy policy = new AsyncTaskRegistry.RetryPolicy(3, "linear", 0L);

    registry.registerTaskWithRetry("task-success", () -> {
      if (attempts.incrementAndGet() == 1) {
        throw new RuntimeException("boom");
      }
      registry.setResult("task-success", "ok");
      return "ok";
    }, Collections.emptySet(), policy);

    registry.executeUntilComplete();

    assertEquals("ok", registry.getResult("task-success"));
    assertEquals(2, attempts.get(), "第二次尝试应成功");
    assertEquals(1, eventStore.events.size(), "应记录一次重试事件");
    RecordingEvent storeEvent = eventStore.events.get(0);
    assertEquals(2, storeEvent.attemptNumber);
    assertEquals("boom", storeEvent.failureReason);
  }

  @Test
  public void testMaxRetriesExceeded() {
    AsyncTaskRegistry.RetryPolicy policy = new AsyncTaskRegistry.RetryPolicy(2, "linear", 0L);

    registry.registerTaskWithRetry("task-fail", () -> {
      throw new RuntimeException("always fail");
    }, Collections.emptySet(), policy);

    assertThrows(MaxRetriesExceededException.class, () -> registry.executeUntilComplete());
    assertTrue(registry.isFailed("task-fail"));
  }

  @Test
  public void testBackoffDelay() throws Exception {
    // 更新为使用新的 DeterminismContext 签名
    Method method = AsyncTaskRegistry.class.getDeclaredMethod("calculateBackoff",
        int.class, String.class, long.class, DeterminismContext.class);
    method.setAccessible(true);

    DeterminismContext ctx = new DeterminismContext();
    long exp = (long) method.invoke(registry, 3, "exponential", 50L, ctx);
    long expBase = 50L * (1L << (3 - 1));
    assertTrue(exp >= expBase && exp < expBase + 25, "指数退避应在预期范围内");

    ctx = new DeterminismContext();
    long linear = (long) method.invoke(registry, 2, "linear", 100L, ctx);
    long linearBase = 100L * 2;
    assertTrue(linear >= linearBase && linear < linearBase + 50, "线性退避应在预期范围内");
  }

  @Test
  public void testRetryMetadataLogging() {
    AtomicInteger attempts = new AtomicInteger();
    AsyncTaskRegistry.RetryPolicy policy = new AsyncTaskRegistry.RetryPolicy(3, "exponential", 100L);

    registry.registerTaskWithRetry("task-metadata", () -> {
      if (attempts.incrementAndGet() < 3) {
        throw new RuntimeException("transient failure");
      }
      registry.setResult("task-metadata", "success");
      return "success";
    }, Collections.emptySet(), policy);

    registry.executeUntilComplete();

    // 验证事件存储记录了正确的 retry metadata
    assertEquals(2, eventStore.events.size(), "应记录2次重试事件（第1次和第2次失败）");

    RecordingEvent event1 = eventStore.events.get(0);
    assertEquals(2, event1.attemptNumber, "第1次重试的 attemptNumber 应为 2");
    assertEquals("transient failure", event1.failureReason);
    assertTrue(event1.backoffDelayMs >= 100L, "第1次重试的 backoff 应 >=100ms");

    RecordingEvent event2 = eventStore.events.get(1);
    assertEquals(3, event2.attemptNumber, "第2次重试的 attemptNumber 应为 3");
    assertEquals("transient failure", event2.failureReason);
    assertTrue(event2.backoffDelayMs >= 200L, "第2次重试的 backoff 应 >=200ms（指数增长）");
  }

  @Test
  public void testMaxRetriesExceptionFields() {
    AsyncTaskRegistry.RetryPolicy policy = new AsyncTaskRegistry.RetryPolicy(3, "linear", 50L);

    registry.registerTaskWithRetry("task-exception", () -> {
      throw new RuntimeException("persistent error");
    }, Collections.emptySet(), policy);

    MaxRetriesExceededException exception = assertThrows(
        MaxRetriesExceededException.class,
        () -> registry.executeUntilComplete()
    );

    // 验证异常字段
    assertEquals(3, exception.getMaxAttempts(), "maxAttempts 应为 3");
    assertEquals("persistent error", exception.getFailureReason(), "failureReason 应为原始异常消息");
    assertTrue(exception.getMessage().contains("Max retries (3) exceeded"));
    assertTrue(exception.getMessage().contains("persistent error"));
  }

  private static final class RecordingEventStore implements PostgresEventStore {
    private final CopyOnWriteArrayList<RecordingEvent> events = new CopyOnWriteArrayList<>();
    private final AtomicLong sequence = new AtomicLong();

    @Override
    public long appendEvent(String workflowId, String eventType, Object payload) {
      return appendEvent(workflowId, eventType, payload, 1, null, null);
    }

    @Override
    public long appendEvent(String workflowId, String eventType, Object payload,
        Integer attemptNumber, Long backoffDelayMs, String failureReason) {
      events.add(new RecordingEvent(workflowId, eventType, attemptNumber, backoffDelayMs, failureReason));
      return sequence.incrementAndGet();
    }

    @Override
    public List<WorkflowEvent> getEvents(String workflowId, long fromSeq) {
      return Collections.emptyList();
    }

    @Override
    public Optional<WorkflowState> getState(String workflowId) {
      return Optional.empty();
    }

    @Override
    public void saveSnapshot(String workflowId, long eventSeq, Object state) {
      // no-op
    }

    @Override
    public Optional<WorkflowSnapshot> getLatestSnapshot(String workflowId) {
      return Optional.empty();
    }
  }

  private record RecordingEvent(String workflowId, String eventType,
                                Integer attemptNumber, Long backoffDelayMs, String failureReason) {
  }
}
