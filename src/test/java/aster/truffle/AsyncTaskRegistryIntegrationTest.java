package aster.truffle;

import aster.truffle.runtime.AsyncTaskRegistry;
import aster.truffle.runtime.AsyncTaskRegistry.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AsyncTaskRegistry 集成测试：覆盖超时/补偿/优先级/环境变量等调度场景。
 */
public class AsyncTaskRegistryIntegrationTest {

  private Map<String, String> originalEnv;
  private boolean envModified;

  @BeforeEach
  public void captureEnv() throws Exception {
    originalEnv = EnvTestSupport.snapshotEnv();
    envModified = false;
  }

  @AfterEach
  public void restoreEnv() throws Exception {
    if (envModified && originalEnv != null) {
      EnvTestSupport.restoreEnv(originalEnv);
    }
  }

  @Test
  public void testTimeoutWithCompensation() throws Exception {
    AsyncTaskRegistry registry = new AsyncTaskRegistry(2);
    List<String> compensationOrder = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch fastCompleted = new CountDownLatch(1);

    try {
      // Set workflowId - required for compensation to work
      registry.setWorkflowId("test-workflow");

      registry.registerTaskWithDependencies(
          "fast",
          () -> {
            fastCompleted.countDown();
            return "fast";
          },
          Set.of(),
          0L,
          () -> compensationOrder.add("fast"),
          0);

      registry.registerTaskWithDependencies(
          "timeout",
          () -> {
            fastCompleted.await(100, TimeUnit.MILLISECONDS);
            Thread.sleep(80);
            return "slow";
          },
          Set.of(),
          20L,
          () -> compensationOrder.add("timeout"),
          1);

      RuntimeException ex = assertThrows(RuntimeException.class, registry::executeUntilComplete);
      Throwable timeout = registry.getException("timeout");
      assertTrue(timeout instanceof TimeoutException);
      assertEquals(TaskStatus.COMPLETED, registry.getStatus("fast"));
      assertEquals(TaskStatus.FAILED, registry.getStatus("timeout"));
      assertEquals(List.of("fast"), compensationOrder);
    } finally {
      registry.shutdown();
    }
  }

  @Test
  public void testPriorityWithDependencies() {
    AsyncTaskRegistry registry = new AsyncTaskRegistry(1);
    List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());

    try {
      registry.registerTaskWithDependencies(
          "parent-fast",
          () -> {
            executionOrder.add("parent-fast");
            return null;
          },
          Collections.emptySet(),
          0L,
          null,
          1);

      registry.registerTaskWithDependencies(
          "parent-slow",
          () -> {
            executionOrder.add("parent-slow");
            return null;
          },
          Collections.emptySet(),
          0L,
          null,
          5);

      registry.registerTaskWithDependencies(
          "child",
          () -> {
            executionOrder.add("child");
            return null;
          },
          Set.of("parent-fast", "parent-slow"),
          0L,
          null,
          0);

      registry.executeUntilComplete();
      assertEquals(List.of("parent-fast", "parent-slow", "child"), executionOrder);
    } finally {
      registry.shutdown();
    }
  }

  @Test
  public void testTimeoutPropagation() {
    AsyncTaskRegistry registry = new AsyncTaskRegistry(2);
    AtomicBoolean downstreamExecuted = new AtomicBoolean(false);

    try {
      registry.registerTaskWithDependencies(
          "root",
          () -> null,
          Collections.emptySet(),
          0L,
          null,
          1);

      registry.registerTaskWithDependencies(
          "slow-mid",
          () -> {
            Thread.sleep(80);
            return null;
          },
          Set.of("root"),
          25L,
          null,
          1);

      registry.registerTaskWithDependencies(
          "leaf",
          () -> {
            downstreamExecuted.set(true);
            return null;
          },
          Set.of("slow-mid"),
          0L,
          null,
          1);

      assertThrows(RuntimeException.class, registry::executeUntilComplete);
      assertTrue(registry.getException("slow-mid") instanceof TimeoutException);
      assertEquals(TaskStatus.FAILED, registry.getStatus("slow-mid"));
      assertTrue(registry.isCancelled("leaf"));
      assertFalse(downstreamExecuted.get());
    } finally {
      registry.shutdown();
    }
  }

  @Test
  public void testCompensationOrder() {
    AsyncTaskRegistry registry = new AsyncTaskRegistry(1);
    List<String> completionOrder = Collections.synchronizedList(new ArrayList<>());
    List<String> compensationOrder = Collections.synchronizedList(new ArrayList<>());

    try {
      // Set workflowId - required for compensation to work
      registry.setWorkflowId("test-workflow");

      registerCompensatedTask(registry, "start", Collections.emptySet(), 5, completionOrder, compensationOrder);
      registerCompensatedTask(registry, "fan-left", Set.of("start"), 1, completionOrder, compensationOrder);
      registerCompensatedTask(registry, "fan-right", Set.of("start"), 2, completionOrder, compensationOrder);
      registerCompensatedTask(registry, "diamond", Set.of("fan-left", "fan-right"), 0, completionOrder, compensationOrder);
      registerCompensatedTask(registry, "diamond-left", Set.of("diamond"), 1, completionOrder, compensationOrder);
      registerCompensatedTask(registry, "diamond-right", Set.of("diamond"), 2, completionOrder, compensationOrder);

      registry.registerTaskWithDependencies(
          "terminal",
          () -> {
            throw new IllegalStateException("terminal failure");
          },
          Set.of("diamond-left", "diamond-right"),
          0L,
          null,
          0);

      RuntimeException ex = assertThrows(RuntimeException.class, registry::executeUntilComplete);
      assertTrue(ex instanceof IllegalStateException);

      List<String> expectedCompletion = List.of(
          "start",
          "fan-left",
          "fan-right",
          "diamond",
          "diamond-left",
          "diamond-right"
      );
      assertEquals(expectedCompletion, completionOrder);

      List<String> expectedCompensation = List.of(
          "diamond-right",
          "diamond-left",
          "diamond",
          "fan-right",
          "fan-left",
          "start"
      );
      assertEquals(expectedCompensation, compensationOrder);
    } finally {
      registry.shutdown();
    }
  }

  @Test
  public void testFailurePropagation() {
    AsyncTaskRegistry registry = new AsyncTaskRegistry(2);
    AtomicInteger childExecutionCount = new AtomicInteger(0);

    try {
      registry.registerTaskWithDependencies(
          "start",
          () -> null,
          Collections.emptySet(),
          0L,
          null,
          1);

      registry.registerTaskWithDependencies(
          "critical",
          () -> {
            throw new IllegalArgumentException("boom");
          },
          Set.of("start"),
          0L,
          null,
          1);

      registry.registerTaskWithDependencies(
          "child-a",
          () -> {
            childExecutionCount.incrementAndGet();
            return null;
          },
          Set.of("critical"),
          0L,
          null,
          1);

      registry.registerTaskWithDependencies(
          "child-b",
          () -> {
            childExecutionCount.incrementAndGet();
            return null;
          },
          Set.of("child-a"),
          0L,
          null,
          1);

      RuntimeException ex = assertThrows(RuntimeException.class, registry::executeUntilComplete);
      assertTrue(ex instanceof IllegalArgumentException);
      assertEquals(TaskStatus.FAILED, registry.getStatus("critical"));
      assertEquals(0, childExecutionCount.get(), "下游任务不应执行");
    } finally {
      registry.shutdown();
    }
  }

  @Test
  public void testEnvironmentVariableConfiguration() throws Exception {
    EnvTestSupport.setEnvVar("ASTER_THREAD_POOL_SIZE", "2");
    EnvTestSupport.setEnvVar("ASTER_DEFAULT_TIMEOUT_MS", "30");
    envModified = true;

    AsyncTaskRegistry registry = new AsyncTaskRegistry();
    try {
      ThreadPoolExecutor executor = extractExecutor(registry);
      assertEquals(2, executor.getCorePoolSize());

      Field timeoutField = AsyncTaskRegistry.class.getDeclaredField("defaultTimeoutMs");
      timeoutField.setAccessible(true);
      long defaultTimeout = timeoutField.getLong(registry);
      assertEquals(30L, defaultTimeout);

      registry.registerTaskWithDependencies(
          "root",
          () -> null,
          Collections.emptySet(),
          0L,
          null,
          1);

      registry.registerTaskWithDependencies(
          "env-timeout",
          () -> {
            Thread.sleep(80);
            return null;
          },
          Set.of("root"),
          0L,
          null,
          1);

      assertThrows(RuntimeException.class, registry::executeUntilComplete);
      Throwable timeout = registry.getException("env-timeout");
      assertTrue(timeout instanceof TimeoutException);
      assertEquals("任务超时: env-timeout", timeout.getMessage());
    } finally {
      registry.shutdown();
    }
  }

  private static void registerCompensatedTask(
      AsyncTaskRegistry registry,
      String taskId,
      Set<String> dependencies,
      int priority,
      List<String> completionOrder,
      List<String> compensationOrder) {
    registry.registerTaskWithDependencies(
        taskId,
        () -> {
          completionOrder.add(taskId);
          return taskId;
        },
        dependencies,
        0L,
        () -> compensationOrder.add(taskId),
        priority);
  }

  private static ThreadPoolExecutor extractExecutor(AsyncTaskRegistry registry) throws Exception {
    Field executorField = AsyncTaskRegistry.class.getDeclaredField("executor");
    executorField.setAccessible(true);
    return (ThreadPoolExecutor) executorField.get(registry);
  }
}
