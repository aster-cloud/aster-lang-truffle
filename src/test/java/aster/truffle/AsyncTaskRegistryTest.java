package aster.truffle;

import aster.truffle.runtime.AsyncTaskRegistry;
import aster.truffle.runtime.AsyncTaskRegistry.TaskStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static aster.truffle.EnvTestSupport.restoreEnv;
import static aster.truffle.EnvTestSupport.setEnvVar;
import static aster.truffle.EnvTestSupport.snapshotEnv;

/**
 * 单元测试：AsyncTaskRegistry 基础功能
 *
 * 验证 Phase 1 单线程调度模型的核心功能：
 * 1. 任务注册
 * 2. 状态查询
 * 3. 结果存储
 * 4. executeNext() 调度
 * 5. 异常捕获
 */
public class AsyncTaskRegistryTest {

  private AsyncTaskRegistry registry;
  private AtomicLong taskIdGenerator;

  @BeforeEach
  public void setup() {
    // Create registry directly without AsterContext dependency
    registry = new AsyncTaskRegistry();
    taskIdGenerator = new AtomicLong(0);
  }

  /**
   * Generate task ID similar to AsterContext.generateTaskId()
   */
  private String generateTaskId() {
    return "task-" + taskIdGenerator.incrementAndGet();
  }

  /**
   * 测试1：任务注册与状态查询
   */
  @Test
  public void testRegisterTask() {
    String taskId = generateTaskId();
    registry.registerTask(taskId, () -> {
      // Empty task
    });

    // 验证任务已注册且状态为 PENDING
    assertEquals(TaskStatus.PENDING, registry.getStatus(taskId));
    assertEquals(1, registry.getTaskCount());
    assertEquals(1, registry.getPendingCount());
  }

  /**
   * 测试2：executeNext() 成功执行任务
   */
  @Test
  public void testExecuteNextSuccess() {
    AtomicInteger counter = new AtomicInteger(0);
    String taskId = generateTaskId();

    registry.registerTask(taskId, () -> {
      counter.incrementAndGet();
      registry.setResult(taskId, 42);
    });

    // 执行任务
    registry.executeNext();

    // 验证任务已完成
    assertEquals(TaskStatus.COMPLETED, registry.getStatus(taskId));
    assertEquals(1, counter.get());
    assertEquals(42, registry.getResult(taskId));
  }

  /**
   * 测试3：executeNext() 捕获任务异常
   */
  @Test
  public void testExecuteNextException() {
    String taskId = generateTaskId();
    RuntimeException expectedException = new RuntimeException("Test exception");

    registry.registerTask(taskId, () -> {
      throw expectedException;
    });

    // 执行任务
    registry.executeNext();

    // 验证任务状态为 FAILED
    assertEquals(TaskStatus.FAILED, registry.getStatus(taskId));
    assertEquals(expectedException, registry.getException(taskId));
  }

  /**
   * 测试4：getResult() 对失败任务抛出异常
   */
  @Test
  public void testGetResultThrowsOnFailedTask() {
    String taskId = generateTaskId();

    registry.registerTask(taskId, () -> {
      throw new RuntimeException("Test exception");
    });

    registry.executeNext();

    // 验证 getResult() 抛出异常
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
      registry.getResult(taskId);
    });

    assertTrue(thrown.getMessage().contains("Task failed"));
  }

  /**
   * 测试5：多任务 FIFO 调度
   */
  @Test
  public void testMultipleTasksFIFO() {
    AtomicInteger counter = new AtomicInteger(0);

    String task1 = generateTaskId();
    String task2 = generateTaskId();
    String task3 = generateTaskId();

    registry.registerTask(task1, () -> {
      counter.set(1);
      registry.setResult(task1, 1);
    });

    registry.registerTask(task2, () -> {
      counter.set(2);
      registry.setResult(task2, 2);
    });

    registry.registerTask(task3, () -> {
      counter.set(3);
      registry.setResult(task3, 3);
    });

    // 执行第一个任务
    registry.executeNext();
    assertEquals(1, counter.get());
    assertEquals(TaskStatus.COMPLETED, registry.getStatus(task1));

    // 执行第二个任务
    registry.executeNext();
    assertEquals(2, counter.get());
    assertEquals(TaskStatus.COMPLETED, registry.getStatus(task2));

    // 执行第三个任务
    registry.executeNext();
    assertEquals(3, counter.get());
    assertEquals(TaskStatus.COMPLETED, registry.getStatus(task3));
  }

  /**
   * 测试6：isCompleted() 和 isFailed()
   */
  @Test
  public void testStatusHelpers() {
    String task1 = generateTaskId();
    String task2 = generateTaskId();

    registry.registerTask(task1, () -> {
      registry.setResult(task1, "success");
    });

    registry.registerTask(task2, () -> {
      throw new RuntimeException("fail");
    });

    // 初始状态
    assertFalse(registry.isCompleted(task1));
    assertFalse(registry.isFailed(task1));

    // 执行成功任务
    registry.executeNext();
    assertTrue(registry.isCompleted(task1));
    assertFalse(registry.isFailed(task1));

    // 执行失败任务
    registry.executeNext();
    assertFalse(registry.isCompleted(task2));
    assertTrue(registry.isFailed(task2));
  }

  /**
   * 测试7：任务 GC 清理
   */
  @Test
  public void testGarbageCollection() throws InterruptedException {
    String taskId = generateTaskId();

    registry.registerTask(taskId, () -> {
      registry.setResult(taskId, 42);
    });

    registry.executeNext();
    assertEquals(1, registry.getTaskCount());

    // GC 不应立即移除刚完成的任务
    registry.gc();
    assertEquals(1, registry.getTaskCount());

    // 等待 TTL 过期（设计文档中 TTL 为 60 秒，但测试中可以接受任务仍在）
    // 注意：这个测试仅验证 gc() 不会崩溃，实际 TTL 测试需要更长时间
  }

  /**
   * 测试8：不存在的任务
   */
  @Test
  public void testNonExistentTask() {
    String nonExistentId = "task-999999";

    assertNull(registry.getStatus(nonExistentId));
    assertFalse(registry.isCompleted(nonExistentId));
    assertFalse(registry.isFailed(nonExistentId));

    RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
      registry.getResult(nonExistentId);
    });

    assertTrue(thrown.getMessage().contains("Task not found"));
  }

  @Test
  public void testTaskTimeoutCancelsDependents() {
    AtomicInteger downstreamCounter = new AtomicInteger(0);

    registry.registerTaskWithDependencies("slow", () -> {
      try {
        Thread.sleep(150);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      registry.setResult("slow", "late");
      return null;
    }, Collections.emptySet(), 20L);

    registry.registerTaskWithDependencies("downstream", () -> {
      downstreamCounter.incrementAndGet();
      registry.setResult("downstream", "should-not-run");
      return null;
    }, Set.of("slow"));

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> registry.executeUntilComplete());
    assertTrue(thrown.getCause() instanceof TimeoutException);
    assertTrue(thrown.getCause().getMessage().contains("slow"));
    assertTrue(registry.isFailed("slow"));
    assertTrue(registry.isCancelled("downstream"));
    assertEquals(0, downstreamCounter.get(), "下游任务应在超时后被取消");
  }

  @Test
  public void testTimeoutDisabledWhenZero() {
    AtomicInteger counter = new AtomicInteger(0);

    registry.registerTaskWithDependencies("no-timeout", () -> {
      try {
        Thread.sleep(30);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      counter.incrementAndGet();
      registry.setResult("no-timeout", "done");
      return null;
    }, Collections.emptySet(), 0L);

    assertDoesNotThrow(() -> registry.executeUntilComplete());
    assertEquals(1, counter.get());
    assertTrue(registry.isCompleted("no-timeout"));
  }

  @Test
  public void testCompensationLifoOrder() {
    List<String> compensationOrder = Collections.synchronizedList(new ArrayList<>());

    // Set workflowId - required for compensation to work
    registry.setWorkflowId("test-workflow");

    registry.registerTaskWithDependencies(
        "taskA",
        () -> {
          registry.setResult("taskA", "A");
          return null;
        },
        Collections.emptySet(),
        0L,
        () -> compensationOrder.add("A"));

    registry.registerTaskWithDependencies(
        "taskB",
        () -> {
          registry.setResult("taskB", "B");
          return null;
        },
        Set.of("taskA"),
        0L,
        () -> compensationOrder.add("B"));

    registry.registerTaskWithDependencies(
        "taskC",
        () -> {
          registry.setResult("taskC", "C");
          return null;
        },
        Set.of("taskB"),
        0L,
        () -> compensationOrder.add("C"));

    registry.registerTaskWithDependencies(
        "taskFail",
        () -> {
          throw new RuntimeException("boom");
        },
        Set.of("taskC"));

    assertThrows(RuntimeException.class, () -> registry.executeUntilComplete());
    assertEquals(List.of("C", "B", "A"), compensationOrder);
  }

  @Test
  public void testCompensationFailureIsolation() {
    List<String> compensationOrder = Collections.synchronizedList(new ArrayList<>());

    // Set workflowId - required for compensation to work
    registry.setWorkflowId("test-workflow");

    registry.registerTaskWithDependencies(
        "task1",
        () -> {
          registry.setResult("task1", "ok1");
          return null;
        },
        Collections.emptySet(),
        0L,
        () -> compensationOrder.add("task1"));

    registry.registerTaskWithDependencies(
        "task2",
        () -> {
          registry.setResult("task2", "ok2");
          return null;
        },
        Set.of("task1"),
        0L,
        () -> {
          compensationOrder.add("task2");
          throw new RuntimeException("compensation boom");
        });

    registry.registerTaskWithDependencies(
        "taskFail",
        () -> {
          throw new RuntimeException("fail");
        },
        Set.of("task2"));

    assertThrows(RuntimeException.class, () -> registry.executeUntilComplete());
    assertEquals(List.of("task2", "task1"), compensationOrder);
  }

  @Test
  public void testTasksWithoutCompensationDoNotAffectStack() {
    List<String> compensationOrder = Collections.synchronizedList(new ArrayList<>());

    registry.registerTaskWithDependencies(
        "taskX",
        () -> {
          registry.setResult("taskX", "x");
          return null;
        },
        Collections.emptySet(),
        0L,
        null);

    registry.registerTaskWithDependencies(
        "taskY",
        () -> {
          registry.setResult("taskY", "y");
          return null;
        },
        Set.of("taskX"),
        0L,
        null);

    registry.registerTaskWithDependencies(
        "taskFail",
        () -> {
          throw new RuntimeException("fail");
        },
        Set.of("taskY"),
        0L,
        () -> compensationOrder.add("fail-comp")); // 仅失败任务有补偿

    assertThrows(RuntimeException.class, () -> registry.executeUntilComplete());
    // 传统 Saga 模式：仅成功任务执行补偿（用于回滚），失败任务不执行补偿
    // 如需清理失败任务的资源，应在任务内部使用 try-finally
    assertEquals(Collections.emptyList(), compensationOrder);
  }

  @Test
  public void testHigherPriorityRunsFirstWhenReady() {
    AsyncTaskRegistry singleThreadRegistry = new AsyncTaskRegistry(1);
    List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());

    singleThreadRegistry.registerTaskWithDependencies(
        "low",
        () -> {
          executionOrder.add("low");
          singleThreadRegistry.setResult("low", "l");
          return null;
        },
        Collections.emptySet(),
        0L,
        null,
        5);

    singleThreadRegistry.registerTaskWithDependencies(
        "high",
        () -> {
          executionOrder.add("high");
          singleThreadRegistry.setResult("high", "h");
          return null;
        },
        Collections.emptySet(),
        0L,
        null,
        0);

    singleThreadRegistry.executeUntilComplete();
    assertEquals(List.of("high", "low"), executionOrder);
  }

  @Test
  public void testDependencyRespectedOverPriority() {
    AsyncTaskRegistry singleThreadRegistry = new AsyncTaskRegistry(1);
    List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());

    singleThreadRegistry.registerTaskWithDependencies(
        "root",
        () -> {
          executionOrder.add("root");
          singleThreadRegistry.setResult("root", "r");
          return null;
        },
        Collections.emptySet(),
        0L,
        null,
        5);

    singleThreadRegistry.registerTaskWithDependencies(
        "child",
        () -> {
          executionOrder.add("child");
          singleThreadRegistry.setResult("child", "c");
          return null;
        },
        Set.of("root"),
        0L,
        null,
        0);

    singleThreadRegistry.executeUntilComplete();
    assertEquals(List.of("root", "child"), executionOrder);
  }

  @Test
  public void testLoadThreadPoolSizeFromEnv() throws Exception {
    Map<String, String> originalEnv = snapshotEnv();
    try {
      setEnvVar("ASTER_THREAD_POOL_SIZE", "3");
      int size = invokeLoadThreadPoolSize();
      assertEquals(3, size);
    } finally {
      restoreEnv(originalEnv);
    }
  }

  @Test
  public void testLoadThreadPoolSizeInvalidLogsWarning() throws Exception {
    Map<String, String> originalEnv = snapshotEnv();
    PrintStream originalErr = System.err;
    ByteArrayOutputStream errCapture = new ByteArrayOutputStream();
    System.setErr(new PrintStream(errCapture, true, StandardCharsets.UTF_8));
    try {
      setEnvVar("ASTER_THREAD_POOL_SIZE", "0");
      int size = invokeLoadThreadPoolSize();
      assertEquals(Runtime.getRuntime().availableProcessors(), size);
      assertTrue(errCapture.toString(StandardCharsets.UTF_8).contains("ASTER_THREAD_POOL_SIZE 必须 > 0"));
    } finally {
      System.setErr(originalErr);
      restoreEnv(originalEnv);
    }
  }

  @Test
  public void testLoadDefaultTimeoutFromEnv() throws Exception {
    Map<String, String> originalEnv = snapshotEnv();
    try {
      setEnvVar("ASTER_DEFAULT_TIMEOUT_MS", "1234");
      long timeout = invokeLoadDefaultTimeout();
      assertEquals(1234L, timeout);
    } finally {
      restoreEnv(originalEnv);
    }
  }

  @Test
  public void testLoadDefaultTimeoutInvalidLogsWarning() throws Exception {
    Map<String, String> originalEnv = snapshotEnv();
    PrintStream originalErr = System.err;
    ByteArrayOutputStream errCapture = new ByteArrayOutputStream();
    System.setErr(new PrintStream(errCapture, true, StandardCharsets.UTF_8));
    try {
      setEnvVar("ASTER_DEFAULT_TIMEOUT_MS", "-5");
      long timeout = invokeLoadDefaultTimeout();
      assertEquals(0L, timeout);
      assertTrue(errCapture.toString(StandardCharsets.UTF_8).contains("ASTER_DEFAULT_TIMEOUT_MS 必须 >= 0"));
    } finally {
      System.setErr(originalErr);
      restoreEnv(originalEnv);
    }
  }

  /**
   * 测试增强的死锁诊断功能
   *
   * 构造死锁场景：注册任务但不注册其依赖，导致依赖永远无法满足
   * 验证错误消息包含：
   * 1. 死锁检测提示
   * 2. 待处理任务及其依赖列表（带优先级）
   *
   * 注意：这个测试验证的是死锁诊断的信息输出，而非循环依赖检测
   * （循环依赖在注册时就会被 DependencyGraph 拒绝）
   */
  @Test
  public void testDeadlockDiagnostics() {
    // 注册几个任务，它们依赖一个未注册的任务 "ghost"
    // 这会导致死锁：readyTasks 为空，但 remainingTasks > 0

    registry.registerTaskWithDependencies(
        "taskA",
        () -> {
          registry.setResult("taskA", "A");
          return null;
        },
        Set.of("ghost"),  // 依赖不存在的任务
        0L,
        null,
        1  // 优先级 1
    );

    registry.registerTaskWithDependencies(
        "taskB",
        () -> {
          registry.setResult("taskB", "B");
          return null;
        },
        Set.of("ghost"),  // 依赖不存在的任务
        0L,
        null,
        2  // 优先级 2
    );

    registry.registerTaskWithDependencies(
        "taskC",
        () -> {
          registry.setResult("taskC", "C");
          return null;
        },
        Set.of("taskA"),  // taskC 依赖 taskA（也无法执行）
        0L,
        null,
        3  // 优先级 3
    );

    // 验证抛出 IllegalStateException（死锁检测）
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> registry.executeUntilComplete());

    // 验证错误消息包含增强的诊断信息
    String message = exception.getMessage();
    assertNotNull(message, "错误消息不应为 null");

    // 验证包含关键诊断信息
    assertTrue(message.contains("死锁检测"), "错误消息应包含'死锁检测'提示");
    assertTrue(message.contains("待处理任务及其依赖"), "错误消息应包含待处理任务列表标题");

    // 验证包含任务名称和依赖信息
    assertTrue(message.contains("taskA") || message.contains("taskB") || message.contains("taskC"),
        "错误消息应包含至少一个待处理任务的名称");

    // 验证包含优先级信息
    assertTrue(message.contains("优先级"),
        "错误消息应包含优先级信息");

    // 验证包含依赖关系信息（等待）
    assertTrue(message.contains("等待"),
        "错误消息应包含'等待'关键字表示依赖关系");
  }

  private static int invokeLoadThreadPoolSize() throws Exception {
    Method method = AsyncTaskRegistry.class.getDeclaredMethod("loadThreadPoolSize");
    method.setAccessible(true);
    return (int) method.invoke(null);
  }

  private static long invokeLoadDefaultTimeout() throws Exception {
    Method method = AsyncTaskRegistry.class.getDeclaredMethod("loadDefaultTimeout");
    method.setAccessible(true);
    return (long) method.invoke(null);
  }

}
