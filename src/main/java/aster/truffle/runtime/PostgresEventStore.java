package aster.truffle.runtime;

import aster.runtime.workflow.EventStore;

/**
 * Truffle 运行时访问 PostgreSQL 事件存储的最小接口。
 * <p>
 * 该接口扩展 {@link EventStore}，便于 AsyncTaskRegistry 记录重试事件
 * 而无需直接依赖 Quarkus 模块中的具体实现。
 */
public interface PostgresEventStore extends EventStore {
}
