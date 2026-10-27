package cmws.core;

import java.io.Serializable;

/**
 * Write {@code record} to stable storage. The host must complete every {@code Persist} in a batch
 * of effects before it performs any other effect of that batch (sends, executions), so that a
 * promise or an acceptance is on disk before anyone can learn about it.
 */
public record Persist(Serializable record) implements Effect {}
