package cmws.core;

/**
 * Something a protocol state machine asks its host to do.
 *
 * <p>The protocol cores in this project are pure: they take an event (a message, a timer, a
 * decision) and return the effects it causes, without sending, scheduling or persisting anything
 * themselves. A thin adapter (a {@code Node} when running under the test framework, a network
 * runtime later) turns the effects into real actions. Protocol-specific effects extend this
 * interface; {@link Send} and {@link SetTimer} are common to all of them.
 */
public interface Effect {}
