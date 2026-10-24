package cmws.core;

import dslabs.framework.Timer;

/** Arm {@code timer} to fire after {@code millis} milliseconds. */
public record SetTimer(Timer timer, int millis) implements Effect {}
