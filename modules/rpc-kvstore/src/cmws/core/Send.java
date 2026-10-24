package cmws.core;

import dslabs.framework.Address;
import dslabs.framework.Message;

/** Send {@code message} to {@code to}. */
public record Send(Message message, Address to) implements Effect {}
