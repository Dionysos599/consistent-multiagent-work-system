package cmws.atmostonce;

import dslabs.framework.Result;
import lombok.Data;

/**
 * Placeholder result for a command that arrived after the same client had already moved on to a
 * newer one. The command is not executed; the client has given up on it, so nobody reads the value.
 */
@Data
public final class StaleCommand implements Result {}
