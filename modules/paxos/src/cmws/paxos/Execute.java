package cmws.paxos;

import cmws.atmostonce.AMOCommand;
import cmws.core.Effect;

/**
 * A command has been decided and is next in the log: apply it to the replicated state machine.
 *
 * @param respond true if this replica is the active leader and should answer the client
 */
record Execute(AMOCommand command, boolean respond) implements Effect {}
