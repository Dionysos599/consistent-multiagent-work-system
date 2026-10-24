package cmws.shardkv;

import cmws.atmostonce.AMOCommand;
import cmws.core.Effect;

/** Put {@code command} into the group's Paxos log; it takes effect when the log decides it. */
record Propose(AMOCommand command) implements Effect {}
