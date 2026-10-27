package cmws.paxos;

import cmws.atmostonce.AMOCommand;
import java.io.Serializable;

/**
 * What a replica writes to stable storage. Replaying these records rebuilds the acceptor state
 * (promised ballot, accepted and chosen entries) after a restart.
 */
final class PaxosRecords {
  /** The highest ballot this replica has promised not to go below. */
  record Promise(Ballot ballot) implements Serializable {}

  /** An entry accepted for a slot; a {@code null} command is a no-op. */
  record Accept(int slot, Ballot ballot, AMOCommand command) implements Serializable {}

  /** A slot known to be decided. */
  record Learn(int slot, Ballot ballot, AMOCommand command) implements Serializable {}

  private PaxosRecords() {}
}
