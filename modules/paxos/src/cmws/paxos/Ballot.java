package cmws.paxos;

import dslabs.framework.Address;
import java.io.Serializable;
import lombok.Data;

/**
 * Ballot represents a unique identifier for a round of Paxos, consisting of a round number and
 * server address.
 */
@Data
class Ballot implements Serializable, Comparable<Ballot> {
  private final int round;
  private final Address server;

  @Override
  public int compareTo(Ballot other) {
    if (this.round != other.round) {
      return Integer.compare(this.round, other.round);
    }
    return this.server.compareTo(other.server);
  }
}
