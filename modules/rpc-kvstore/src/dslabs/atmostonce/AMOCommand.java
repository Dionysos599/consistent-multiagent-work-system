package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Command;
import lombok.Data;

@Data
public final class AMOCommand implements Command {
  private final Address clientAddress;
  private final int sequenceNum;
  private final Command command;

  @Override
  public boolean readOnly() {
    return command.readOnly();
  }
}
