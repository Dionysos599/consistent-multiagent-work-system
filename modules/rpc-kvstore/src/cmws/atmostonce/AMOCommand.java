package cmws.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Command;
import lombok.Data;

@Data
public final class AMOCommand implements Command {
  private final Command command;
  private final int sequenceNum;
  private final Address sender;

  @Override
  public boolean readOnly() {
    return command.readOnly();
  }
}
