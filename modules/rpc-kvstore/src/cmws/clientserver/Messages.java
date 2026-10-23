package cmws.clientserver;

import cmws.atmostonce.AMOCommand;
import cmws.atmostonce.AMOResult;
import dslabs.framework.Message;
import lombok.Data;

@Data
class Request implements Message {
  private final AMOCommand command;
}

@Data
class Reply implements Message {
  private final AMOResult result;
}
