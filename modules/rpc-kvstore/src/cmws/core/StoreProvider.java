package cmws.core;

import dslabs.framework.Address;
import java.io.IOException;

/** Opens the stable storage that belongs to one node. */
@FunctionalInterface
public interface StoreProvider {
  DurableStore open(Address node) throws IOException;
}
