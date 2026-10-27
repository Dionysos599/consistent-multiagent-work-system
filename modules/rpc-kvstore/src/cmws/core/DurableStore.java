package cmws.core;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/** Append-only stable storage for one node's records. */
public interface DurableStore {
  /** Append the records, in order, and return only once they are durable. */
  void append(List<Serializable> records) throws IOException;

  /** Every record written so far, in order. */
  List<Serializable> readAll() throws IOException;
}
