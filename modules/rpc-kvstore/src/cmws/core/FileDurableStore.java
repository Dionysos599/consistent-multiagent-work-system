package cmws.core;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * A write-ahead log in a single file: each record is a length-prefixed Java-serialized object. An
 * {@link #append} returns only after the data has been forced to disk. A record that was only
 * partly written when the process died is ignored when the log is read back.
 */
public final class FileDurableStore implements DurableStore {
  private final Path file;
  private FileOutputStream out;

  public FileDurableStore(Path file) throws IOException {
    this.file = file;
    Files.createDirectories(file.toAbsolutePath().getParent());
  }

  @Override
  public synchronized void append(List<Serializable> records) throws IOException {
    if (records.isEmpty()) {
      return;
    }
    if (out == null) {
      out = new FileOutputStream(file.toFile(), true);
    }

    ByteArrayOutputStream batch = new ByteArrayOutputStream();
    DataOutputStream data = new DataOutputStream(batch);
    for (Serializable record : records) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (ObjectOutputStream oos = new ObjectOutputStream(bytes)) {
        oos.writeObject(record);
      }
      data.writeInt(bytes.size());
      bytes.writeTo(data);
    }
    data.flush();

    batch.writeTo(out);
    out.getFD().sync();
  }

  @Override
  public synchronized List<Serializable> readAll() throws IOException {
    List<Serializable> records = new ArrayList<>();
    if (!Files.exists(file)) {
      return records;
    }
    try (DataInputStream in =
        new DataInputStream(new BufferedInputStream(new FileInputStream(file.toFile())))) {
      while (true) {
        int length;
        try {
          length = in.readInt();
        } catch (EOFException end) {
          break;
        }
        byte[] bytes = new byte[length];
        try {
          in.readFully(bytes);
        } catch (EOFException torn) {
          break; // the last record was cut short by a crash
        }
        try (ObjectInputStream ois = new ObjectInputStream(new java.io.ByteArrayInputStream(bytes))) {
          records.add((Serializable) ois.readObject());
        } catch (ClassNotFoundException | java.io.StreamCorruptedException e) {
          throw new IOException("corrupt record in " + file, e);
        }
      }
    }
    return records;
  }

  public synchronized void close() throws IOException {
    if (out != null) {
      out.close();
      out = null;
    }
  }
}
