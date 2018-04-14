package it.polimi.affetti.tspoon.tgraph.twopc;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by affo on 31/07/17.
 */
public class FileWAL {
    private String fileName;
    private File wal;
    private ObjectOutputStream out;

    public FileWAL(String fileName) {
        this.fileName = fileName;
    }

    public void open() throws IOException {
        wal = new File(fileName);
        wal.createNewFile();
        out = new ObjectOutputStream(new FileOutputStream(wal, false));
    }

    public void close() throws IOException {
        out.close();
    }

    public void addEntry(WAL.Entry entry) {
        try {
            out.writeObject(entry);
            out.flush();
            out.reset();
        } catch (IOException e) {
            // make it crash, we cannot avoid persisting the WAL
            throw new RuntimeException("Cannot persist to WAL");
        }
    }

    /**
     * If namespace is null it returns every entry, no matter the namespace
     * @param namespace
     * @return
     * @throws IOException
     */
    public Iterator<WAL.Entry> replay(String namespace) throws IOException {
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(wal));

        try {
            List<WAL.Entry> entries = new ArrayList<>();
            while (true) {
                try {
                    WAL.Entry e = (WAL.Entry) in.readObject();
                    if (e.updates != null && (namespace == null || e.updates.isInvolved(namespace))) {
                        entries.add(e);
                    }
                } catch (EOFException eof) {
                    break;
                }
            }

            return entries.iterator();
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Cannot recover from WAL: " + ex.getMessage());
        } finally {
            in.close();
        }
    }

    /**
     * Empties the file
     */
    public void clear() throws IOException {
        close();
        open();
    }
}
