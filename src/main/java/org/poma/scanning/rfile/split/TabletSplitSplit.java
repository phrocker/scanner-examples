package org.poma.scanning.rfile.split;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This InputSplit contains a set of child InputSplits. Any InputSplit inserted into this collection must have a public default constructor.
 * 
 * In 3.x this can be replaced with CompositeInputSplit
 */
public class TabletSplitSplit extends org.apache.hadoop.mapreduce.InputSplit implements Writable {
    
    public static final String TABLE_NOT_SET = "TABLE_NOT_SET";
    
    protected int fill = 0;
    protected long totsize = 0L;
    protected String table = TABLE_NOT_SET;
    protected InputSplit[] splits;
    
    public TabletSplitSplit() {}
    
    public TabletSplitSplit(int capacity) {
        splits = new InputSplit[capacity];
    }
    
    /**
     * Sets the tablet table name.
     * 
     * @param table
     */
    public void setTable(String table) {
        this.table = table;
    }
    
    /**
     * Returns the table name, not the table Id.
     * 
     * @return
     */
    public String getTable() {
        return table;
    }
    
    /**
     * Add an InputSplit to this collection.
     * 
     * @throws IOException
     *             If capacity was not specified during construction or if capacity has been reached.
     * @throws InterruptedException
     */
    public void add(InputSplit s) throws IOException, InterruptedException {
        if (null == splits) {
            throw new IOException("Uninitialized InputSplit");
        }
        if (fill == splits.length) {
            throw new IOException("Too many splits");
        }
        
        splits[fill++] = s;
        totsize += s.getLength();
    }
    
    /**
     * Get ith child InputSplit.
     */
    public InputSplit get(int i) {
        return splits[i];
    }
    
    /**
     * Return the aggregate length of all child InputSplits currently added.
     */
    public long getLength() throws IOException {
        return splits.length;
    }
    
    /**
     * Get the length of ith child InputSplit.
     * 
     * @throws InterruptedException
     */
    public long getLength(int i) throws IOException, InterruptedException {
        return splits[i].getLength();
    }
    
    /**
     * Collect a set of hosts from all child InputSplits.
     * 
     * @throws InterruptedException
     */
    public String[] getLocations() throws IOException, InterruptedException {
        HashSet<String> hosts = new HashSet<>();
        for (InputSplit s : splits) {
            String[] hints = s.getLocations();
            if (hints != null && hints.length > 0) {
                Collections.addAll(hosts, hints);
            }
        }
        return hosts.toArray(new String[hosts.size()]);
    }
    
    /**
     * getLocations from ith InputSplit.
     * 
     * @throws InterruptedException
     */
    public String[] getLocation(int i) throws IOException, InterruptedException {
        return splits[i].getLocations();
    }
    
    /**
     * Write splits in the following format. {@code
     * <count><class1><class2>...<classn><split1><split2>...<splitn>
    * }
     */
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, table);
        WritableUtils.writeVInt(out, splits.length);
        for (InputSplit s : splits) {
            Text.writeString(out, s.getClass().getName());
        }
        for (InputSplit s : splits) {
            if (s instanceof Writable) {
                ((Writable) s).write(out);
            }
        }
    }
    
    /**
     * {@inheritDoc}
     * 
     * @throws IOException
     *             If the child InputSplit cannot be read, typically for failing access checks.
     */
    @SuppressWarnings("unchecked")
    // Generic array assignment
    public void readFields(DataInput in) throws IOException {
        table = WritableUtils.readString(in);
        int card = WritableUtils.readVInt(in);
        if (splits == null || splits.length != card) {
            splits = new InputSplit[card];
        }
        Class<? extends InputSplit>[] cls = new Class[card];
        try {
            for (int i = 0; i < card; ++i) {
                cls[i] = Class.forName(Text.readString(in)).asSubclass(InputSplit.class);
            }
            for (int i = 0; i < card; ++i) {
                splits[i] = ReflectionUtils.newInstance(cls[i], null);
                if (splits[i] instanceof Writable) {
                    ((Writable) splits[i]).readFields(in);
                }
            }
        } catch (ClassNotFoundException e) {
            throw (IOException) new IOException("Failed split init").initCause(e);
        }
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("{ ");
        for (InputSplit split : splits) {
            builder.append("[").append(split).append("] ");
        }
        builder.append(" }");
        return builder.toString();
    }
    
}
