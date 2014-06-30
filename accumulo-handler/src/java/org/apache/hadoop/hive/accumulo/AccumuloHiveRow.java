package org.apache.hadoop.hive.accumulo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Writable;

/**
 * Holds column tuples for rowID. Each tuple contains column family label, qualifier label, and byte
 * array value.
 */
public class AccumuloHiveRow implements Writable {

  private String rowId;
  List<ColumnTuple> tuples = new ArrayList<ColumnTuple>();

  public AccumuloHiveRow() {}

  public AccumuloHiveRow(String rowId) {
    this.rowId = rowId;
  }

  public void setRowId(String rowId) {
    this.rowId = rowId;
  }

  /**
   * 
   * @return true if this instance has a tuple containing fam and qual, false otherwise.
   */
  public boolean hasFamAndQual(String fam, String qual) {
    for (ColumnTuple tuple : tuples) {
      if (tuple.getCf().equals(fam) && tuple.getCq().equals(qual)) {
        return true;
      }
    }
    return false;
  }

  /**
   * 
   * @return byte [] value for first tuple containing fam and qual or null if no match.
   */
  public byte[] getValue(String fam, String qual) {
    for (ColumnTuple tuple : tuples) {
      if (tuple.getCf().equals(fam) && tuple.getCq().equals(qual)) {
        return tuple.getValue();
      }
    }
    return null;
  }

  public String getRowId() {
    return rowId;
  }

  public void clear() {
    this.rowId = null;
    this.tuples = new ArrayList<ColumnTuple>();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("AccumuloHiveRow{");
    builder.append("rowId='").append(rowId).append("', tuples: ");
    for (ColumnTuple tuple : tuples) {
      builder.append(tuple.toString());
      builder.append("\n");
    }
    return builder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof AccumuloHiveRow) {
      AccumuloHiveRow other = (AccumuloHiveRow) o; 
      if (null == rowId) {
        if (null != other.rowId) {
          return false;
        }
      } else if (!rowId.equals(other.rowId)) {
        return false;
      }

      return tuples.equals(other.tuples);
    }

    return false;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    if (null != rowId) {
      dataOutput.writeBoolean(true);
      dataOutput.writeUTF(rowId);
    } else {
      dataOutput.writeBoolean(false);
    }
    int size = tuples.size();
    dataOutput.writeInt(size);
    for (ColumnTuple tuple : tuples) {
      dataOutput.writeUTF(tuple.getCf());
      dataOutput.writeUTF(tuple.getCq());
      byte[] value = tuple.getValue();
      dataOutput.writeInt(value.length);
      dataOutput.write(value);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    if (dataInput.readBoolean()) {
      rowId = dataInput.readUTF();
    }
    int size = dataInput.readInt();
    for (int i = 0; i < size; i++) {
      String cf = dataInput.readUTF();
      String qual = dataInput.readUTF();
      int valSize = dataInput.readInt();
      byte[] val = new byte[valSize];
      for (int j = 0; j < valSize; j++) {
        val[j] = dataInput.readByte();
      }
      tuples.add(new ColumnTuple(cf, qual, val));
    }
  }

  public void add(String cf, String qual, byte[] val) {
    tuples.add(new ColumnTuple(cf, qual, val));
  }

  public static class ColumnTuple {
    private final String cf;
    private final String cq;
    private final byte[] value;

    public ColumnTuple(String cf, String cq, byte[] value) {
      this.value = value;
      this.cf = cf;
      this.cq = cq;
    }

    public byte[] getValue() {
      return value;
    }

    public String getCf() {
      return cf;
    }

    public String getCq() {
      return cq;
    }

    @Override
    public int hashCode() {
      HashCodeBuilder hcb = new HashCodeBuilder(9683, 68783);
      return hcb.append(cf).append(cq).append(value).toHashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ColumnTuple) {
        ColumnTuple other = (ColumnTuple) o;
        if (null == cf) {
          if (null != other.cf) {
            return false;
          }
        } else if (!cf.equals(other.cf)) {
          return false;
        }

        if (null == cq) {
          if (null != other.cq) {
            return false;
          }
        } else if (!cq.equals(other.cq)) {
          return false;
        }

        if (null == value) {
          if (null != other.value) {
            return false;
          }
        }

        return Arrays.equals(value, other.value);
      }

      return false;
    }

    @Override
    public String toString() {
      return cf + " " + cq + " " + new String(value);
    }
  }
}
