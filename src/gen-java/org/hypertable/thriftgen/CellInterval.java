/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.hypertable.thriftgen;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Specifies a range of cells
 * 
 * <dl>
 *   <dt>start_row</dt>
 *   <dd>The row to start scan with. Must not contain nulls (0x00)</dd>
 * 
 *   <dt>start_column</dt>
 *   <dd>The column (prefix of column_family:column_qualifier) of the
 *   start row for the scan</dd>
 * 
 *   <dt>start_inclusive</dt>
 *   <dd>Whether the start row is included in the result (default: true)</dd>
 * 
 *   <dt>end_row</dt>
 *   <dd>The row to end scan with. Must not contain nulls</dd>
 * 
 *   <dt>end_column</dt>
 *   <dd>The column (prefix of column_family:column_qualifier) of the
 *   end row for the scan</dd>
 * 
 *   <dt>end_inclusive</dt>
 *   <dd>Whether the end row is included in the result (default: true)</dd>
 * </dl>
 */
public class CellInterval implements org.apache.thrift.TBase<CellInterval, CellInterval._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("CellInterval");

  private static final org.apache.thrift.protocol.TField START_ROW_FIELD_DESC = new org.apache.thrift.protocol.TField("start_row", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField START_COLUMN_FIELD_DESC = new org.apache.thrift.protocol.TField("start_column", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField START_INCLUSIVE_FIELD_DESC = new org.apache.thrift.protocol.TField("start_inclusive", org.apache.thrift.protocol.TType.BOOL, (short)3);
  private static final org.apache.thrift.protocol.TField END_ROW_FIELD_DESC = new org.apache.thrift.protocol.TField("end_row", org.apache.thrift.protocol.TType.STRING, (short)4);
  private static final org.apache.thrift.protocol.TField END_COLUMN_FIELD_DESC = new org.apache.thrift.protocol.TField("end_column", org.apache.thrift.protocol.TType.STRING, (short)5);
  private static final org.apache.thrift.protocol.TField END_INCLUSIVE_FIELD_DESC = new org.apache.thrift.protocol.TField("end_inclusive", org.apache.thrift.protocol.TType.BOOL, (short)6);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new CellIntervalStandardSchemeFactory());
    schemes.put(TupleScheme.class, new CellIntervalTupleSchemeFactory());
  }

  public String start_row; // optional
  public String start_column; // optional
  public boolean start_inclusive; // optional
  public String end_row; // optional
  public String end_column; // optional
  public boolean end_inclusive; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    START_ROW((short)1, "start_row"),
    START_COLUMN((short)2, "start_column"),
    START_INCLUSIVE((short)3, "start_inclusive"),
    END_ROW((short)4, "end_row"),
    END_COLUMN((short)5, "end_column"),
    END_INCLUSIVE((short)6, "end_inclusive");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // START_ROW
          return START_ROW;
        case 2: // START_COLUMN
          return START_COLUMN;
        case 3: // START_INCLUSIVE
          return START_INCLUSIVE;
        case 4: // END_ROW
          return END_ROW;
        case 5: // END_COLUMN
          return END_COLUMN;
        case 6: // END_INCLUSIVE
          return END_INCLUSIVE;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __START_INCLUSIVE_ISSET_ID = 0;
  private static final int __END_INCLUSIVE_ISSET_ID = 1;
  private BitSet __isset_bit_vector = new BitSet(2);
  private _Fields optionals[] = {_Fields.START_ROW,_Fields.START_COLUMN,_Fields.START_INCLUSIVE,_Fields.END_ROW,_Fields.END_COLUMN,_Fields.END_INCLUSIVE};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.START_ROW, new org.apache.thrift.meta_data.FieldMetaData("start_row", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.START_COLUMN, new org.apache.thrift.meta_data.FieldMetaData("start_column", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.START_INCLUSIVE, new org.apache.thrift.meta_data.FieldMetaData("start_inclusive", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.END_ROW, new org.apache.thrift.meta_data.FieldMetaData("end_row", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.END_COLUMN, new org.apache.thrift.meta_data.FieldMetaData("end_column", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.END_INCLUSIVE, new org.apache.thrift.meta_data.FieldMetaData("end_inclusive", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(CellInterval.class, metaDataMap);
  }

  public CellInterval() {
    this.start_inclusive = true;

    this.end_inclusive = true;

  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public CellInterval(CellInterval other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetStart_row()) {
      this.start_row = other.start_row;
    }
    if (other.isSetStart_column()) {
      this.start_column = other.start_column;
    }
    this.start_inclusive = other.start_inclusive;
    if (other.isSetEnd_row()) {
      this.end_row = other.end_row;
    }
    if (other.isSetEnd_column()) {
      this.end_column = other.end_column;
    }
    this.end_inclusive = other.end_inclusive;
  }

  public CellInterval deepCopy() {
    return new CellInterval(this);
  }

  @Override
  public void clear() {
    this.start_row = null;
    this.start_column = null;
    this.start_inclusive = true;

    this.end_row = null;
    this.end_column = null;
    this.end_inclusive = true;

  }

  public String getStart_row() {
    return this.start_row;
  }

  public CellInterval setStart_row(String start_row) {
    this.start_row = start_row;
    return this;
  }

  public void unsetStart_row() {
    this.start_row = null;
  }

  /** Returns true if field start_row is set (has been assigned a value) and false otherwise */
  public boolean isSetStart_row() {
    return this.start_row != null;
  }

  public void setStart_rowIsSet(boolean value) {
    if (!value) {
      this.start_row = null;
    }
  }

  public String getStart_column() {
    return this.start_column;
  }

  public CellInterval setStart_column(String start_column) {
    this.start_column = start_column;
    return this;
  }

  public void unsetStart_column() {
    this.start_column = null;
  }

  /** Returns true if field start_column is set (has been assigned a value) and false otherwise */
  public boolean isSetStart_column() {
    return this.start_column != null;
  }

  public void setStart_columnIsSet(boolean value) {
    if (!value) {
      this.start_column = null;
    }
  }

  public boolean isStart_inclusive() {
    return this.start_inclusive;
  }

  public CellInterval setStart_inclusive(boolean start_inclusive) {
    this.start_inclusive = start_inclusive;
    setStart_inclusiveIsSet(true);
    return this;
  }

  public void unsetStart_inclusive() {
    __isset_bit_vector.clear(__START_INCLUSIVE_ISSET_ID);
  }

  /** Returns true if field start_inclusive is set (has been assigned a value) and false otherwise */
  public boolean isSetStart_inclusive() {
    return __isset_bit_vector.get(__START_INCLUSIVE_ISSET_ID);
  }

  public void setStart_inclusiveIsSet(boolean value) {
    __isset_bit_vector.set(__START_INCLUSIVE_ISSET_ID, value);
  }

  public String getEnd_row() {
    return this.end_row;
  }

  public CellInterval setEnd_row(String end_row) {
    this.end_row = end_row;
    return this;
  }

  public void unsetEnd_row() {
    this.end_row = null;
  }

  /** Returns true if field end_row is set (has been assigned a value) and false otherwise */
  public boolean isSetEnd_row() {
    return this.end_row != null;
  }

  public void setEnd_rowIsSet(boolean value) {
    if (!value) {
      this.end_row = null;
    }
  }

  public String getEnd_column() {
    return this.end_column;
  }

  public CellInterval setEnd_column(String end_column) {
    this.end_column = end_column;
    return this;
  }

  public void unsetEnd_column() {
    this.end_column = null;
  }

  /** Returns true if field end_column is set (has been assigned a value) and false otherwise */
  public boolean isSetEnd_column() {
    return this.end_column != null;
  }

  public void setEnd_columnIsSet(boolean value) {
    if (!value) {
      this.end_column = null;
    }
  }

  public boolean isEnd_inclusive() {
    return this.end_inclusive;
  }

  public CellInterval setEnd_inclusive(boolean end_inclusive) {
    this.end_inclusive = end_inclusive;
    setEnd_inclusiveIsSet(true);
    return this;
  }

  public void unsetEnd_inclusive() {
    __isset_bit_vector.clear(__END_INCLUSIVE_ISSET_ID);
  }

  /** Returns true if field end_inclusive is set (has been assigned a value) and false otherwise */
  public boolean isSetEnd_inclusive() {
    return __isset_bit_vector.get(__END_INCLUSIVE_ISSET_ID);
  }

  public void setEnd_inclusiveIsSet(boolean value) {
    __isset_bit_vector.set(__END_INCLUSIVE_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case START_ROW:
      if (value == null) {
        unsetStart_row();
      } else {
        setStart_row((String)value);
      }
      break;

    case START_COLUMN:
      if (value == null) {
        unsetStart_column();
      } else {
        setStart_column((String)value);
      }
      break;

    case START_INCLUSIVE:
      if (value == null) {
        unsetStart_inclusive();
      } else {
        setStart_inclusive((Boolean)value);
      }
      break;

    case END_ROW:
      if (value == null) {
        unsetEnd_row();
      } else {
        setEnd_row((String)value);
      }
      break;

    case END_COLUMN:
      if (value == null) {
        unsetEnd_column();
      } else {
        setEnd_column((String)value);
      }
      break;

    case END_INCLUSIVE:
      if (value == null) {
        unsetEnd_inclusive();
      } else {
        setEnd_inclusive((Boolean)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case START_ROW:
      return getStart_row();

    case START_COLUMN:
      return getStart_column();

    case START_INCLUSIVE:
      return Boolean.valueOf(isStart_inclusive());

    case END_ROW:
      return getEnd_row();

    case END_COLUMN:
      return getEnd_column();

    case END_INCLUSIVE:
      return Boolean.valueOf(isEnd_inclusive());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case START_ROW:
      return isSetStart_row();
    case START_COLUMN:
      return isSetStart_column();
    case START_INCLUSIVE:
      return isSetStart_inclusive();
    case END_ROW:
      return isSetEnd_row();
    case END_COLUMN:
      return isSetEnd_column();
    case END_INCLUSIVE:
      return isSetEnd_inclusive();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof CellInterval)
      return this.equals((CellInterval)that);
    return false;
  }

  public boolean equals(CellInterval that) {
    if (that == null)
      return false;

    boolean this_present_start_row = true && this.isSetStart_row();
    boolean that_present_start_row = true && that.isSetStart_row();
    if (this_present_start_row || that_present_start_row) {
      if (!(this_present_start_row && that_present_start_row))
        return false;
      if (!this.start_row.equals(that.start_row))
        return false;
    }

    boolean this_present_start_column = true && this.isSetStart_column();
    boolean that_present_start_column = true && that.isSetStart_column();
    if (this_present_start_column || that_present_start_column) {
      if (!(this_present_start_column && that_present_start_column))
        return false;
      if (!this.start_column.equals(that.start_column))
        return false;
    }

    boolean this_present_start_inclusive = true && this.isSetStart_inclusive();
    boolean that_present_start_inclusive = true && that.isSetStart_inclusive();
    if (this_present_start_inclusive || that_present_start_inclusive) {
      if (!(this_present_start_inclusive && that_present_start_inclusive))
        return false;
      if (this.start_inclusive != that.start_inclusive)
        return false;
    }

    boolean this_present_end_row = true && this.isSetEnd_row();
    boolean that_present_end_row = true && that.isSetEnd_row();
    if (this_present_end_row || that_present_end_row) {
      if (!(this_present_end_row && that_present_end_row))
        return false;
      if (!this.end_row.equals(that.end_row))
        return false;
    }

    boolean this_present_end_column = true && this.isSetEnd_column();
    boolean that_present_end_column = true && that.isSetEnd_column();
    if (this_present_end_column || that_present_end_column) {
      if (!(this_present_end_column && that_present_end_column))
        return false;
      if (!this.end_column.equals(that.end_column))
        return false;
    }

    boolean this_present_end_inclusive = true && this.isSetEnd_inclusive();
    boolean that_present_end_inclusive = true && that.isSetEnd_inclusive();
    if (this_present_end_inclusive || that_present_end_inclusive) {
      if (!(this_present_end_inclusive && that_present_end_inclusive))
        return false;
      if (this.end_inclusive != that.end_inclusive)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(CellInterval other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    CellInterval typedOther = (CellInterval)other;

    lastComparison = Boolean.valueOf(isSetStart_row()).compareTo(typedOther.isSetStart_row());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStart_row()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.start_row, typedOther.start_row);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStart_column()).compareTo(typedOther.isSetStart_column());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStart_column()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.start_column, typedOther.start_column);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStart_inclusive()).compareTo(typedOther.isSetStart_inclusive());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStart_inclusive()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.start_inclusive, typedOther.start_inclusive);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetEnd_row()).compareTo(typedOther.isSetEnd_row());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetEnd_row()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.end_row, typedOther.end_row);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetEnd_column()).compareTo(typedOther.isSetEnd_column());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetEnd_column()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.end_column, typedOther.end_column);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetEnd_inclusive()).compareTo(typedOther.isSetEnd_inclusive());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetEnd_inclusive()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.end_inclusive, typedOther.end_inclusive);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CellInterval(");
    boolean first = true;

    if (isSetStart_row()) {
      sb.append("start_row:");
      if (this.start_row == null) {
        sb.append("null");
      } else {
        sb.append(this.start_row);
      }
      first = false;
    }
    if (isSetStart_column()) {
      if (!first) sb.append(", ");
      sb.append("start_column:");
      if (this.start_column == null) {
        sb.append("null");
      } else {
        sb.append(this.start_column);
      }
      first = false;
    }
    if (isSetStart_inclusive()) {
      if (!first) sb.append(", ");
      sb.append("start_inclusive:");
      sb.append(this.start_inclusive);
      first = false;
    }
    if (isSetEnd_row()) {
      if (!first) sb.append(", ");
      sb.append("end_row:");
      if (this.end_row == null) {
        sb.append("null");
      } else {
        sb.append(this.end_row);
      }
      first = false;
    }
    if (isSetEnd_column()) {
      if (!first) sb.append(", ");
      sb.append("end_column:");
      if (this.end_column == null) {
        sb.append("null");
      } else {
        sb.append(this.end_column);
      }
      first = false;
    }
    if (isSetEnd_inclusive()) {
      if (!first) sb.append(", ");
      sb.append("end_inclusive:");
      sb.append(this.end_inclusive);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bit_vector = new BitSet(1);
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class CellIntervalStandardSchemeFactory implements SchemeFactory {
    public CellIntervalStandardScheme getScheme() {
      return new CellIntervalStandardScheme();
    }
  }

  private static class CellIntervalStandardScheme extends StandardScheme<CellInterval> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, CellInterval struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // START_ROW
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.start_row = iprot.readString();
              struct.setStart_rowIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // START_COLUMN
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.start_column = iprot.readString();
              struct.setStart_columnIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // START_INCLUSIVE
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.start_inclusive = iprot.readBool();
              struct.setStart_inclusiveIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // END_ROW
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.end_row = iprot.readString();
              struct.setEnd_rowIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // END_COLUMN
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.end_column = iprot.readString();
              struct.setEnd_columnIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // END_INCLUSIVE
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.end_inclusive = iprot.readBool();
              struct.setEnd_inclusiveIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, CellInterval struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.start_row != null) {
        if (struct.isSetStart_row()) {
          oprot.writeFieldBegin(START_ROW_FIELD_DESC);
          oprot.writeString(struct.start_row);
          oprot.writeFieldEnd();
        }
      }
      if (struct.start_column != null) {
        if (struct.isSetStart_column()) {
          oprot.writeFieldBegin(START_COLUMN_FIELD_DESC);
          oprot.writeString(struct.start_column);
          oprot.writeFieldEnd();
        }
      }
      if (struct.isSetStart_inclusive()) {
        oprot.writeFieldBegin(START_INCLUSIVE_FIELD_DESC);
        oprot.writeBool(struct.start_inclusive);
        oprot.writeFieldEnd();
      }
      if (struct.end_row != null) {
        if (struct.isSetEnd_row()) {
          oprot.writeFieldBegin(END_ROW_FIELD_DESC);
          oprot.writeString(struct.end_row);
          oprot.writeFieldEnd();
        }
      }
      if (struct.end_column != null) {
        if (struct.isSetEnd_column()) {
          oprot.writeFieldBegin(END_COLUMN_FIELD_DESC);
          oprot.writeString(struct.end_column);
          oprot.writeFieldEnd();
        }
      }
      if (struct.isSetEnd_inclusive()) {
        oprot.writeFieldBegin(END_INCLUSIVE_FIELD_DESC);
        oprot.writeBool(struct.end_inclusive);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class CellIntervalTupleSchemeFactory implements SchemeFactory {
    public CellIntervalTupleScheme getScheme() {
      return new CellIntervalTupleScheme();
    }
  }

  private static class CellIntervalTupleScheme extends TupleScheme<CellInterval> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, CellInterval struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetStart_row()) {
        optionals.set(0);
      }
      if (struct.isSetStart_column()) {
        optionals.set(1);
      }
      if (struct.isSetStart_inclusive()) {
        optionals.set(2);
      }
      if (struct.isSetEnd_row()) {
        optionals.set(3);
      }
      if (struct.isSetEnd_column()) {
        optionals.set(4);
      }
      if (struct.isSetEnd_inclusive()) {
        optionals.set(5);
      }
      oprot.writeBitSet(optionals, 6);
      if (struct.isSetStart_row()) {
        oprot.writeString(struct.start_row);
      }
      if (struct.isSetStart_column()) {
        oprot.writeString(struct.start_column);
      }
      if (struct.isSetStart_inclusive()) {
        oprot.writeBool(struct.start_inclusive);
      }
      if (struct.isSetEnd_row()) {
        oprot.writeString(struct.end_row);
      }
      if (struct.isSetEnd_column()) {
        oprot.writeString(struct.end_column);
      }
      if (struct.isSetEnd_inclusive()) {
        oprot.writeBool(struct.end_inclusive);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, CellInterval struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(6);
      if (incoming.get(0)) {
        struct.start_row = iprot.readString();
        struct.setStart_rowIsSet(true);
      }
      if (incoming.get(1)) {
        struct.start_column = iprot.readString();
        struct.setStart_columnIsSet(true);
      }
      if (incoming.get(2)) {
        struct.start_inclusive = iprot.readBool();
        struct.setStart_inclusiveIsSet(true);
      }
      if (incoming.get(3)) {
        struct.end_row = iprot.readString();
        struct.setEnd_rowIsSet(true);
      }
      if (incoming.get(4)) {
        struct.end_column = iprot.readString();
        struct.setEnd_columnIsSet(true);
      }
      if (incoming.get(5)) {
        struct.end_inclusive = iprot.readBool();
        struct.setEnd_inclusiveIsSet(true);
      }
    }
  }

}

