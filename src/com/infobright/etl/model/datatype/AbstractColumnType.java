/*
The MIT License

Copyright (c) 2009 Infobright Inc.

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
*/

package com.infobright.etl.model.datatype;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Types;

import com.infobright.etl.model.ValueConverter;
import com.infobright.etl.model.ValueConverterException;
import com.infobright.logging.EtlLogger;

public abstract class AbstractColumnType {

  private String columnName;
  private boolean isNull;
  private boolean checkValues = false;
  
  /**
   * Sets column data supplied on byteBuffer
   * @param byteBuffer
   * @throws InvalidDataException
   */
  public abstract void setData(ByteBuffer byteBuffer) throws InvalidDataException;

  /**
   * Sets column data supplied from string
   * @param string
   */
  public abstract void setData(String string);

  /**
   * Gets column data as a string
   * @return string
   */
  public abstract String getDataAsString();

  /**
   * Gets column data and writes it to the supplied byteBuffer
   * @param byteBuffer
   */
  public abstract void getData(ByteBuffer byteBuffer);

  /**
   * Setter to mark this column as null.
   * 
   * @param b
   */
  public void setIsNull(boolean b) {
    isNull = b;
    if (b) {
      zeroOutData();
    }
  }

  /**
   * @return true if this column data is null
   */
  public boolean getIsNull() {
    return isNull;
  }
  
  /**
   * Sets the column name.
   * @param columnName
   */
  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }
  
  /**
   * @return column name
   */
  public String getColumnName() {
    return columnName;
  }
  
  /**
   * Used to set the data to an init value because even when
   * a field is null, the binary format loader/unloader
   * expects there to be dummy data in the record.
   */
  protected abstract void zeroOutData();

  /**
   * Sets column data supplied from kettle Value
   * @param value
   * @throws ValueConverterException
   */
  public abstract void setData(Object value, ValueConverter meta) throws ValueConverterException;

  /**
   * Is this a type of data (CHAR, VARCHAR DATE, DATETIME, etc.), that needs
   * to be enclosed for text load?
   */
  public abstract boolean isNeedsEnclosures();

  /**
   * Should strings and binary types be checked for size before being
   * passed to the database? (Should be set to TRUE if you support an
   * error path)
   * 
   * @param checkValues
   */
  public void setCheckValues(boolean checkValues) {
    this.checkValues = checkValues;
  }

  public boolean isCheckValues() {
    return checkValues;
  }
  
  /**
   * Factory method that creates an instance of the column type
   * appropriate for the JDBC column type supplied.  When new
   * column types are added, they must also be added to this method.
   * 
   * @param columnName
   * @param columnType @see java.sql.Types
   * @param columnTypeName 
   * @param precision
   * @param scale
   * @param charset
   * @param logger
   * @return a column type
   */
  public static AbstractColumnType getInstance(String columnName, int columnType,
      String columnTypeName, int precision, int scale, Charset charset,
      EtlLogger logger) {
    if (logger != null) {
      String logMsg;
      if (scale == 0) {
        logMsg = String.format("Column: %s %s(%d)", columnName, columnTypeName, precision);
      } else {
        logMsg = String.format("Column: %s %s(%d,%d)", columnName, columnTypeName, precision, scale);
      }
      logger.info(logMsg);
    }
    AbstractColumnType col = null;
    if (columnType == Types.VARCHAR) {
      col = new VarcharType(precision, charset);
    } else if (columnType == Types.SMALLINT) {
      col = new SmallintType();
    } else if (columnType == Types.INTEGER) {
      // MEDIUMINT is MySQL-specific and does not have its own sql Type
      if ("MEDIUMINT".equalsIgnoreCase(columnTypeName)) {
        col = new MediumintType();
      } else {
        col = new IntegerType();
      }
    } else if (columnType == Types.TINYINT || columnType == Types.BOOLEAN) {
      col = new TinyintType();
    } else if (columnType == Types.BIGINT) {
      col = new BigintType();
    } else if (columnType == Types.FLOAT || columnType == Types.REAL) {
      col = new FloatType();
    } else if (columnType == Types.DOUBLE) {
      col = new DoubleType();
    } else if (columnType == Types.CHAR) {
      col = new CharType(precision, charset);
    } else if (columnType == Types.TIMESTAMP) {
      // TIMESTAMP, DATETIME are treated the same
      col = new DatetimeType();
    } else if (columnType == Types.DATE) {
      if (precision == 4) {
        col = new YearType(); // show up as precision 4
      } else { /* precision == 10 */
        col = new DateIntType();
      }
    } else if (columnType == Types.BINARY) {
      col = new BinaryType(precision);
    } else if (columnType == Types.VARBINARY) {
      col = new VarbinaryType(precision);
    } else if (columnType == Types.LONGVARCHAR) {
      col = new TextType(precision, charset);
    } else if (columnType == Types.DECIMAL) {
      col = new DecimalType(precision, scale);
    } else if (columnType == Types.TIME) {
      col = new TimeType();
    } else {
      throw new RuntimeException("Unsupported type (" + columnTypeName + "," + columnType
          + ") for column " + columnName);
    }
    col.setColumnName(columnName);
    return col;
  }

  /**
   * Thrown when invalid data is supplied.
   * 
   */
  public class InvalidDataException extends Exception {

    public InvalidDataException(UnsupportedEncodingException e) {
      super(e);
    }

    /**
     * Serial version UID
     */
    private static final long serialVersionUID = -222513235333508756L;
    
  }
}
