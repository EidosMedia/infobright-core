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

package com.infobright.etl.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.infobright.etl.model.datatype.AbstractColumnType;
import com.infobright.logging.EtlLogger;

/**
 * Data structure that stores all of the data for one row to be
 * written to Infobright.
 * 
 * @author geoffrey.falk@infobright.com
 */
public abstract class BrighthouseRecord {
  
  protected final List<AbstractColumnType> columns;
  
  private final Charset charset;
  
  /**
   * Reads the abstract column types from the SQL metadata.
   * 
   * @param md The metadata from the JDBC result set
   * @param charset the character set to use to encode String values
   *   for CHAR, VARCHAR column types
   * @param logger the logger to use
   * @param checkValues whether to check strings for length and
   * throw an exception immediately. Useful if implementing an error
   * path for rejected records. 
   *
   * @return list of column types from the table in the database
   * @throws SQLException
   */
  public static List<AbstractColumnType> readColumnTypes(ResultSetMetaData md, Charset charset, EtlLogger logger, boolean checkValues) throws SQLException {
    List<AbstractColumnType> columns = new ArrayList<AbstractColumnType>();
    for (int i = 1; i <= md.getColumnCount(); i++) {
      // In theory, could find out the character set encoding for each
      // column from the database, and pass it here, instead of relying on
      // the character set parameter being passed in. However, the character
      // encoding is not available from the standard JDBC/SQL metadata.
      AbstractColumnType colType = AbstractColumnType.getInstance(md.getColumnName(i), md.getColumnType(i),
          md.getColumnTypeName(i), md.getPrecision(i), md.getScale(i), charset, logger);
      colType.setCheckValues(checkValues);
      columns.add(colType);
    }
    return columns;
  }
  
  protected BrighthouseRecord(List<AbstractColumnType> columns, Charset charset) {
    this.columns = columns;
    this.charset = charset;
  }
  
  /**
   * Gets the number of columns.
   * 
   * @return number of columns
   */
  public abstract int size();

  /**
   * writes data to the output stream
   * 
   * @throws ValueConverterException on invalid data???FIXME
   * @param outputStream
   */
  public abstract void writeTo(OutputStream outputStream) throws IOException;

  /**
   * Sets value. Must be called repeatedly for each column.
   */
  public abstract void setData(int colidx, Object value, ValueConverter meta) throws ValueConverterException;

  protected Charset getCharset() {
    return charset;
  }

}
