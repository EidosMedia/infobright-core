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

import static com.infobright.etl.model.DataFormatParams.*;

import java.nio.charset.Charset;
import java.util.List;

import com.infobright.etl.model.datatype.AbstractColumnType;
import com.infobright.logging.EtlLogger;

/**
 * bh_dataformat supported by Infobright
 */
public enum DataFormat {
  
  TXT_VARIABLE(
    FORMATSPEC_ICE,
    DISPLAYNAME_ICE,
    "load data %sinfile '%s' into table %s fields terminated by '"
        + TextRecord.TXT_DELIMITER + "' optionally enclosed by '"
        + TextRecord.TXT_ENCLOSURE + "' escaped by '\\\\' "
        + " lines terminated by '"
        + System.getProperty("line.separator") + "';",
    new BrighthouseRecordFactory() {
      public BrighthouseRecord createRecord(List<AbstractColumnType> columns, Charset charset, EtlLogger logger) {
        return new TextRecord(columns, charset);
      }
    }
  ),
  
  BINARY(
    FORMATSPEC_IEE,
    DISPLAYNAME_IEE,
    "load data %sinfile '%s' into table %s;",
    new BrighthouseRecordFactory() {
      public BrighthouseRecord createRecord(List<AbstractColumnType> columns, Charset charset, EtlLogger logger) {
        return new TeradataBinaryRecord(columns, charset, logger);
      }
    }
  );

  private final String bhDataFormat;
  private final String displayText;
  private final String sql;
  private final BrighthouseRecordFactory recordFactory;
  
  private DataFormat(String bhDataFormat, String displayText,
      String sql, BrighthouseRecordFactory recordFactory) {
    this.bhDataFormat = bhDataFormat;
    this.displayText = displayText;
    this.sql = sql;
    this.recordFactory = recordFactory;
  }

  public String getBhDataFormat() {
    return bhDataFormat;
  }
  
  public String getDisplayText() {
    return displayText;
  }

  /**
   * Kept for backward compatibility
   * 
   * @param pipeName
   * @param tableName
   * @param useLocalInfile
   * @return
   */
  public String getLoadSQL(String pipeName, String tableName) {
    return getLoadSQL(pipeName, tableName, false);
  }
  
  public String getLoadSQL(String pipeName, String tableName, boolean useLocalInfile) {
    return String.format(sql, useLocalInfile ? "local " : "", pipeName, tableName);
  }
  
  // Returns the appropriate type depending on the display text
  // in the combo box
  public static DataFormat valueForDisplayName(String text) {
    if (DISPLAYNAME_ICE.equals(text)) {
      return DataFormat.TXT_VARIABLE;
    } else if (DISPLAYNAME_IEE.equals(text)) {
      return DataFormat.BINARY;
    } else {
      return null;
    }
  }

  public BrighthouseRecord createRecord(List<AbstractColumnType> columns, Charset charset, EtlLogger logger) {
    return recordFactory.createRecord(columns, charset, logger);
  }
}