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
import java.util.List;

import com.infobright.etl.model.datatype.AbstractColumnType;

class TextRecord extends BrighthouseRecord {

  TextRecord(List<AbstractColumnType> columns, Charset charset) {
    super(columns, charset);
    data = new String[columns.size()];
  }
  
  /**
   * Delimiter to use for text load
   */
  static final String TXT_DELIMITER = ",";
  
  /**
   * Enclosure to use for text load
   */
  static final Character TXT_ENCLOSURE = '\"';

  /**
   * Escape character to use for text load
   */
  static final Character TXT_ESC_CHAR = '\\';

  /**
   * Indication of null in a text load file
   */
  static final String NULL_STR = "\\N";
  
  private final byte[] lineTerminator = System.getProperty("line.separator").getBytes();
  private final String[] data;

  @Override
  public int size() {
    return data.length;
  }
  
  @Override
  public void writeTo(OutputStream outputStream) throws IOException {
    for (int i = 0; i < data.length; i++) {
      if (data[i] != null) {
        byte[] bytes = data[i].getBytes(getCharset().name());
        outputStream.write(bytes);
      }
      if (i < data.length - 1) {
        outputStream.write(TXT_DELIMITER.getBytes());
      }
    }
    outputStream.write(lineTerminator);
  }

  @Override
  public void setData(int colidx, Object value, ValueConverter meta)
      throws ValueConverterException {
    if (value == null) {
      data[colidx] = NULL_STR;
      return;
    }
    AbstractColumnType type = columns.get(colidx);
    type.setData(value, meta);
    String sData = type.getDataAsString();
    if (sData == null) {
      data[colidx] = NULL_STR;
      return;
    }
    // If database column needs enclosures, use enclosures
    if (type.isNeedsEnclosures()) {
      sData = escapeString(sData);
    }
    data[colidx] = sData;
  }

  private String escapeString(String str) {
    StringBuffer buf = new StringBuffer(TXT_ENCLOSURE.toString());
    if (str.indexOf(TXT_ENCLOSURE) >= 0 || str.indexOf(TXT_ESC_CHAR) >= 0) {
      // escape all embedded enclosure and escape characters
      for (int i = 0; i < str.length(); i++) {
        char c = str.charAt(i);
        if (TXT_ENCLOSURE.equals(c) || TXT_ESC_CHAR.equals(c)) {
          buf.append(TXT_ESC_CHAR);
        }
        buf.append(c);
      }
    } else {
      buf.append(str);
    }
    buf.append(TXT_ENCLOSURE);
    return buf.toString();
  }
  
  public String toString() {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < data.length; i++) {
      if (data[i] != null) {
        buf.append(data[i]);
      }
      if (i < data.length - 1) {
        buf.append(TXT_DELIMITER);
      }
    }
    return buf.toString();
  }
}
