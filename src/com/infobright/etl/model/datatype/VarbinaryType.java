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

import java.nio.ByteBuffer;

import com.infobright.etl.model.ValueConverter;
import com.infobright.etl.model.ValueConverterException;

class VarbinaryType extends AbstractColumnType {

  private final int maxlength;
  private int length;
  private byte[] data;

  /**
   * @param len length (should be 0..65535)
   */
  VarbinaryType(int len) {
    if (len > 65535) {
      throw new IllegalArgumentException("length of varbinary must be <=65535");
    }
    maxlength = len;
    length = 0;
    data = new byte[len];
  }
  
  @Override
  public void setData(ByteBuffer byteBuffer) throws InvalidDataException {
    length = byteBuffer.getShort();
    byteBuffer.get(data, 0, length);
  }

  @Override
  public String getDataAsString() {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < length; i++) {
      sb.append(String.format("%02x", data[i]));
    }
    return sb.toString();
  }

  @Override
  public void getData(ByteBuffer byteBuffer) {
    byteBuffer.putShort((short)length);
    byteBuffer.put(data, 0, length);    
  }

  @Override
  public void setData(String string) {
    int numbytes = string.length() / 2;
    if (isCheckValues() && numbytes > maxlength) {
      throw new ValueConverterException("data is too big for this column");
    }
    int offset = 0;
    for (int i = 0; i < string.length(); i += 2) {
      String hex = string.substring(i, i + 2);
      data[offset++] = (byte) java.lang.Integer.parseInt(hex, 16);
    }
    length = offset;
    padZerosStartingAt(numbytes);
  }

  private void padZerosStartingAt(int startIdx) {
    for (int i = startIdx; i < data.length; i++) {
      data[i] = 0;
    }
  }

  @Override
  protected void zeroOutData() {
    for (int i = 0; i < data.length; i++) {
      data[i] = 0;
    }
    length = 0;
  }

  @Override
  public void setData(Object value, ValueConverter meta)
      throws ValueConverterException {
    if (value == null) {
      setIsNull(true);
    } else {
      byte[] bytes = meta.getBinary(value);
      if (bytes == null) {
        setIsNull(true);
      } else {
        setIsNull(false);
        if (isCheckValues() && bytes.length > length) {
          throw new ValueConverterException("data is too big for this column");
        }
        data = bytes;
      }
    }
  }

  @Override
  public final boolean isNeedsEnclosures() {
    return true;
  }
}
