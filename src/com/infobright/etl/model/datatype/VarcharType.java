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

import com.infobright.etl.model.ValueConverter;
import com.infobright.etl.model.ValueConverterException;

class VarcharType extends AbstractColumnType {

  private byte[] data = new byte[0];
  private final int length;
  private final Charset charset;
  
  VarcharType(int len, Charset charset) {
    this.length = len;
    this.charset = charset;
  }

  /**
   * Used when reading in from the stream (not currently used)
   */
  @Override
  public void setData(ByteBuffer byteBuffer) throws InvalidDataException {
    short len = byteBuffer.getShort();
    data = new byte[len];
    byteBuffer.get(data);
  }

  @Override
  public String getDataAsString() {
    try {
      return new String(data, charset.name());
    } catch (UnsupportedEncodingException e) {
      // should not happen, since we started with a valid Charset
      throw new RuntimeException(e);
    }
  }

  /**
   * This is the method that's used for output in binary format.
   */
  @Override
  public void getData(ByteBuffer byteBuffer) {
    byteBuffer.putShort((short)data.length);
    byteBuffer.put(data);
  }

  @Override
  public void setData(String string) throws ValueConverterException {
    if (string == null) {
      setIsNull(true);
    } else {
      if (isCheckValues() && string.length() > length) {
        throw new ValueConverterException("data is too big for this column");
      }
      setIsNull(false);
      try {
        data = string.getBytes(charset.name());
      } catch (UnsupportedEncodingException e) {
        // should not happen, since we started with a valid Charset
        throw new RuntimeException(e);
      }

    }
  }

  @Override
  protected void zeroOutData() {
    data = new byte[0];
  }

  @Override
  public void setData(Object value, ValueConverter meta)
      throws ValueConverterException {
    if (value == null) {
      setIsNull(true);
    } else {
      String val = meta.getString(value);
      if (val == null) {
        setIsNull(true);
      } else {
        setIsNull(false);
        setData(val);
      }
    }
  }

  @Override
  public final boolean isNeedsEnclosures() {
    return true;
  }
}
