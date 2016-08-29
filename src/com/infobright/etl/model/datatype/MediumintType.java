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
import java.nio.ByteOrder;

import com.infobright.etl.model.ValueConverter;
import com.infobright.etl.model.ValueConverterException;

/**
 * A MySQL MEDIUMINT (3 bytes).
 * @author gfalk
 */
class MediumintType extends AbstractColumnType {

  private int data;
  
  @Override
  public String getDataAsString() {
    return java.lang.Integer.toString(data);
  }

  @Override
  public void getData(ByteBuffer byteBuffer) {
    short lsbb = (short)(data & 0xffff);
    byte msb = (byte)((data >> 16) & 0xff);
    if (byteBuffer.order() == ByteOrder.LITTLE_ENDIAN) {
      byteBuffer.putShort(lsbb);
      byteBuffer.put(msb);
    } else {
      byteBuffer.put(msb);
      byteBuffer.putShort(lsbb);
    }
  }
  
  @Override
  public void setData(ByteBuffer byteBuffer) throws InvalidDataException {
    short lsbb;
    byte msb;
    if (byteBuffer.order() == ByteOrder.LITTLE_ENDIAN) {
      lsbb = byteBuffer.getShort();
      msb = byteBuffer.get();
    } else {
      msb = byteBuffer.get();
      lsbb = byteBuffer.getShort();
    }
    data = (((int)lsbb) & 0xffff) | (((int)msb) << 16);
  }

  @Override
  public void setData(String string) {
    data = java.lang.Integer.valueOf(string);
  }

  @Override
  protected void zeroOutData() {
    data = 0;
  }

  @Override
  public void setData(Object value, ValueConverter meta)
      throws ValueConverterException {
    if (value == null) {
      setIsNull(true);
    } else {
      Long val = meta.getInteger(value);
      if (val == null) {
        setIsNull(true);
        } else {
        setIsNull(false);
        if (val >= 1L<<23 || val < -(1L<<23)) {
          throw new ValueConverterException("Value " + val + " out of range for MEDIUMINT");
        }
        data = val.intValue();
      }
    }
  }
  
  @Override
  public final boolean isNeedsEnclosures() {
    return false;
  }
}
