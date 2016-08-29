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


class SmallintType extends AbstractColumnType {

  private Short data = new Short((short)0);
  
  @Override
  public String getDataAsString() {
    return data.toString();
  }

  @Override
  public void getData(ByteBuffer byteBuffer) {
    byteBuffer.putShort(data);
  }
  
  @Override
  public void setData(ByteBuffer byteBuffer) throws InvalidDataException {
    data = byteBuffer.getShort();
  }

  @Override
  public void setData(String string) {
    data = Short.valueOf(string);
  }

  @Override
  protected void zeroOutData() {
    data = (short) 0;
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
        if (val >= 1L<<15 || val < -(1L<<15)) {
          throw new ValueConverterException("Value " + val + " out of range for SMALLINT");
        }
        data = val.shortValue();
      }
    }
  }
  
  @Override
  public final boolean isNeedsEnclosures() {
    return false;
  }
}
