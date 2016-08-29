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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.infobright.etl.model.ValueConverter;
import com.infobright.etl.model.ValueConverterException;

abstract class DateType extends AbstractColumnType {

  private final SimpleDateFormat format; 
  private Date zero = new Date(0);
  private Date data = zero;
  private byte[] buf;
  
  DateType(String datePattern) {
    this.format = new SimpleDateFormat(datePattern);
    this.buf = new byte[this.format.toPattern().length()];
  }
  
  @Override
  public void setData(ByteBuffer byteBuffer) throws InvalidDataException {
    byteBuffer.get(buf);
    try {
      if (getIsNull()) {
        return;  // do nothing because the date data is bogus for this null field
      }
      String str = new String(buf, "ISO-8859-1");
      setData(str);
    } catch (UnsupportedEncodingException e1) {
      throw new RuntimeException(e1);
    }
  }

  @Override
  public void getData(ByteBuffer byteBuffer) {
    byteBuffer.put(getDataAsString().getBytes());    
  }
  
  @Override
  public String getDataAsString() {
    return format.format(data);
  }

  @Override
  public void setData(String string) {
    try {
      data = format.parse(string);
    } catch (ParseException e2) {
      throw new RuntimeException(e2);
    }
  }

  @Override
  protected void zeroOutData() {
    data = zero;
  }

  public void setData(Date d) {
    this.data = d;
  }
  
  protected Date getData() {
    return data;
  }
  
  @Override
  public void setData(Object value, ValueConverter meta)
      throws ValueConverterException {
    if (value == null) {
      setIsNull(true);
    } else {
      Date date = meta.getDate(value);
      if (date == null) {
        setIsNull(true);
      } else {
        setIsNull(false);
        setData(date);
      }
    }
  }

  @Override
  public final boolean isNeedsEnclosures() {
    return false;
  }
}
