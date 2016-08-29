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
import java.util.Calendar;
import java.util.GregorianCalendar;


class DateIntType extends DateType {
  private Calendar calendar = new GregorianCalendar();
  
  DateIntType() {
    super("yyyy-MM-dd");
  }
  
  @Override
  public void setData(ByteBuffer byteBuffer) throws InvalidDataException {
    int date = byteBuffer.getInt();
    int year = 0;
    int month = 0;
    int day = 0;
    if (date > 0) {
      // this is a date from 1900 and greater
      //    (1900 + (tddate)/10000)* 10000 + (tddate)%10000;
      year = 1900 + (date/10000);
      int rem = date % 10000;
      month = (rem / 100) - 1;
      day = rem % 100;

    } else if (date < 0) {
      // this is a date before 1900
      //   (1899-(~tddate)/10000) * 10000 + (9999-(~tddate)%10000);
      date = ~date;
      year = 1899 - (date/10000);
      int rem = date % 10000;
      month = ((9999 - rem) / 100) - 1;
      day = (9999 - rem) % 100;
    }

    calendar.clear();
    calendar.set(year, month, day);
    setData(calendar.getTime());
  }

  @Override
  public void getData(ByteBuffer byteBuffer) {
    calendar.clear();
    calendar.setTime(getData());
    int year = calendar.get(Calendar.YEAR);
    int date = 0;
    if (year >= 1900) {
      // this is a date from 1900 and greater
      //    (1900 + (tddate)/10000)* 10000 + (tddate)%10000;
      int month = calendar.get(Calendar.MONTH) + 1;
      int day = calendar.get(Calendar.DAY_OF_MONTH);
      date = (year - 1900) * 10000;
      date += (month * 100);
      date += day;
    } else {
      // this is a date before 1900
      //   (1899-(~tddate)/10000) * 10000 + (9999-(~tddate)%10000);
      int month = calendar.get(Calendar.MONTH) + 1;
      int day = calendar.get(Calendar.DAY_OF_MONTH);
      date = (year - 1900) * 10000;
      date += (month * 100);
      date += day;
    }
    byteBuffer.putInt(date);
  }

}
