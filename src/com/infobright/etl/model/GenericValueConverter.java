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

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.infobright.etl.model.ValueConverter;
import com.infobright.etl.model.ValueConverterException;

/**
 * Converts objects to various types, doing the "obvious" conversions.
 * 
 * There is one instance for each type to be converted.
 * 
 * @author gfalk
 */
public class GenericValueConverter implements ValueConverter {

  protected final DateFormat dateFormat_DATE = new SimpleDateFormat("yyyy-MM-dd");
  protected final DateFormat dateFormat_DATETIME = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  
  //@Override
  public BigDecimal getBigNumber(Object object) throws ValueConverterException {
    if (object == null) {
      return null;
    }
    if (object instanceof BigDecimal) {
      return (BigDecimal)object;
    } else if (object instanceof Float) {
      return new BigDecimal((Float)object);
    } else if (object instanceof Double) {
      return new BigDecimal((Double)object);
    } else if (object instanceof Number) {
      return new BigDecimal(((Number)object).longValue());
    }  else if (object instanceof String) {
      try {
        return new BigDecimal((String)object);
      } catch (NumberFormatException e) {}
    }
    throw new ValueConverterException("value \"" + object.toString() + "\" of type " + object.getClass().getName() + " is not convertible to BigDecimal");
  }

  //@Override
  public byte[] getBinary(Object object) throws ValueConverterException {
    if (object == null) {
      return null;
    }
    if (object instanceof byte[]) {
      return (byte[])object;
    } else {
      throw new ValueConverterException("value \"" + object.toString() + "\" of type " + object.getClass().getName() + " is not convertible to byte[]");
    }
  }

  //@Override
  public byte[] getBinaryString(Object object) throws ValueConverterException {
    return getBinary(object);
  }

  //@Override
  public Boolean getBoolean(Object object) throws ValueConverterException {
    if (object == null) {
      return null;
    }
    if (object instanceof Boolean) {
      return (Boolean)object;
    } else {
      throw new ValueConverterException("value \"" + object.toString() + "\" of type " + object.getClass().getName() + " is not convertible to Boolean");
    }
  }

  //@Override
  public Date getDate(Object object) throws ValueConverterException {
    if (object == null) {
      return null;
    }
    if (object instanceof Date) {
      return (Date)object;
    } else { // TODO handle String, date format
      throw new ValueConverterException("value \"" + object.toString() + "\" of type " + object.getClass().getName() + " is not convertible to Date");
    }
  }

  //@Override
  public Long getInteger(Object object) throws ValueConverterException {
    if (object == null) {
      return null;
    }
    if (object instanceof Number) {
      return ((Number)object).longValue();
    } else if (object instanceof Boolean) {
      Boolean bool = (Boolean) object;
      return (bool ? 1L : 0L);
    } else if (object instanceof String) {
      String str = (String)object;
      if (str.length() == 0) {
        return null;
      }
      try {
        return Long.parseLong(str);
      } catch (NumberFormatException e) {}
    }
    throw new ValueConverterException("value \"" + object.toString() + "\" of type " + object.getClass().getName() + " is not convertible to Long");
  }

  //@Override
  public Double getNumber(Object object) throws ValueConverterException {
    if (object == null) {
      return null;
    }
    if (object instanceof Number) {
      return ((Number)object).doubleValue();
    } else if (object instanceof String) {
      String str = (String)object;
      if (str.length() == 0) {
        return null;
      }
      try {
        return Double.parseDouble(str);
      } catch (NumberFormatException e) {}
    }
    throw new ValueConverterException("value \"" + object.toString() + "\" of type " + object.getClass().getName() + " is not convertible to Double");
  }

  //@Override
  public String getString(Object object) throws ValueConverterException {
    if (object == null) {
      return null;
    }
    if (object instanceof String) {
      return (String) object;
    } else if (object instanceof Character) {
      return new String(new char[] { (Character)object }, 0, 1);
    } else if (object instanceof char[]) {
      char[] ca = (char[])object;
      return new String(ca, 0, ca.length);
    } else if (object instanceof Number) {
      return String.valueOf(object);
    } else if (object instanceof Date) {
      // ambiguity with Date (don't know if it is DATE, DATETIME or TIMESTAMP).
      // Assume DATETIME
      synchronized (dateFormat_DATETIME) {
        return dateFormat_DATETIME.format((Date)object);
      }
    } else {
      throw new ValueConverterException("value \"" + object.toString() + "\" of type " + object.getClass().getName() + " is not convertible to String");
    }
  }
}
