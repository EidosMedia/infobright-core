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

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import com.infobright.etl.model.ValueConverter;
import com.infobright.etl.model.ValueConverterException;

class DecimalType extends AbstractColumnType {

  private static final BigDecimal TEN = new BigDecimal(10);
  
  private final BigDecimal maxValue;
  private final BigDecimal minValue;
  
  private final int precision;
  
  // stored in premultiplied form (Eg. if scale=2, "1.23" stored as "123")
  private BigDecimal data;

  private final BigDecimal multiplier;
  
  DecimalType(int precision, int scale) {
    // TODO make sure precision is between 1 and 18
    this.precision = precision;
    this.multiplier = TEN.pow(scale);
    this.maxValue = TEN.pow(precision).subtract(BigDecimal.ONE);
    this.minValue = maxValue.negate();
    this.data = BigDecimal.ZERO;
  }
  
  @Override
  public String getDataAsString() {
    return data.divide(multiplier).toString();
  }

  @Override
  public void getData(ByteBuffer byteBuffer) {
    if (precision < 3) { /* byte: 1 byte */
      byteBuffer.put(data.byteValue());
    } else if (precision < 5) { /* short: 2 bytes */
      byteBuffer.putShort(data.shortValue());
    } else if (precision < 10) { /* int: 4 bytes */
      byteBuffer.putInt(data.intValue());
    } else { /* long: 8 bytes */
      byteBuffer.putLong(data.longValue());
    }
  }

  /**
   * @param data the premultiplied value
   */
  private void setData(BigDecimal data) {
    // make sure the data doesn't have more digits than the precision
    if (data.compareTo(maxValue) > 0 || data.compareTo(minValue) < 0) {
      throw new ValueConverterException("value too large for column");
    }
    this.data = data;
  }
  
  @Override
  public void setData(ByteBuffer byteBuffer) throws InvalidDataException {
    BigDecimal tData;
    if (precision < 3) { /* byte: 1 byte */
      tData = new BigDecimal(byteBuffer.get());
    } else if (precision < 5) { /* short: 2 bytes */
      tData = new BigDecimal(byteBuffer.getShort());
    } else if (precision < 10){  /* int: 4 bytes */
      tData = new BigDecimal(byteBuffer.getInt());
    } else { /* long: 8 bytes */
      tData = new BigDecimal(byteBuffer.getLong());
    }
    setData(tData);
  }
  
  /**
   * @throws ValueConverterException if value has too many digits for the
   * precision
   */
  @Override
  public void setData(String string) {
    setData(new BigDecimal(string).multiply(multiplier));
  }

  /**
   * @throws ValueConverterException if value has too many digits for the
   * precision
   */
  @Override
  public void setData(Object value, ValueConverter meta) {
    if (value == null) {
      setIsNull(true);
    } else {
      BigDecimal val = meta.getBigNumber(value);
      if (val == null) {
        setIsNull(true);
      } else {
        setIsNull(false);
        setData(val.multiply(multiplier));
      }
    }
  }

  @Override
  protected void zeroOutData() {
    data = BigDecimal.ZERO;
  }
  
  @Override
  public final boolean isNeedsEnclosures() {
    return false;
  }
}
