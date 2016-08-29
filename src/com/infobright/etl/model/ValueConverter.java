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
import java.util.Date;

/**
 * Converts objects to various types, doing the "obvious" conversions.
 * 
 * There is one instance for each type to be converted.
 * 
 * @author gfalk
 */
public interface ValueConverter {

  /** Convert the supplied data to a String */
  String getString(Object object) throws ValueConverterException;

  /** convert the supplied data to a binary string representation (for writing text) */
  byte[] getBinaryString(Object object) throws ValueConverterException;
  
  /** Convert the supplied data to a Number */
  Double getNumber(Object object) throws ValueConverterException;

  /** Convert the supplied data to a BigNumber */
  BigDecimal getBigNumber(Object object) throws ValueConverterException;

  /** Convert the supplied data to an Integer*/
  Long getInteger(Object object) throws ValueConverterException;

  /** Convert the supplied data to a Date */
  Date getDate(Object object) throws ValueConverterException;

  /** Convert the supplied data to a Boolean */
  Boolean getBoolean(Object object) throws ValueConverterException;

  /** Convert the supplied data to binary data */
  byte[] getBinary(Object object) throws ValueConverterException;

}
