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

class NullIndicator {

  byte[] bytes;

  public NullIndicator(int numColumns) {
    // one byte is required per 8 columns, or fraction thereof
    int numBytes = ceilXOver8(numColumns);
    bytes = new byte[numBytes];
  }

  public boolean isNull(int columnIdx) {
    int byteIdx = columnIdx / 8;
    int bitIdx = columnIdx % 8;
    int val = bytes[byteIdx];
    val = val >> (7 - bitIdx);
    return ((val & 0x01) == 1);
  }
  
  public void setToNull(int columnIdx) {
    int byteIdx = columnIdx / 8;
    int bitIdx = columnIdx % 8;
    int bitSet = 0x01 << (7 - bitIdx);
    bytes[byteIdx] = (byte) (bytes[byteIdx] | bitSet);
  }
  
  public void reset() {
    for (int i = 0; i < bytes.length; i++)
      bytes[i] = 0;
  }

  public byte[] getBytes() {
    return bytes;
  }
  
  /**
   * Computes ceil(x/8) using integer arithmetic.
   * 
   * @param x numerator (must be >= 0)
   * @return
   */
  private static int ceilXOver8(int x) {
    assert x >= 0;
    return (x == 0) ? 0 : ((x - 1) / 8) + 1;
  }
}
