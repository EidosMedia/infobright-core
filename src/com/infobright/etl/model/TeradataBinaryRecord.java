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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.List;

import com.infobright.etl.model.ValueConverter;
import com.infobright.etl.model.ValueConverterException;
import com.infobright.etl.model.datatype.AbstractColumnType;
import com.infobright.logging.EtlLogger;

class TeradataBinaryRecord extends BrighthouseRecord {
 
  private static final int BUFFER_SIZE = 100000; // TODO is this long enough?
  
  private final ByteBuffer byteBuffer;

  private final NullIndicator nullind;
  
  @SuppressWarnings("unused")
  private final EtlLogger logger;

  TeradataBinaryRecord(List<AbstractColumnType> columns, Charset charset) {
    this(columns, charset, null);
  }

  TeradataBinaryRecord(List<AbstractColumnType> columns, Charset charset, EtlLogger logger) {
    super(columns, charset);
    this.logger = logger;
    nullind = new NullIndicator(columns.size()); 
    byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
  }
  
  @Override
  public int size() {
    return columns.size();
  }
  
  @Override
  public void writeTo(OutputStream os) throws IOException {
  
    /*
     * skip the first 2 bytes representing the length of the record
     * until we can figure out how long the record should be
     */ 
    
    byteBuffer.rewind();
    short len = 0;
    byteBuffer.putShort(len);
    int startingPosition = byteBuffer.position();

    /*
     * Now put out the null indicator bytes
     */
    nullind.reset();
    int colidx = -1;
    for (AbstractColumnType col : columns) {
      colidx++;
      if (col.getIsNull()) {
        nullind.setToNull(colidx);
      }
    }
    byteBuffer.put(nullind.getBytes());
  
    /*
     * Now write out each column data in its native format
     */
    
    for (AbstractColumnType col : columns) {
        col.getData(byteBuffer);
    }
    
    /*
     * Now write the real length at the beginning of the buffer
     */
    int currentPosition = byteBuffer.position();
    len = (short) (currentPosition - startingPosition);
    byteBuffer.rewind();
    byteBuffer.putShort(len);
    byteBuffer.position(currentPosition);

    /*
     * write the data to the output stream
     */
    byte[] data = byteBuffer.array();
    os.write(data, 0, len + 2);
  }

  @Override
  public void setData(int colidx, Object value, ValueConverter meta) throws ValueConverterException {
    AbstractColumnType col = columns.get(colidx);
    if (value == null) {
      col.setIsNull(true);
    } else {
      col.setIsNull(false);
      col.setData(value, meta);
    }
  }
}
