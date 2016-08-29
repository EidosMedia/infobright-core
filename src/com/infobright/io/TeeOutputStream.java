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

package com.infobright.io;

import java.io.IOException;
import java.io.OutputStream;

public class TeeOutputStream extends OutputStream {

  private final OutputStream os1, os2;
  
  public TeeOutputStream(OutputStream os1, OutputStream os2) {
    this.os1 = os1;
    this.os2 = os2;
  }
  
  @Override
  public void write(int b) throws IOException {
    Exception e1 = null;
    Exception e2 = null;
    try {
      os1.write(b);
    } catch (Exception e) {
      e1 = e;
    }
    try {
      os2.write(b);
    } catch (Exception e) {
      e2 = e;
    }
    rethrow(e1);
    rethrow(e2);
  }

  @Override
  public void flush() throws IOException {
    Exception e1 = null;
    Exception e2 = null;
    try {
      os1.flush();
    } catch (Exception e) {
      e1 = e;
    }
    try {
      os2.flush();
    } catch (Exception e) {
      e2 = e;
    }
    rethrow(e1);
    rethrow(e2);
  }
  
  @Override
  public void close() throws IOException {
    Exception e1 = null;
    Exception e2 = null;
    try {
      os1.close();
    } catch (Exception e) {
      e1 = e;
    }
    try {
      os2.close();
    } catch (Exception e) {
      e2 = e;
    }
    rethrow(e1);
    rethrow(e2);
  }
  
  /**
   * 
   * @param e IOException or RuntimeException
   * @throws IOException
   * @throws RuntimeException
   */
  private void rethrow(Exception e) throws IOException {
    if (e != null) {
      if (e instanceof IOException) {
        throw (IOException) e;
      } else {
        throw (RuntimeException) e;
      }
    }
  }
}
