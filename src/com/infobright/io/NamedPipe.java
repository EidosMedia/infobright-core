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

/**
 * Bridge to named pipe functions for different platforms.
 */
abstract class NamedPipe {

  /**
   * The full pipe name (example: /tmp/foo.pipe on Linux)
   */
  private final String pipeName;

  protected NamedPipe(String name) {
    pipeName = name;
  }
  
  public abstract void connect() throws IOException;
    
  public abstract int read(byte[] bytes, int offset, int length) throws IOException;
  
  public abstract int write(byte[] bytes, int offset, int length) throws IOException;
  
  public abstract void close() throws IOException;
  
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    close();
  }

  protected String getPipeName() {
    return pipeName;
  }
}
