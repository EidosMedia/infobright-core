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

class RemoteNamedPipe extends NamedPipe {

  private final ClientProxy proxy;
  
  RemoteNamedPipe(String pipeName, ClientProxy proxy) {
    super(pipeName);
    this.proxy = proxy;
  }

  @Override
  public void connect() throws IOException {
    proxy.connect(getPipeName());
  }

  @Override
  public void close() throws IOException {
    proxy.disconnect();
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    throw new UnsupportedOperationException("Read not supported");
  }

  @Override
  public int write(byte[] bytes, int offset, int length) throws IOException {
    proxy.write(bytes, offset, length);
    return length; // FIXME
  }

}
