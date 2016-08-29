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

import com.infobright.logging.EtlLogger;

/**
 * An output stream which writes to a named pipe.
 */
public class NamedPipeOutputStream extends OutputStream {
  
  private NamedPipe namedPipe = null;
  private byte[] oneByte = new byte[1];
  private final EtlLogger logger;
  
  public NamedPipeOutputStream(String pipeName) throws IOException {
    this(pipeName, null, null);
  }
  
  /**
   * Creates and returns an OutputStream which can write to the named pipe.
   * @param pipeName 
   * @throws IOException 
   */
  public NamedPipeOutputStream(String pipeName, ClientProxy proxy) throws IOException {
    this(pipeName, proxy, null);
  }

  /**
   * Creates and returns an OutputStream which can write to the named pipe.
   * @param pipeName
   * @param proxy The client proxy to use (or null)
   * @param logger 
   * @throws IOException 
   */
  public NamedPipeOutputStream(String pipeName, ClientProxy proxy, EtlLogger logger) throws IOException {
    this.logger = logger;
    if (this.logger != null) {
      this.logger.debug(String.format("creating named pipe client \"%s\"", pipeName));
    }
    namedPipe = new NamedPipeFactory(proxy).createClient(pipeName); 
    namedPipe.connect();
    if (logger != null) {
      logger.debug("NamedPipeFactory.createClient(name) returned " + namedPipe);
    }
  }
  
  /** {@inheritDoc}
   * @see java.io.OutputStream#write(int)
   */
  @Override
  public void write(int b) throws IOException {
    oneByte[0] = (byte) b;
    write(oneByte, 0, 1);
  }

  /** {@inheritDoc}
   * @see java.io.OutputStream#write(byte[], int, int)
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    int bytesWritten = 0;
    
    bytesWritten = namedPipe.write(b, off, len);
    
    if (bytesWritten != len) {
      throw new IOException(String.format("Meant to write %d bytes but wrote %d bytes", len, bytesWritten));
    }
  }
  
  /** {@inheritDoc}
   * @see java.io.OutputStream#close()
   */
  public void close() throws IOException {
    namedPipe.close(); // terminates the load
    namedPipe = null;
  }
  
  /** {@inheritDoc}
   * @see java.lang.Object#finalize()
   */
  @Override
  protected void finalize() throws Throwable {
    if (namedPipe != null) {
      close();
    }
  }
}
