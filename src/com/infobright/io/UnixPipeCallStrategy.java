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
import java.sql.SQLException;
import java.sql.Statement;

import com.infobright.logging.EtlLogger;

class UnixPipeCallStrategy implements PipeCallStrategy {

  private final EtlLogger logger;
  private final ClientProxy proxy;

  public UnixPipeCallStrategy(ClientProxy proxy, EtlLogger logger) {
    this.proxy = proxy;
    this.logger = logger;
  }

  /*
   * (non-Javadoc)
   * @see com.infobright.io.PipeCallStrategy#beforeExecuteCreate(java.lang.String, java.nio.charset.Charset, com.infobright.logging.EtlLogger)
   */
  public NamedPipeOutputStream beforeExecuteCreate(String pipeName)
      throws IOException {
    return new NamedPipeOutputStream(pipeName, proxy, logger);
  }
  
  /*
   * (non-Javadoc)
   * @see com.infobright.io.PipeCallStrategy#afterExecuteCreate(java.lang.String, java.nio.charset.Charset, com.infobright.logging.EtlLogger)
   */
  public NamedPipeOutputStream afterExecuteCreate(String pipeName) throws IOException {
    return null;
  }

  /*
   * (non-Javadoc)
   * @see com.infobright.io.PipeCallStrategy#setupForLoad(java.sql.Statement, java.lang.Object[])
   */
  public void setupForLoad(Statement statement, Object[] params)
      throws SQLException {
  }

}
