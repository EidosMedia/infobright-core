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

import com.infobright.logging.EtlLogger;
import java.sql.Statement;

class WindowsPipeCallStrategy implements PipeCallStrategy {

  private final ClientProxy proxy;
  private final EtlLogger logger;

  WindowsPipeCallStrategy(ClientProxy proxy, EtlLogger logger) {
    this.proxy = proxy;
    this.logger = logger;
  }
  
  /*
   * (non-Javadoc)
   * @see com.infobright.io.PipeCallStrategy#setupForLoad(java.sql.Statement, java.lang.Object[])
   */
  //@Override
  public void setupForLoad(Statement statement, Object[] params)
      throws SQLException {

    if (params.length != 1) {
      throw new IllegalArgumentException("Expected timeout parameter");
    }
    
    int timeout = (Integer) params[0];
    
    String setupSql = "set @bh_pipemode='" + PipeMode.SERVER.getBhPipeMode() + "';";
    if (logger != null) logger.debug(String.format("exec sql: %s", setupSql));
    statement.execute(setupSql);

    setupSql = "set @bh_timeout=" + timeout + ";";
    if (logger != null) logger.debug(String.format("exec sql: %s", setupSql));
    statement.execute(setupSql);
  }

  /*
   * (non-Javadoc)
   * @see com.infobright.io.PipeCallStrategy#beforeExecuteCreate(java.lang.String, java.nio.charset.Charset, com.infobright.logging.EtlLogger)
   */
  //@Override
  public NamedPipeOutputStream beforeExecuteCreate(String pipeName) throws IOException {
    return null;
  }
  
  /*
   * (non-Javadoc)
   * @see com.infobright.io.PipeCallStrategy#afterExecuteCreate(java.lang.String, java.nio.charset.Charset, com.infobright.logging.EtlLogger)
   */
  //@Override
  public NamedPipeOutputStream afterExecuteCreate(String pipeName) throws IOException {
    return new NamedPipeOutputStream(pipeName, proxy, logger);
  }

  /**
   * bh_pipemode supported by Windows version of Brighthouse
   */
  private enum PipeMode {
    CLIENT("client"),
    SERVER("server");
    
    private String bhPipeMode;
    PipeMode(String bhPipeMode) {
      this.bhPipeMode = bhPipeMode;
    }
    public String getBhPipeMode() {
      return bhPipeMode;
    }
  }
}