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


/**
 * <p>Abstract strategy for obtaining a reference to the named
 * pipe on different platforms. On Unix, the FIFO is created
 * before executing the LOAD DATA INFILE statement.
 * the Windows named pipe is obtained after executing the
 * statement. Usage example:</p>
 * 
 * <pre>
 * // try to get output stream before starting load
 * NamedPipeOutputStream os = strategy.beforeExecuteCreate(....);
 * strategy.setupForLoad(statement, ......);
 * statement.execute("set @bh_dataformat=".....");
 * statement.execute("LOAD DATA INFILE .....");
 * if (os == null) {
 *   // else get it afterward
 *   os = strategy.afterExecuteCreate(....);
 * }
 * </pre>
 * 
 * @author geoffrey.falk@infobright.com
 */
interface PipeCallStrategy {

  /**
   * Execute any platform-specific SQL to set up for the LOAD
   * DATA INFILE command. for example, on Windows, need to set
   * bh_pipemode and bh_timeout.
   * 
   * @param statement
   * @param params array of parameters (timeout, etc.)
   * @throws SQLException
   */
  void setupForLoad(Statement statement, Object[] params)
      throws SQLException;
  
  /**
   * Attempt to obtain a reference to the NamedPipeOutputStream
   * before starting the load command.
   * 
   * @param pipeName
   * @param charset
   * @param logger
   * @return null if it doesn't work that way on this platform
   * 
   * @throws IOException
   */
  NamedPipeOutputStream beforeExecuteCreate(String pipeName)
      throws IOException;
 
  /**
   * Attempt to obtain a reference to the NamedPipeOutputStream
   * after starting the load command.
   * 
   * @param pipeName
   * @param charset
   * @param logger
   * @return null if it doesn't work that way on this platform
   * 
   * @throws IOException
   */
  NamedPipeOutputStream afterExecuteCreate(String pipeName)
      throws IOException;
  
}
