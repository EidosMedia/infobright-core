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

package com.infobright.etl.agent;

import com.infobright.io.InfobrightNamedPipeLoader;
import com.infobright.logging.ConsoleEtlLogger;
import com.infobright.logging.ConsoleEtlLogger.Level;


/**
 * Command line arguments.
 * 
 * @author gfalk
 */
class CLArgs {
  
  public static final ConsoleEtlLogger.Level DEFAULT_LOGLEVEL = Level.INFO;

  private static final String PORT_FLAG = "-p";
  private static final String LOGLEVEL_FLAG = "-l";
  
  private final int port;
  private final ConsoleEtlLogger.Level logLevel;
  
  CLArgs(String[] args) throws UsageException {
    try {
      this.port = parsePort(args);
      this.logLevel = parseLogLevel(args);
    } catch (Exception e) {
      throw new UsageException();
    }
  }
  
  /**
   * 
   * @param args
   * @return
   * @throws UsageException
   * @throws NumberFormatException
   */
  private int parsePort(String[] args) throws UsageException {
    int port2 = InfobrightNamedPipeLoader.AGENT_DEFAULT_PORT;
    for (int i = 0; i < args.length; i++) {
      if (PORT_FLAG.equals(args[i])) {
        if (i < args.length - 1) {
          port2 = Integer.parseInt(args[i + 1]);
          break;
        } else {
          throw new UsageException();
        }
      }
    }
    return port2;
  }
  
  /**
   * 
   * @param args
   * @return
   * @throws UsageException
   * @throws NumberFormatException
   */
  private ConsoleEtlLogger.Level parseLogLevel(String[] args)
        throws UsageException {
    ConsoleEtlLogger.Level level = DEFAULT_LOGLEVEL;
    for (int i = 0; i < args.length; i++) {
      if (LOGLEVEL_FLAG.equals(args[i])) {
        if (i < args.length - 1) {
          level = ConsoleEtlLogger.Level.valueOf(args[i + 1].toUpperCase());
          break;
        } else {
          throw new UsageException();
        }
      }
    }
    return level;
  }

  int getPort() {
    return port;
  }
  
  ConsoleEtlLogger.Level getLogLevel() {
    return logLevel;
  }
}
