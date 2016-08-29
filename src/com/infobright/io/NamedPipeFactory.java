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

import com.infobright.logging.EtlLogger;


class NamedPipeFactory {
 
  private final OSType osType;
  
  private final ClientProxy proxy;
  
  private final LinuxHelper linuxHelper;
  private final WindowsHelper windowsHelper;
  
  private NamedPipeFactory(String osName, ClientProxy proxy) {
    osType = new OSType(osName);
    this.proxy = proxy;
    linuxHelper = new LinuxHelper();
    windowsHelper = new WindowsHelper();
  }
  
  /**
   * Constructor to use for remote or local connection.
   * @param proxy If null then use local connection
   * @throws IOException
   */
  public NamedPipeFactory(ClientProxy proxy) throws IOException {
    this(
        proxy == null ? System.getProperty("os.name") : proxy.getOSName(),
        proxy);
  }
  
  /**
   * Constructor to use for local connection.
   */
  public NamedPipeFactory() {
    this(System.getProperty("os.name"), null);
  }

  private class LinuxHelper {

    NamedPipe createPipe(String pipeName)
        throws IOException {
      if (proxy == null) { // local connection
        return new LinuxNamedPipe(pipeName, true);
      } else {
        return new RemoteNamedPipe(pipeName, proxy);
      }
    }
  }
  
  private class WindowsHelper {

    NamedPipe createServerPipe(String pipeName, boolean writeAllowed, boolean readAllowed, int bufsize, long timeout)
        throws IOException {
      if (proxy == null) { // local connection
        return new WindowsNamedPipe(pipeName, writeAllowed, readAllowed, bufsize, timeout);
      } else {
        return new RemoteNamedPipe(pipeName, proxy);
      }
    }

    NamedPipe createClientPipe(String pipeName) throws IOException {
      if (proxy == null) { // local connection
        return new WindowsNamedPipe(pipeName);
      } else {
        return new RemoteNamedPipe(pipeName, proxy);
      }
    }
  }


  NamedPipe createServer(String pipeName) throws IOException {
    return createServer(pipeName, true, true, 4096, 10000);
  }
  
  NamedPipe createServer(String pipeName, boolean writeAllowed, boolean readAllowed, int bufsize, long timeout) throws IOException {
    NamedPipe namedPipe;
    if (osType.isUnix()) {
      namedPipe = linuxHelper.createPipe(pipeName);
    } else if (osType.isWindows()) {
      namedPipe = windowsHelper.createServerPipe(pipeName, writeAllowed, readAllowed, bufsize, timeout);
    } else {
      throw new UnsupportedOperationException("Unsupported platform");
    }
    return namedPipe;
  }

  NamedPipe createClient(String pipeName) throws IOException {
    NamedPipe namedPipe;
    if (osType.isUnix()) {
      namedPipe = linuxHelper.createPipe(pipeName);
    } else if (osType.isWindows()) {
      namedPipe = windowsHelper.createClientPipe(pipeName);
    } else {
      throw new UnsupportedOperationException("Unsupported platform");
    }
    return namedPipe;
  }
  
  public String getNativePipeName(String name) {
    if (osType.isUnix()) {
      return LinuxNamedPipe.getNativeName(name);
    } else if (osType.isWindows()) {
      return WindowsNamedPipe.getNativeName(name);
    } else {
      throw new UnsupportedOperationException("Unsupported platform");
    }
  }

  public PipeCallStrategy getStrategy(EtlLogger logger) {
    if (osType.isUnix()) {
      return new UnixPipeCallStrategy(proxy, logger);
    } else if (osType.isWindows()) {
      return new WindowsPipeCallStrategy(proxy, logger);
    } else {
      throw new UnsupportedOperationException("Unsupported platform");
    }
  }
}
