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

//import org.apache.log4j.Logger;

/**
 * JNI bridge to Windows named pipe functions.
 */
class WindowsNamedPipe extends NamedPipe {
  
  private final static int MAX_RETRIES = 10;
  private final static int TIME_BETWEEN_RETRIES = 200;
  
  static {
    // Note: This section may be reached even on Unix, if the
    // IB server is Linux. Detect this case and skip if not
    // applicable.
    if (new OSType().isWindows()) {
      // load the jni library from the path if possible, otherwise enumerate the path and try loading it 
      // explicitly from that.  this allows us to pick up any runtime or dynamic changes to the java.library.path
      // since those are ignored.  
      UnsatisfiedLinkError error = null;
      try {
        System.loadLibrary("infobright_jni");
      }
      catch (UnsatisfiedLinkError ule) {
        error = ule;
        String[] paths = System.getProperty("java.library.path").split(";");
        String curdir = System.getProperty("user.dir");
        for (String path : paths) {
          try {
            if (!path.contains(":\\")) {
              path = curdir + "/" + path;
            }
            System.load(path + "/infobright_jni.dll"); 
            error = null; 
            break; 
          }
          catch (UnsatisfiedLinkError e) {} // ignore
        }
      }
      if (error != null) {
        throw error;
      }
    }
  }
  
  private long handle; 

  //public static Logger log = Logger.getLogger(WindowsNamedPipe.class);
  
  /**
   * create pipe as client.
   * 
   * @param name
   * @param isServer
   */
  WindowsNamedPipe(String name) throws IOException {
    super(getNativeName(name));
    IOException ioe = null;
    int retries = 0;
    while (retries < MAX_RETRIES) {
      try {
        handle = clientCreate(getPipeName());
        return;
      } catch (IOException ex) {
        ioe = ex;
      }
      try {
        Thread.sleep(TIME_BETWEEN_RETRIES);
      } catch (InterruptedException ie) {}
      retries++;
    }
    // exceeded max retries
    IOException ioe2 = new IOException(ioe.getMessage()
          + ": Confirm that table engine=brighthouse!!!");
    ioe2.setStackTrace(ioe.getStackTrace());
    throw ioe2;
  }

  /**
   * Create pipe as server.
   * 
   * @param name
   * @param isServer
   * @param writeAllowed
   * @param readAllowed
   * @param bufsize
   * @param timeout
   * @throws IOException
   */
  WindowsNamedPipe(String name, boolean writeAllowed, boolean readAllowed, int bufsize, long timeout) throws IOException {
    super(getNativeName(name));
    String pipeName = WindowsNamedPipe.getNativeName(name);
    handle = WindowsNamedPipe.serverCreate(pipeName, writeAllowed, readAllowed, bufsize, timeout);
  }
  
  final static String getNativeName(String name) {
    return String.format("\\\\.\\pipe\\%s", name);
  }
  
  @Override
  public void connect() throws IOException {
  //  if (handle == 0) {
  //    throw new IOException("the named pipe has been closed");
  //  }
  //  serverConnect(handle);
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    if (handle == 0) {
      throw new IOException("the named pipe has been closed");
    }
    return fileRead(handle, bytes, offset, length);
  }
  
  @Override
  public int write(byte[] bytes, int offset, int length) throws IOException {
    ////log.trace(String.format("writing %d bytes to named pipe %s", length, pipeName));
    if (handle == 0) {
      throw new IOException("the named pipe has been closed");
    }
    return fileWrite(handle, bytes, offset, length);
  }
  
  @Override
  public void close() throws IOException {
    if (handle != 0) {
      fileClose(handle);
      handle = 0;
    }
  }
  
  private static native long serverCreate(String name, boolean writeAllowed, boolean readAllowed, int bufsize, long timeout) throws IOException;
  private static native long clientCreate(String name) throws IOException;

  private static native int fileRead(long handle, byte[] bytes, int offset, int length) throws IOException;
  private static native int fileWrite(long handle, byte[] bytes, int offset, int length) throws IOException;
  private static native void fileClose(long handle) throws IOException;

}
