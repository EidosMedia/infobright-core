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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;

//import org.apache.log4j.Logger;

import com.infobright.util.ProcessUtil;

/**
 * Named pipe that uses a FIFO on the Linux platform.
 * Also works on Solaris and many other Unices without change.
 */
class LinuxNamedPipe extends NamedPipe {

  private static final String PIPE_MODE = "644";
  private static final String CHMOD_CMD = "chmod";
  private static final String MKFIFO_CMD = "mkfifo";
  
  private FileInputStream inputStream;
  private OutputStream outputStream;
  //public static Logger log = Logger.getLogger(LinuxNamedPipe.class);
 
  final static String getNativeName(String name) {
    return String.format("/tmp/%s", name);
  }

  LinuxNamedPipe(String name, boolean createFIFO) throws IOException {
    super(getNativeName(name));
    String pipeName = getPipeName();
    if (createFIFO) {
      if (!(new File(pipeName)).exists()) {
        // create the FIFO in the filesystem
        ProcessUtil.runCommand(new String[] {MKFIFO_CMD, pipeName});
        // change mode to allow mysqld process to read the pipe
        ProcessUtil.runCommand(new String[] {CHMOD_CMD, PIPE_MODE, pipeName});
        //log.debug(String.format("created named pipe %s for server", pipeName));
      } else {
        throw new RuntimeException("Can't create named pipe \"" + pipeName +
        "\" as it already exists.");
      }
    }
  }
  
  @Override
  public void connect() throws IOException {
  }
  
  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    if (inputStream == null) {
      inputStream = new FileInputStream(getPipeName());
    }
    return inputStream.read(bytes, offset, length);
  }

  @Override
  public int write(byte[] bytes, int offset, int length) throws IOException {
    ////log.trace(String.format("writing %d bytes to named pipe %s", length, pipeName));
    if (outputStream == null) {
      outputStream = new FileOutputStream(getPipeName());
    }
    outputStream.write(bytes, offset, length);
    return length;
  }
  
  @Override
  public void close() throws IOException {
    if (outputStream == null) {
      // this ensures that we can terminate an empty load.
      // Note: Blocks if there are no readers!
      outputStream = new FileOutputStream(getPipeName()); 
    }
    //log.debug(String.format("closing output stream named pipe %s", getPipeName()));
    outputStream.close();
    if (inputStream != null) {
      //log.debug(String.format("closing input stream named pipe %s", getPipeName()));
      inputStream.close();
    }
    //log.debug("deleting FIFO " + getPipeName());
    File file = new File(getPipeName());
    if (file.exists()) {
      file.delete();
    }
    outputStream = null;
    inputStream = null;
  }
}
