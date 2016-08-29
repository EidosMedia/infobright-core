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

package com.infobright.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ProcessUtil {
  
  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  /**
   * take all data from an input stream and put it in a String.
   * @param inputStream
   * @return
   */
  private static String getCmdOutput(InputStream inputStream) {
    StringBuffer buf = new StringBuffer();
    BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
    try {
      String line = in.readLine();
      while (line != null) {
        buf.append(line);
        buf.append(LINE_SEPARATOR);
      }
    } catch (IOException e) {
      return "Operation failed";
    }
    return buf.toString();
  }

  /**
   * Run a system command.
   * 
   * @param commandWithArgs the command to run, with arguments
   * @throws IOException if the command failed
   */
  public static void runCommand(String[] commandWithArgs) throws IOException {
    Process proc = Runtime.getRuntime().exec(commandWithArgs);
    int status;
    try {
      status = proc.waitFor();
    } catch (InterruptedException inte) {
      String cmdOutput = getCmdOutput(proc.getErrorStream());
      throw new IOException(commandWithArgs[0] + " interrupted: " + cmdOutput);
    }
    if (status != 0) {
      String cmdOutput = getCmdOutput(proc.getErrorStream());
      throw new IOException(commandWithArgs[0] + " failed: " + cmdOutput);
    }
  }
}
