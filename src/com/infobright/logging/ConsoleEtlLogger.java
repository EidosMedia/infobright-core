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

package com.infobright.logging;

import java.util.Date;

public class ConsoleEtlLogger implements EtlLogger {

  public enum Level {
    ALL(-1),
    TRACE(0),
    DEBUG(1),
    INFO(2),
    WARN(3),
    ERROR(4),
    FATAL(5),
    OFF(6);
    
    private final int iLevel;
    
    private Level(int iLevel) {
      this.iLevel = iLevel;
    }
    
    private boolean doLog(Level level) {
      return this.iLevel <= level.iLevel;
    }
  }
  
  private final Level level;
  
  public ConsoleEtlLogger(Level level) {
    this.level = level;
  }
  
  private void print(String sLevel, String message) {
    System.out.println(sLevel + ": " + new Date() + ": " + message);
  }
  
  //@Override
  public void debug(String s) {
    if (level.doLog(Level.DEBUG)) {
      print("DEBUG", s);
    }
  }

  //@Override
  public void error(String s, Throwable cause) {
    if (level.doLog(Level.ERROR)) {
      error(s);
      cause.printStackTrace();
    }
  }

  //@Override
  public void fatal(String s) {
    if (level.doLog(Level.FATAL)) {
      print("FATAL", s);
    }
  }
  
  //@Override
  public void error(String s) {
    if (level.doLog(Level.ERROR)) {
      print("ERROR", s);
    }
  }

  //@Override
  public void info(String s) {
    if (level.doLog(Level.INFO)) {
      print("INFO", s);
    }
  }

  //@Override
  public void trace(String s) {
    if (level.doLog(Level.TRACE)) {
      print("TRACE", s);
    }
  }

  //@Override
  public void warn(String s) {
    if (level.doLog(Level.WARN)) {
      print("WARN", s);
    }
  }

}
