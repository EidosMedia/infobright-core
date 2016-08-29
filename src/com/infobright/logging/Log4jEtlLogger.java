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

import org.apache.log4j.Logger;

public class Log4jEtlLogger implements EtlLogger {

  Logger logger = null;
  public Log4jEtlLogger(Class<?> clazz) {
    logger = Logger.getLogger(clazz);
  }
  
  //@Override
  public void debug(String s) {
    logger.debug(s);
  }

  //@Override
  public void error(String s, Throwable cause) {
    logger.error(s, cause);
  }

  //@Override
  public void error(String s) {
    logger.error(s);
  }

  //@Override
  public void info(String s) {
    logger.info(s);
  }

  //@Override
  public void trace(String s) {
    logger.debug(s); // TODO: the version of log4j kettle 2.5.1 has does not have logger.trace()
  }

  //@Override
  public void warn(String s) {
    logger.warn(s);
  }

  //@Override
  public void fatal(String s) {
    logger.fatal(s);
  }

}
