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

package com.infobright.reflect;

import java.lang.reflect.Method;

/**
 * 
 * @author geoffrey.falk@infobright.com
 *
 */
public class MethodTimedOutException extends Exception {

  private static final long serialVersionUID = 1L;
  
  private final long timeout;
  private final Method method;
  private final Object obj;
  private final Object[] args;

  MethodTimedOutException(long timeout, Method method, Object obj, Object[] args) {
    super("call to " + obj.getClass().getName() + "." + method.getName() +
        " timed out after " + timeout + "ms");
    this.timeout = timeout;
    this.method = method;
    this.obj = obj;
    this.args = args;
  }

  public long getTimeout() {
    return timeout;
  }

  public Method getMethod() {
    return method;
  }

  public Object getObj() {
    return obj;
  }

  public Object[] getArgs() {
    return args;
  }
}
