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

import java.lang.Thread.State;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Invoke a method in such a way that it is subject
 * to a timeout. If the method times out, kill the
 * thread in which the method was running, and throw
 * MethodTimedOutException to the caller.
 * 
 * TODO submit this code to be integrated into java.lang.reflect.Method
 * 
 * @author geoffrey.falk@infobright.com
 */
public class TimeoutEnabledMethod {
  
  private final Method method;

  /**
   * Constructor.
   * 
   * @param method The method to be timeout-enabled
   */
  public TimeoutEnabledMethod(Method method) {
    this.method = method;
    method.setAccessible(true);
  }
  
  /**
   * Invoke a method that can be allowed to run for at most
   * a specified time before timing out.
   * 
   * @param millis the timeout in milliseconds
   * @param obj    the object on which to invoke the method
   * @param args   the method arguments
   * @return       the return value from the method, if any.
   *               Returns null for void methods.
   * @throws MethodTimedOutException if the method did not return within the specified timeout period.
   * @throws InterruptedException if the timer was interrupted while waiting for the thread to return.
   * @throws IllegalAccessException if this Method object enforces Java language access control and the underlying method is inaccessible.
   * @throws IllegalArgumentException if the method is an instance method and the specified object argument is not an instance of the class or interface declaring the underlying method (or of a subclass or implementor thereof); if the number of actual and formal parameters differ; if an unwrapping conversion for primitive arguments fails; or if, after possible unwrapping, a parameter value cannot be converted to the corresponding formal parameter type by a method invocation conversion.
   * @throws InvocationTargetException if the underlying method throws an exception.
   * @throws NullPointerException if the specified object is null and the method is an instance method.
   * @throws ExceptionInInitializerError if the initialization provoked by this method fails.
   */
  public Object invoke(long millis, Object obj, Object ... args)
      throws MethodTimedOutException, InterruptedException,
          IllegalAccessException, IllegalArgumentException,
          InvocationTargetException {
    ExecutorThread worker = new ExecutorThread(obj, args);
    worker.start();
    worker.join(millis);
    if (!(State.TERMINATED.equals(worker.getState()))) {
      // thread didn't complete within the timeout
      worker.interrupt();
      worker.join();
      throw new MethodTimedOutException(millis, method, obj, args);
    }
    if (worker.getIllegalAccessException() != null) {
      throw worker.getIllegalAccessException();
    }
    if (worker.getIllegalArgumentException() != null) {
      throw worker.getIllegalArgumentException();
    }
    if (worker.getInvocationTargetException() != null) {
      throw worker.getInvocationTargetException();
    }
    if (worker.getNullPointerException() != null) {
      throw worker.getNullPointerException();
    }
    if (worker.getExceptionInInitializerError() != null) {
      throw worker.getExceptionInInitializerError();
    }
    return worker.getReturnValue();
  }

  private class ExecutorThread extends Thread {

    private final Object obj;
    private final Object[] args;
    
    private IllegalAccessException illegalAccessException = null;
    private IllegalArgumentException illegalArgumentException = null;
    private InvocationTargetException invocationTargetException = null;
    private NullPointerException nullPointerException = null;
    private ExceptionInInitializerError exceptionInInitializerError = null;
    
    private Object returnValue = null;
 
    public ExecutorThread(Object obj, Object[] args) {
      this.obj = obj;
      this.args = args;
    }
    
    public void run() {
      try {
        returnValue = method.invoke(obj, args);
      } catch (IllegalAccessException e) {
        illegalAccessException = e;
      } catch (IllegalArgumentException e) {
        illegalArgumentException = e;
      } catch (InvocationTargetException e) {
        invocationTargetException = e;
      } catch (NullPointerException e) {
        nullPointerException = e;
      } catch (ExceptionInInitializerError e) {
        exceptionInInitializerError = e;
      }   
    }
    
    private IllegalAccessException getIllegalAccessException() {
      return illegalAccessException;
    }

    private IllegalArgumentException getIllegalArgumentException() {
      return illegalArgumentException; 
    }
    
    private InvocationTargetException getInvocationTargetException() {
      return invocationTargetException;
    }
    
    private NullPointerException getNullPointerException() {
      return nullPointerException;
    }
    
    private Error getExceptionInInitializerError() {
      return exceptionInInitializerError;
    }
    
    private Object getReturnValue() {
      return returnValue;
    }
  }
}
