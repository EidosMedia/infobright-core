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

import java.util.HashSet;
import java.util.Set;

/**
 * Keeps track of loader instances.
 * 
 * Registers a shutdown hook to ensure that all running
 * JDBC statements are cancelled when the JVM shuts down.
 * Note: Shutdown hook may not run if the JVM process is
 * terminated with "kill -9". But people ought to know
 * that that is dangerous.
 * 
 * @author geoffrey.falk@infobright.com
 */
class LoaderInstanceTracker {
  
  private static final Set<InfobrightNamedPipeLoader> instances
        = new HashSet<InfobrightNamedPipeLoader>();
  
  private static long instanceCount = 0L;
  
  static class ShutdownHook extends Thread {
    public void run() {
      InfobrightNamedPipeLoader.setShuttingDown();
      // need to synchronize in case a finalizer runs and
      // tries to remove it from the map
      synchronized (LoaderInstanceTracker.class) {
        for (InfobrightNamedPipeLoader npab : instances) {
          try {
            npab.killQuery();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    }
  }
  
  static {
    Runtime.getRuntime().addShutdownHook(new ShutdownHook());
  }

  static synchronized long register(InfobrightNamedPipeLoader npab) {
    instances.add(npab);
    return instanceCount++;
  }

  static synchronized void unregister(InfobrightNamedPipeLoader npab) {
    instances.remove(npab);
  }
}
