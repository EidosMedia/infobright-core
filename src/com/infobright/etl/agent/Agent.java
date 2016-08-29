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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;

import com.infobright.io.AgentThread;
import com.infobright.logging.ConsoleEtlLogger;
import com.infobright.logging.EtlLogger;

/**
 * An agent that runs on the Infobright server and
 * listens for connections from a remote ETL server
 * running the infobright-core connector. 
 * 
 * @author GFalk
 *
 */
public class Agent {

  private final int port;
  
  private final EtlLogger logger;
  
  private final Set<AgentThread> workers;
  
  private static long nextWorkerID = 0L;
  
  /**
   * @param args
   */
  public static void main(String[] args) throws IOException, UsageException {
    CLArgs clargs = new CLArgs(args);
    int port2 = clargs.getPort();
    ConsoleEtlLogger.Level logLevel = clargs.getLogLevel();
    EtlLogger logger2 = new ConsoleEtlLogger(logLevel);
    new Agent(port2, logger2).execute();
  }
  
  private Agent(int port, EtlLogger logger) {
    this.port = port;
    this.logger = logger;
    this.workers = new HashSet<AgentThread>();
    new Reaper().start();
    logger.info("Infobright remote load agent started on port " + port);
  }
  
  private void execute() {
    try {
      ServerSocket ss = new ServerSocket(port);
      while (true) {
        AgentThread worker = new AgentThread(ss.accept(), ++nextWorkerID, logger);
        synchronized (workers) {
          workers.add(worker);
        }
        worker.start();
      }
    } catch (IOException e) {
      logger.error("Infobright remote load agent failed", e);
      System.exit(1);
    }
  }

  private class Reaper extends Thread {
    
    private final int REAP_INTERVAL = 1000;

    private Reaper() {
      setDaemon(true);
    }
    
    public void run() {
      while (true) {
        try {
          Thread.sleep(REAP_INTERVAL);
        } catch (InterruptedException e) {
          // server exited or killed
          return;
        }
        Set<AgentThread> workersCopy;
        synchronized (workers) {
          // create private copy to avoid concurrent modification
          workersCopy = new HashSet<AgentThread>(workers);
        }
        for (AgentThread worker : workersCopy) {
          if (State.TERMINATED == worker.getState()) {
            try {
              worker.join();
              synchronized (workers) {
                workers.remove(worker);
              }
              logger.debug("Reaped worker [" + worker.getWorkerId() + "]");
            } catch (InterruptedException e) {
              return;
            }
          }
        }
      }
    }
  }
}
