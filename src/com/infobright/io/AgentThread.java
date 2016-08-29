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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.sql.SQLException;

import com.infobright.logging.EtlLogger;

public class AgentThread extends Thread {

  static final String OK_MSG = "ok";
  static final String FAIL_MSG = "fail";

  private final long id;
  
  private final Socket socket;
  
  private final DataInputStream in;
  private final DataOutputStream out;
  
  private final EtlLogger logger;
  
  public AgentThread(Socket socket, long id, EtlLogger logger) throws IOException {
    this.socket = socket;
    this.in = new DataInputStream(socket.getInputStream());
    this.out = new DataOutputStream(socket.getOutputStream());
    this.logger = logger;
    this.id = id;
  }

  public long getWorkerId() {
    return id;
  }

  public void run() {
    OutputStream outStr = null;
    InetAddress clientAddr = socket.getInetAddress();
    String clientInfo = clientAddr.getHostName()
        + " (" + clientAddr.getHostAddress() + ")";
    try {
      if (logger != null) {
        logger.info("[" + id + "] Client connection received from " + clientInfo);
      }
      out.writeUTF(System.getProperty("os.name"));
      String pipeName = in.readUTF();
      if (logger != null) logger.debug("[" + id + "] Using pipeName \"" + pipeName + "\"");

      try {
        outStr = createOutputStream(pipeName);
      } catch (Exception e) {
        out.writeUTF(FAIL_MSG);
        throw e;
      }
      
      // tell the client that the pipe is created
      out.writeUTF(OK_MSG);
      
      if (logger != null) logger.debug("[" + id + "] Created output stream");
      byte[] oneByte = new byte[1];
      
      long counter = 0L;
      
      int data = in.read();
      while (data != -1) {
        oneByte[0] = (byte) data;
        outStr.write(oneByte);
        data = in.read();
        counter++;
      }
      
      if (logger != null) {
        logger.info("[" + id + "] wrote " + counter + " bytes to the pipe");
        logger.info("[" + id + "] Connection from " + clientInfo + " closing");
      }
      socket.close();
    } catch (Exception e) {
      logger.error("[" + id + "] Connection from " + clientInfo + " got exception", e);
    } finally {
      if (outStr != null) {
        try {
          outStr.close();
        } catch (IOException e) { 
          logger.error("[" + id + "] Failed to close output stream", e);
        }
      }
      try {
        socket.close();
      } catch (IOException e) {
        logger.error("[" + id + "] Failed to close socket", e);
      }
    }
  }

  /**
   * Create the output stream. 
   * 
   * TODO refactor to eliminate common code with InfobrightNamedPipeClient
   * @throws Exception
   * @throws SQLException
   */
  public OutputStream createOutputStream(String pipeName) throws Exception, SQLException {
    OutputStream os0;

    NamedPipeFactory factory = new NamedPipeFactory();
    PipeCallStrategy strategy = factory.getStrategy(logger);

    /* This is initiated by RemoteNamedPipe on the client side at the
     * correct time, so it doesn't matter if the stream comes back
     * from the beforeExecuteCreate or afterExecuteCreate
     * method. */
    
    os0 = strategy.beforeExecuteCreate(pipeName);
    if (os0 == null) {
      os0 = strategy.afterExecuteCreate(pipeName);
    }
    
    return os0;
  }
}
  
