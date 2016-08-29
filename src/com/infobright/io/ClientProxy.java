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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;

import com.infobright.reflect.MethodTimedOutException;
import com.infobright.reflect.TimeoutEnabledMethod;

class ClientProxy {

  private static final long DEFAULT_TIMEOUT = 5000;

  private final String hostName;
  private final long timeout;
  
  private String osName;

  private final Socket socket;
  private final DataInputStream in;
  private final DataOutputStream out;
  
  String getHostName() {
    return hostName;
  }

  /**
   * Connect to the Infobright agent on the remote server.
   * 
   * @param hostName
   * @param port
   * @throws IOException if host could not be reached or timeout
   */
  ClientProxy(String hostName, int port) throws IOException {
    this(hostName, port, DEFAULT_TIMEOUT);
  }
  
  /**
   * Connect to the Infobright agent on the remote server.
   * 
   * @param hostName
   * @param port
   * @throws IOException if host could not be reached or timeout
   */
  ClientProxy(String hostName, int port, long timeout) throws IOException {
    this.hostName = hostName;
    this.timeout = timeout;
    socket = new Socket(hostName, port);
    in = new DataInputStream(socket.getInputStream());
    out = new DataOutputStream(
        new BufferedOutputStream(socket.getOutputStream()));
    osName = in.readUTF();
  }
  
  String getOSName() throws IOException {
    return osName;
  }

  /**
   * Attempt to connect to the agent. Wait for the timeout before cancelling
   * the attempt.
   * 
   * @param pipeName
   * @throws IOException
   */
  void connect(String pipeName) throws IOException {
    try {
      Method connectIndefinite = getClass().getDeclaredMethod("connectIndefinite", String.class);
      connectIndefinite.setAccessible(true);
      TimeoutEnabledMethod connect = new TimeoutEnabledMethod(connectIndefinite);
      connect.invoke(timeout, this, pipeName);
    } catch (MethodTimedOutException e) {
      throw new IOException("connection to agent timed out after " + timeout + "ms");
    } catch (InvocationTargetException e) {
      throw new IOException(e.getMessage());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Attempt to connect to the agent. Wait forever for a response.
   * 
   * @param pipeName
   * @throws IOException
   */
  void connectIndefinite(String pipeName) throws IOException {
    out.writeUTF(pipeName);
    out.flush();
    String response = in.readUTF();
    if (!(AgentThread.OK_MSG.equals(response))) {
      throw new IOException("Agent failed to create pipe on remote side");
    }
  }
  
  void write(byte[] data) throws IOException {
    out.write(data);
  }
  
  void write(byte[] data, int offset, int length) throws IOException {
    out.write(data, offset, length);
  }
  
  void disconnect() throws IOException {
    out.close();
    socket.close();
  }
}
