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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;

import com.infobright.etl.model.BrighthouseRecord;
import com.infobright.etl.model.DataFormat;
import com.infobright.etl.model.datatype.AbstractColumnType;
import com.infobright.logging.EtlLogger;

/**
 * The main class used for preparing pipes and threads
 * for Infobright loading of data.
 */
public class InfobrightNamedPipeLoader {
  
  public static final Charset DEFAULT_CHARSET = Charset.forName("ISO-8859-1");

  public static final int AGENT_DEFAULT_PORT = 5555;
  
  private static final String JDBC_MYSQL = "jdbc:mysql://";
  private static final String DEFAULT_PIPENAME_PREFIX = "bhnamedpipe";
  private static final int DEFAULT_TIMEOUT_SECONDS = 15;
  
  private String pipeNamePrefix = DEFAULT_PIPENAME_PREFIX;
  private String pipeName;
  private DataFormat dataFormat = DataFormat.TXT_VARIABLE;
  private int timeout = DEFAULT_TIMEOUT_SECONDS;
  private final Charset charset;

  private ExecutionThread executionThread;
  private final PipeCallStrategy strategy;
  private final EtlLogger logger;
  private final Connection connection;
  private final String tableName;
  private final String sql;
  
  private final NamedPipeFactory factory;
  
  private long id;
  private OutputStream os = null;
  
  private OutputStream debugOs = null;
  
  private boolean runStarted = false;

  private final ClientProxy proxy;

  private static boolean shuttingDown = false;
  
  
  /**
   * Constructs a new loader instance.  No database action taken yet.
   * 
   * @param tableName
   * @param connection
   * @param logger
   * @param dataFormat
   * @param charset
   * @throws Exception
   */
  public InfobrightNamedPipeLoader(String tableName, Connection 
      connection, EtlLogger logger, DataFormat dataFormat, Charset charset)
      throws Exception {
    this(tableName, connection, logger, dataFormat, charset,
        AGENT_DEFAULT_PORT);
  }

  /**
   * Constructs a new loader instance.  No database action taken yet.
   *
   * @param tableName
   * @param connection
   * @param logger
   * @param dataFormat
   * @param charset
   * @param agentPort  the port number on the remote machine where the
   *                   agent is running (defaults to 5555)
   * @throws Exception
   */
  public InfobrightNamedPipeLoader(String tableName, Connection 
      connection, EtlLogger logger, DataFormat dataFormat, Charset charset,
      int agentPort)
          throws Exception {

    this.tableName = tableName;
    this.connection = connection;
    this.dataFormat = dataFormat;
    this.logger = logger;
    this.charset = charset;

    String hostName = _getHostName(connection);
    boolean isLocal = _isLocal(hostName);
    
    // Use LOAD DATA LOCAL INFILE if (1) connection is remote; (2) local client
    // is Linux/Unix; and (3) the IB release supports it. In this case all pipe
    // operations are done locally using Unix semantics.
    boolean useLocalInfile =
        !isLocal && new OSType().isUnix() && new IBVersionUtil(connection).isSupportsLocalInfile();
    
    if (isLocal || useLocalInfile) {
      proxy = null;
      factory = new NamedPipeFactory();
    } else {
      proxy = new ClientProxy(hostName, agentPort);
      factory = new NamedPipeFactory(proxy);
    }

    strategy = factory.getStrategy(logger);
    id = LoaderInstanceTracker.register(this);
    
    // the named pipe name will be the prefix with a date/time stamp appended.
    // since multiple loaders may be started in the same millisecond, we append the instance # to the 
    // end of the name to ensure uniqueness.
    pipeName = String.format("%s_%tH_%<tM_%<tS_%<tL-%d", this.pipeNamePrefix, new Date(), id);
    sql = dataFormat.getLoadSQL(getEscapedPipeName(pipeName), tableName, useLocalInfile);
  }

  /**
   * @param connection2
   * @return the database server hostname, derived from the JDBC URL
   * @throws SQLException
   * @throws RuntimeException
   */
  private static String _getHostName(Connection connection2) throws SQLException {
    // URL format: jdbc:mysql://hostname[:port]/dbname
    String url = connection2.getMetaData().getURL();
    if (!(url.startsWith(JDBC_MYSQL))) {
      throw new RuntimeException("This does not look like a MySQL URL!");
    }
    String host = url.substring(JDBC_MYSQL.length());
    int index = host.indexOf(":");
    if (index == -1) {
      index = host.indexOf("/");
    }
    if (index == -1) {
      throw new RuntimeException("Can't extract hostname from JDBC URL (malformed?)");
    }
    host = host.substring(0, index);
    return host;
  }
  
  /**
   * Determine if hostname is local.
   * 
   * If hostName == "localhost" or "127.0.0.1" then the load
   * will be done using a local pipe. Otherwise it will use a
   * remote pipe. The external IP address of the server is not
   * recognized as a local address.
   */
  private static boolean _isLocal(String hostName) {
    return ("localhost".equalsIgnoreCase(hostName) || "127.0.0.1".equals(hostName));
  }
  
  /**
   * Constructs a new loader instance.  No database action taken yet.
   *
   * @param tableName
   * @param connection
   * @param logger
   * @param dataFormat
   * @throws Exception
   */
  public InfobrightNamedPipeLoader(String tableName, Connection connection,
      EtlLogger logger, DataFormat dataFormat) throws Exception {
    this(tableName, connection, logger, dataFormat, DEFAULT_CHARSET);
  }
  
  /**
   * Spins off a thread to prepare for a database named pipe
   * interaction.
   *
   * @throws Exception 
   */
  private void startExecutionThread() throws Exception {

    if (isShuttingDown()) {
      // JVM is shutting down. Do not start any more loads!
      return;
    }

    // Make sure sql has been set
    assert(sql != null);
    
    // start up thread and check that it is happy
    executionThread = new ExecutionThread(connection, this.getDataFormat(), this.pipeName);
    executionThread.start();
    
    int maxWait = timeout * 1000;
    final int sleepTime = 200;
    long startTime = System.currentTimeMillis();
    while (!executionThread.getConnecting() && executionThread.isAlive() && (System.currentTimeMillis() - startTime) < maxWait) {
      try {
        Thread.sleep(sleepTime);
      } catch (Exception ex) {}
    }
    
    // did the start up thread get to the right place (connecting) without an exception?
    if (!(executionThread.isAlive() && executionThread.getConnecting())) {
      if (executionThread.ex != null) {
        throw executionThread.ex;
      } else {
        throw new Exception("BrightHouse background thread did not start as expected.");
      }
    }
  }

  /**
   * Joins to the sql connection background thread (i.e. wait until
   * that thread is finished with its work) and then checks to see
   * if it had any exceptions.
   * 
   * @throws Exception
   */
  protected void joinExecutionThread() throws Exception {
    executionThread.join();
    if (executionThread.ex != null) {
      throw executionThread.ex;
    }
  }
  
  class ExecutionThread extends Thread {

    private final Connection connection;
    private final DataFormat dataFormat;
    private final String pipeName;

    private Statement statement;
    
    boolean connecting = false;
    Exception ex = null;
    private boolean alreadyKilled = false;
    
    ExecutionThread(Connection connection, DataFormat dataFormat, String pipeName) {
      this.connection = connection;
      this.dataFormat = dataFormat;
      this.pipeName = pipeName;
    }

    /** {@inheritDoc}
     * @see java.lang.Thread#run()
     */
    public void run() {
      try {
        statement = connection.createStatement();
        String setupSql;

        setupSql = "set @bh_dataformat='" + dataFormat.getBhDataFormat() + "';";
        if (logger != null) logger.debug(String.format("exec sql: %s", setupSql));
        statement.execute(setupSql);

        strategy.setupForLoad(statement, new Integer[] { timeout } );
        
        if (logger != null) logger.debug(String.format("ID#%d starting %s load thread via named pipe %s", getID(), dataFormat, pipeName));
        connecting = true;
        
        if (logger != null) logger.debug(String.format("ID#%d executing sql: %s", getID(), sql));
        try {
          statement.execute(sql);
        } finally {
          if (proxy != null) {
            proxy.disconnect();
          }
        }
        if (logger != null) logger.debug(String.format("load thread via named pipe %s stopped", pipeName));
        
      } catch (Exception se) {
        if (logger != null) logger.error("Exception during sql named pipe use", se);
        ex = se;
      } catch (UnsatisfiedLinkError error) {
        ex = new Exception(error.getMessage() + ", java.library.path=" +
            System.getProperty("java.library.path"), error);
        
      } finally {
        connecting = false;
      }
    }
    
    /**
     * @return true if the named pipe connection to the db server is being called.
     */
    boolean getConnecting() {
      return connecting;
    }
    
    /**
     * Kill the executing query.
     */
    synchronized void killQuery() throws SQLException {
      if (!alreadyKilled) {
       if (statement != null) {
          statement.cancel();
          if (logger != null) logger.debug(String.format("ID#%d SQL statement cancelled", getID()));
          alreadyKilled = true;
        }
      }
    }
  }

  private DataFormat getDataFormat() {
    return dataFormat;
  }
  
  public String getPipeName() {
    return pipeName;
  }

  private String getEscapedPipeName(String pipeNm) {
    return factory.getNativePipeName(pipeNm).replace("\\", "\\\\");
  }
  
  public void setPipeNamePrefix(String pipeNamePrefix) {
    this.pipeNamePrefix = pipeNamePrefix;
  }

  public int getTimeout() {
    return timeout;
  }

  /** in seconds */
  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public EtlLogger getEtlLogger() {
    return logger;
  }
  
  /**
   * Kill the executing query.
   */
  public void killQuery() throws SQLException {
    if (executionThread != null) {
      executionThread.killQuery();
    }
  }

  private Connection getConnection() {
    return connection;
  }

  private String getTableName() {
    return tableName;
  }
  
  @Override
  public void finalize() {
    LoaderInstanceTracker.unregister(this);
  }

  long getID() {
    return id;
  }

  /**
   * Returns an output stream suitable for writing data to the BH loader.
   * 
   * @deprecated This should return java.io.OutputStream, but to change the
   * API now would require a recompile of all clients. Please use
   * getOutputStream2() in all new code.
   * 
   * @return OutputStream
   * @throws IOException
   */
  public NamedPipeOutputStream getOutputStream() throws IOException {
	if (os instanceof NamedPipeOutputStream) {
      return (NamedPipeOutputStream) os;
	} else {
	  throw new UnsupportedOperationException("Running in debug mode. Please use" +
	      " getOutputStream2() instead of getOutputStream()");
	}
  }
  
  /**
   * Returns an output stream suitable for writing data to the BH loader.
   * 
   * @return OutputStream
   * @throws IOException
   */
  public OutputStream getOutputStream2() {
	return os;
  }

  /**
   * Has no effect. Kept for backwards compatibility.
   * 
   * @deprecated
   */
  public void checkConnectionIsReady(int timeout) throws Exception {
  }

  /**
   * Sets a secondary output stream to which all data to the named pipe
   * will be captured. Useful for debugging.
   * 
   * @param debugOs
   */
  public void setDebugOutputStream(OutputStream debugOs) {
    if (runStarted) {
      throw new IllegalStateException("must be called before start()");
    }
    this.debugOs = debugOs;
  }
  
  /**
   * Kick off the load. Must be initiated by the client.
   * 
   * @throws Exception
   * @throws SQLException
   */
  public void start() throws Exception, SQLException {
    runStarted = true;
    OutputStream os0;
    
    // On Unix we need to set up the named pipe before kicking off the load.
    // Returns null on Windows.
    os0 = strategy.beforeExecuteCreate(getPipeName());
    
    // Kick off the Brighthouse load command
    if (logger == null) {
      // no timing required
      startExecutionThread();
    } else {
      // timing required
      long startTime = new Date().getTime();
      startExecutionThread();
      long timeToExec = new Date().getTime() - startTime;
      logger.debug("SQL load started in " + timeToExec + " ms");
    }
    
    // On Windows the load command creates the pipe
    if (os0 == null) {
      os0 = strategy.afterExecuteCreate(getPipeName());
    }
    
    if (debugOs == null) {
      os = os0;
    } else {
      os = new TeeOutputStream(os0, debugOs);
    }
  }

  /**
   * Closes any open output streams. Waits for the loader thread
   * to finish.
   * 
   * @throws Exception
   */
  public void stop() throws Exception {
    if (os != null) {
      if (logger != null) logger.debug("Loader closing output stream");
      os.close();
    } else {
      if (logger != null) logger.debug("Loader: No output stream to close!");
    }
    if (logger != null) logger.debug("Waiting for SQL load command to finish");
    joinExecutionThread();
    if (logger != null) logger.debug("SQL load finished");
  }

  /**
   * Returns a blank record for passing data. Normally called
   * only once per loader instance.
   * 
   * Must be called once before starting the run.
   *
   * @param checkValues whether to validate the values passed. If false,
   *   the record.setData() will normally not throw an exception, and
   *   the loader will throw an exception on commit. If true,
   *   the record.setData() will throw ValueConverterException???FIXME
   *   when setting the value. In this case the ETL tool can handle it
   *   gracefully, perhaps sending the row to an error stream.
   * 
   * @throws SQLException
   * @throws IllegalStateException if the run has already started
   */
  public BrighthouseRecord createRecord(boolean checkValues)
          throws SQLException {
    if (runStarted) {
      throw new IllegalStateException("Run is already started");
    }
    Statement stmt = getConnection().createStatement();
    ResultSet rs = stmt.executeQuery("select * from `" + getTableName() + "` limit 0");
    ResultSetMetaData md = rs.getMetaData();
    List<AbstractColumnType> columns = BrighthouseRecord.readColumnTypes(md, charset, logger, checkValues);
    rs.close();
    stmt.close();
    return getDataFormat().createRecord(columns, charset, logger);
  }

  synchronized static void setShuttingDown() {
    shuttingDown = true;
  }

  synchronized static boolean isShuttingDown() {
    return shuttingDown;
  }

}
