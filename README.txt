Loading to local and remote hosts:
----------------------------------

If the IB hostname passed to the connector matches
"localhost" or "127.0.0.1", then the load will be
done using a local pipe. Otherwise it will use a
remote pipe. (An external IP address of the server
cannot be used; it will not be recognized as local.)

If loading to a remote host, and either of the following
conditions applies, then you must start the Infobright
remote load agent:

1) Your IB release is < 3.5.0
   *or*
2) Your client is running on Windows.

The command to start the agent on the IB server is:

  java -jar infobright-core-3.4.jar [-p port] [-l loglevel]
  
The output can be redirected to a log file.

The port defaults to 5555. (ETL tools such as Kettle and
Talend may not yet have the ability to specify a different port
number.)

The loglevel can be one of: "info", "error", "debug".



Instructions for IB running on Windows (w32, x64):
--------------------------------------------------

The lib directory contains 2 DDLs: infobright_jni_32bit.dll, infobright_jni_64bit.dll.
The source code for these libraries is in the "cpp" directory.

You must ensure that the appropriate library is renamed to infobright_jni.dll and
placed on the Java library path. The easiest way to do this is to copy it into
the C:\WINDOWS folder.

When running on a 32-bit JVM, rename infobright_jni_32bit.dll to infobright_jni.dll.
When running on a 64-bit JVM, rename infobright_jni_64bit.dll to infobright_jni.dll.
