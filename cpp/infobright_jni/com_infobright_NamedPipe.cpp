#include "jniutils.h"
#include <windows.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     com_infobright_io_WindowsNamedPipe
 * Method:    serverCreate
 * Signature: (Ljava/lang/String;ZZIJ)J
 */
JNIEXPORT jlong JNICALL Java_com_infobright_io_WindowsNamedPipe_serverCreate
(JNIEnv *env , jclass clazz, jstring jPipeName, jboolean allowWrite, jboolean allowRead, jint bufsize, jlong timeout)
{
  string pipeName = getString(env, jPipeName);

  // setup access mode
  DWORD pipeAccess /*= PIPE_ACCESS_DUPLEX*/;
  if (allowWrite && allowRead) pipeAccess = PIPE_ACCESS_DUPLEX;
  else if (allowWrite) pipeAccess = PIPE_ACCESS_OUTBOUND;
  else if (allowRead) pipeAccess = PIPE_ACCESS_INBOUND;

  /* If you attempt to create multiple instances of a pipe with this flag, 
     creation of the first instance succeeds, but creation of the next instance 
     fails with ERROR_ACCESS_DENIED.
     
     Windows 2000/NT:  This flag is not supported until Windows 2000 SP2 and Windows XP.
   */
  //pipeAccess |= FILE_FLAG_FIRST_PIPE_INSTANCE;

  // setup access mode
  DWORD pipeMode = 0;

  /* Data is written to the pipe as a stream of bytes. This mode cannot be used with PIPE_READMODE_MESSAGE. */
  pipeMode |= PIPE_TYPE_BYTE;  
  /* Data is read from the pipe as a stream of bytes. This mode can be used with either PIPE_TYPE_MESSAGE or PIPE_TYPE_BYTE. */
  pipeMode |= PIPE_READMODE_BYTE;
  /* Blocking mode is enabled. When the pipe handle is specified in the ReadFile, WriteFile, or 
   * ConnectNamedPipe function, the operations are not completed until there is data to read, 
   * all data is written, or a client is connected. Use of this mode can mean waiting indefinitely 
   * in some situations for a client process to perform an action. */
  pipeMode |= PIPE_WAIT;

  // now create the pipe!
  HANDLE hPipe;
  hPipe = CreateNamedPipe(pipeName.c_str(), pipeAccess, pipeMode, 
                          PIPE_UNLIMITED_INSTANCES, // max. instances
                          bufsize, // output buffer size
                          bufsize, // input buffer size
                          (DWORD) timeout, // client time-out
                          NULL); // no security attribute

  if (hPipe == INVALID_HANDLE_VALUE)
  {
    throwJavaExceptionFromLastWindowsError(env, "java/io/IOException", "Could not create named pipe: ");
  }
  return (jlong) hPipe;
}

/*
 * Class:     com_infobright_io_WindowsNamedPipe
 * Method:    fileRead
 * Signature: (J[BII)I
 */
#define BUFSIZE 1024
JNIEXPORT jint JNICALL Java_com_infobright_io_WindowsNamedPipe_fileRead
  (JNIEnv *env, jclass clazz, jlong hPipe, jbyteArray bytes, jint offset, jint len)
{
  DWORD bytesRead = 0;
  signed char stackBuf[BUFSIZE];
  int maxlen = env->GetArrayLength(bytes);
  if (offset+len>maxlen) len=maxlen-offset;

  // if the target length is less than our default (stack-allocated) buffer, then
  // dynamically allocate a new buffer to use.
  signed char* buf;
  if (len > BUFSIZE)
    buf = new signed char[len];
  else
    buf = stackBuf;

  if (!ReadFile((HANDLE) hPipe, buf, len, &bytesRead, NULL))
  {
    throwJavaExceptionFromLastWindowsError(env, "java/io/IOException", "Named pipe read failed: ");
  }
  else
  {
    env->SetByteArrayRegion(bytes, offset, bytesRead, (const jbyte*) buf);
  }

  if (len > BUFSIZE) delete [] buf;  // delete the buffer if we had to allocate it.
  return (jint) bytesRead;
}

/*
 * Class:     com_infobright_io_WindowsNamedPipe
 * Method:    fileWrite
 * Signature: (J[BII)I
 */
JNIEXPORT jint JNICALL Java_com_infobright_io_WindowsNamedPipe_fileWrite
  (JNIEnv *env, jclass clazz, jlong hPipe, jbyteArray bytes, jint offset, jint len)
{
  DWORD bytesWritten;
  signed char stackBuf[BUFSIZE];
  int maxlen = env->GetArrayLength(bytes);
  if (offset+len>maxlen) len=maxlen-offset;

  // if the target length is less than our default (stack-allocated) buffer, then
  // dynamically allocate a new buffer to use.
  signed char* buf;
  if (len > BUFSIZE)
    buf = new signed char[len];
  else
    buf = stackBuf;

  // move the bytes from the java array to our local one
  env->GetByteArrayRegion(bytes, offset, len, (jbyte*) buf);

  if (!WriteFile((HANDLE) hPipe, buf, len, &bytesWritten, NULL))
  {
    throwJavaExceptionFromLastWindowsError(env, "java/io/IOException", "Named pipe write failed: ");
  }

  if (len > BUFSIZE) delete [] buf;  // delete the buffer if we had to allocate it.
  return bytesWritten;
}

/*
 * Class:     com_infobright_io_WindowsNamedPipe
 * Method:    fileClose
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_infobright_io_WindowsNamedPipe_fileClose
  (JNIEnv *env, jclass clazz, jlong handle)
{
  if (!CloseHandle((HANDLE) handle))
  {
    throwJavaExceptionFromLastWindowsError(env, "java/io/IOException", "Could not close named pipe: ");
  }
}

/*
 * Class:     com_infobright_io_WindowsNamedPipe
 * Method:    clientCreate
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_infobright_io_WindowsNamedPipe_clientCreate
  (JNIEnv *env , jclass clazz, jstring jPipeName)
{
  string pipeName = getString(env, jPipeName);
  
  HANDLE hFile = CreateFile(pipeName.c_str(), GENERIC_WRITE, 0, NULL, OPEN_EXISTING, 0, NULL);
  if(hFile == INVALID_HANDLE_VALUE)
  {
    throwJavaExceptionFromLastWindowsError(env, "java/io/IOException", "Could not open client pipe for writing: ");
  }

  return (jlong) hFile;
}

#ifdef __cplusplus
}
#endif
