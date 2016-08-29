#include <windows.h>
#include <stdio.h>
#include <lmerr.h>

#include "jniutils.h"
#include <sstream>
#include <iostream>
using namespace std;

string getString(JNIEnv* env, const jstring& s)
{
	const jchar* str = env->GetStringChars(s, NULL);
  int len = env->GetStringLength(s);

	if (str == NULL) return NULL;  // jni already has thrown out of memory exception

  // we grabbed the string as a 16-bit unicode string.  but we can only handle the high-bytes (where 8-bit
  // characters are stored).   luckily that is all that should be set!
  //
	string res;
  res.resize(len);
  for (int i=0; i<len*2; i+=2)
    res[i/2] = (char) ((char*)str)[i];

	env->ReleaseStringChars(s, str);
	return res;
}

////////////////////////////////////////////////////////////////////////////////
// Exception related and other utility functions

const void throwJavaException(JNIEnv* env, const string& exceptionName, const string& message)
{
	jclass exceptionClass = env->FindClass(exceptionName.c_str());

	// bad exception class, so look for one we know and throw that
  if (exceptionClass == NULL)
  {
    if (env->ExceptionOccurred()) env->ExceptionClear();  // clear so we can call FindClass
		exceptionClass = env->FindClass("java/lang/ClassNotFoundException");
  }

  if (exceptionClass != NULL) 
  	env->ThrowNew(exceptionClass, message.c_str());
}

const void throwJavaExceptionFromLastWindowsError(JNIEnv* env, const string& exceptionName, const string& prefix)
{
  HMODULE hModule = NULL; // default to system source
  DWORD dwLastError = GetLastError();
  LPVOID lpMsgBuf;
  DWORD dwFormatFlags = FORMAT_MESSAGE_ALLOCATE_BUFFER |
        FORMAT_MESSAGE_IGNORE_INSERTS |
        FORMAT_MESSAGE_FROM_SYSTEM ;

  string errorMessage = prefix;

  //
  // If dwLastError is in the network range, 
  //  load the message source.
  //

  if(dwLastError >= NERR_BASE && dwLastError <= MAX_NERR)
  {
    hModule = LoadLibraryEx(TEXT("netmsg.dll"), NULL, LOAD_LIBRARY_AS_DATAFILE);
    if(hModule != NULL) dwFormatFlags |= FORMAT_MESSAGE_FROM_HMODULE;
  }

  //
  // Call FormatMessage() to allow for message 
  //  text to be acquired from the system 
  //  or from the supplied module handle.
  //

  if(!FormatMessageA(dwFormatFlags, hModule, // module to get message from (NULL == system)
                     dwLastError,
                     MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), // default language
                     (LPSTR) &lpMsgBuf,
                     0,
                     NULL))
  {
    char buf[100];
    sprintf(buf, "Unknown error: %u", dwLastError);
    errorMessage += buf;
  }
  else
  {
    errorMessage = errorMessage + (char*)lpMsgBuf;
    LocalFree(lpMsgBuf);
  }

  //
  // If we loaded a message source, unload it.
  //
  if(hModule != NULL) FreeLibrary(hModule);

  throwJavaException(env, exceptionName, errorMessage);
}
