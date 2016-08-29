#ifndef _jniutils_h
#define _jniutils_h

#include <jni.h>
#include <string>
using namespace std;

string getString(JNIEnv* env, const jstring& s);

const void throwJavaException(JNIEnv* env, const string& exceptionName, const string& message);
const void throwJavaExceptionFromLastWindowsError(JNIEnv* env, const string& exceptionName, const string& prefix);

#endif
