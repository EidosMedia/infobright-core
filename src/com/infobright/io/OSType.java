package com.infobright.io;

class OSType {

  private static final String S_WIN = "win";
  private static final String S_SUNOS = "SunOS";
  private static final String S_LINUX = "linux";
  
  private final String osName;
  
  OSType(String osName) {
    this.osName = osName;
  }
  
  OSType() {
    this(System.getProperty("os.name"));
  }
  
  public boolean isLinux() {
    return S_LINUX.equalsIgnoreCase(osName);
  }
  
  public boolean isSolaris() {
    return S_SUNOS.equalsIgnoreCase(osName);
  }
  
  public boolean isWindows() {
    return osName.toLowerCase().contains(S_WIN);
  }
  
  public boolean isUnix() {
    return isLinux() || isSolaris();
  }
}
