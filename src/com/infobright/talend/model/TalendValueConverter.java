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

package com.infobright.talend.model;

import java.util.HashMap;
import java.util.Map;

import com.infobright.etl.model.GenericValueConverter;
import com.infobright.etl.model.ValueConverterException;

/**
 * Converts objects from the type Talend thinks it is (the Talend
 * type), to types in the database. The target type is determined
 * by the column type in the database.
 * 
 * For now, we don't do anything special besides the generic
 * conversions.
 * 
 * @author gfalk
 */
public class TalendValueConverter extends GenericValueConverter {

  @SuppressWarnings("unused")
  private final String talendType;
  
  private final static Map<String, TalendValueConverter> typeMap =
      new HashMap<String, TalendValueConverter>();
  
  // FIXME accept date format parameter
  private TalendValueConverter(String talendType) {
    this.talendType = talendType;
  }
  
  public static TalendValueConverter getInstance(String talendType) {
    TalendValueConverter instance;
    synchronized (typeMap) {
      instance = typeMap.get(talendType);
      if (instance == null) {
        instance = new TalendValueConverter(talendType);
        typeMap.put(talendType, instance);
      }
    }
    return instance;
  }
  
  @Override
  public String getString(Object object) throws ValueConverterException {
    if (object == null) {
      return null;
    }
    /*
    if (object instanceof Date) {
      // TODO: If talend type = DATE, use YYYY-MM-DD
      // else if talend type = DATETIME or TIMESTAMP, use YYYY-MM-DD HH-mm-ss

    } else { */
      return super.getString(object);
    /* } */
  }
  
}
