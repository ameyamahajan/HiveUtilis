package com.hive.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class FirstUDF extends UDF {
  
  public Text evaluate(Text input) {
    if(input == null) return null;
    return new Text("Ameya's Simple UDF".toString());
  }
}
