package com.hive.udfs;

import java.net.URLDecoder;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class URIUnescape extends UDF{
	  public Text evaluate(Text input) {
		    if(input == null) return null;
		    String url = input.toString();
		    @SuppressWarnings("deprecation")
			String decode = URLDecoder.decode(url);
		    return new Text(decode.toString()); 
		  }
}
