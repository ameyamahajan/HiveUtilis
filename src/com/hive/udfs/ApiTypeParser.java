package com.hive.udfs;

import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;



public final class ApiTypeParser extends UDF {
	public Text evaluate(Text input) {
		if(input == null) return null;
		try {
			String url = input.toString(); 
			String decode_url = URLDecoder.decode(url);
			Pattern extract_action = Pattern.compile("[?&]action=(.+?)(&|$)",Pattern.CASE_INSENSITIVE);
			String action = "";
			String result = "Unknown";
			Matcher m = extract_action.matcher(decode_url); 
			if(m.find()){  
				action=m.group(1).trim();
			}
			else if(Pattern.compile("[?&](artist|song)=(.+?)(&|$)",2).matcher(decode_url).find()){
				action="lyrics";
			}
			if ("lyrics".equalsIgnoreCase(action)){
				result = "LyricWiki";
			}
			else if("nirvana".equalsIgnoreCase(action) ){
				result = "Nirvana";
			}
			else if ("imageserving".equalsIgnoreCase(action)){
				result = "Wikia Extensions to MediaWiki";
			}
			else if("opensearch".equalsIgnoreCase(action)){
				result = "Core MediaWiki";
			}
			else if("query".equalsIgnoreCase(action) && Pattern.compile("[?&](?:prop|list|meta)=wk.*",2).matcher(decode_url).find()) {
				result = "Wikia Extensions to MediaWiki";
			}
			extract_action =  null ;
			return (new Text(result.toString()));
		}catch(IllegalArgumentException ex){
			System.out.println(ex.getMessage());
			return null;

		}
		catch(IllegalStateException ex){
			System.out.println(ex.getMessage());
			return null;
		}
		catch(Exception ex){
			System.out.println(ex.getMessage());
			return null; 
		}
	}
}
