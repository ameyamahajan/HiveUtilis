package com.hive.udfs;

import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Arrays; 

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class ApiFunctionParser  extends UDF{
	public Text evaluate(Text input) {
		if(input == null) return null;
		try{
			String url = input.toString(); 
			url = url.replaceAll("%([^0-9A-F]{2}|[^0-9A-F][0-9A-F]|[0-9A-F][^0-9A-F])","$1");
			// Remove the illegal "%" sign ex. 
			//1)%ro  
			//2)%3M 
			//3) %D3  


			url = url.replaceAll("%.{1,2}$","");
			// Remove the illegal "%" sign at end of url ex. 
			//1) <url>%  
			// 2)<url>%D 

			String decode_url = URLDecoder.decode(url);
			Pattern extract_action = Pattern.compile("[?&]action=(.+?)(&|$)",2);
			Pattern extract_function = Pattern.compile("[?&]func=(.+?)(&|$)",2);
			String action = "";
			String result = "";
			Matcher action_pattern_matcher = extract_action.matcher(decode_url);
			Matcher function_pattern_matcher = extract_function.matcher(decode_url);

			if (action_pattern_matcher.find()) {
				action=action_pattern_matcher.group(1).trim();
				result=action;
			}
			else if(Pattern.compile("[?&](artist|song)=(.+?)(&|$)",2).matcher(decode_url).find())
			{result="lyrics";}
			else if (Pattern.compile("^/api.php$",2).matcher(decode_url).find())
			{result="[documentation]";}
			else 
			{result="[invalid]";}

			if ("lyrics".equals(result)) {
				if (function_pattern_matcher.find()){
					result=function_pattern_matcher.group(1); 
				}
				else if(Pattern.compile("[?&]song=(.+?)(&|$)",2).matcher(decode_url).find()){
					result="getSong"; 
				}
				else if (Pattern.compile("[?&]artist=(.+?)(&|$)",2).matcher(decode_url).find()){
					result="getArtist"; 
				}else {
					result="[invalid]"; 
				}	
			}
			else if ("query".equalsIgnoreCase(result)){
				action_pattern_matcher = Pattern.compile("[?&](prop|list|meta|titles)=([^&]+)",2).matcher(decode_url);
				String sep="", colon=":" , pipe="";
				result = "";
				String[] str_arr;
				for(int arr_index = 0 ; action_pattern_matcher.find() ; arr_index++ ){
					if(action_pattern_matcher.groupCount() < 2) { continue;}
					if (action_pattern_matcher.group(1).equals("titles")) {
						result=result+sep+"query:"+action_pattern_matcher.group(1) ;
						sep="|**|";
						continue;
					}
					result=result+sep+"query:"+action_pattern_matcher.group(1)+colon;
					sep="|**|";
					str_arr = action_pattern_matcher.group(2).split("\\|"); 
					Arrays.sort(str_arr);
					for (String e:str_arr ){
						result=result+pipe+e; 
						pipe="|";
					}
					pipe="";

				}

			}
			extract_action =  null ;
			extract_function= null ;
			action_pattern_matcher = null ;
			function_pattern_matcher = null ; 
			if (result.trim() == "") result = "[invalid]"; 
			return (new Text(result.toString()));
		}
		catch(IllegalArgumentException ex){
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
