package com.gdf.fdf;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class GDFParserReduce extends Reducer<Text, Text, Text, IntWritable> {
	String[] x = {"X", "R0", "R1", "R3", "R6", "R9"};
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Set<String> set = new HashSet<>();
		for(String e : x)
			set.add(e);
		
		JSONParser parser = new JSONParser();
		int count = 0;
		// Map<String, PrintWriter> pwMap = new HashMap<>();
		// construct the folder for the key
		String dirName = "./gdf-output/" + key.toString() + "/";
		File directory = new File(String.valueOf(dirName));
	    if (! directory.exists()){
	        directory.mkdir();
	    }
		
		for (Text value : values) {
			count ++;
			
			Object obj;
			try {
				obj = parser.parse(value.toString());
				JSONObject jsonObj = (JSONObject) obj;
				// X, R0, R1, R3, R6, R9 have Doc_Ref_No while others have Common_Doc_Ref_No
				String docRefNo, sectionCode;
				sectionCode = (String) jsonObj.get("Sec_Code");
				if (set.contains(sectionCode)) {
					docRefNo= (String) jsonObj.get("Doc_Ref_No");
				} else {
					docRefNo= (String) jsonObj.get("Common_Doc_Ref_No");
				}
								
				System.out.println("in reducer doc ref no: " + docRefNo + " for section code: " + sectionCode);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// System.out.println("count: " + value.toString());
			
			// TODO: given dirName above, for each new sectionCode, create a new PrintWriter and cache it. Otherwise, retrieve current one. Close all.
			
			/*
			PrintWriter pw;
			if (pwMap.containsKey(curDocRefNo + sectionCode))
				pw = map.get(curDocRefNo + sectionCode);
			else {
				String dirName = path + "/" + curDocRefNo + "/";
				File directory = new File(String.valueOf(dirName));
			    if (! directory.exists()){
			        directory.mkdir();
			    }
				
				pw = new PrintWriter(new BufferedWriter(new FileWriter(dirName + sectionCode + ".json", true)));
				map.put(curDocRefNo + sectionCode, pw);
			}
			pw.println(sb.toString());
			*/
		}
		System.out.println("in reducer key: " + key + " with count: " + count);
		
		context.write(key, new IntWritable(count));
	}
}
