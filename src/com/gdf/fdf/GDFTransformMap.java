package com.gdf.fdf;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class GDFTransformMap extends Mapper<LongWritable, Text, Text, Text> {
	Map<String, JSONObject> constant_tables = null; 
	JSONObject mappingRecipe = null;
	JSONParser parser = new JSONParser();	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		// 0. initialize the cache for constant_tables
		if(constant_tables == null) {
			System.out.println("get constant_tables ready only once.");
			constant_tables = Util.getConstantTables("./recipes/constant_tables.json");			
		}
		// retrieve the mapping recipe
		if (mappingRecipe == null) {
			try {
				mappingRecipe = (JSONObject) Util.getJsonObjMap("./recipes/mapping.json");
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		
		String line = value.toString();
		String selected_line = "no use";
		JSONObject jsonObj = null;
		String secCode = null;
		String selectPattern = "AG";
		try {
			// 1. convert current value to a JSON object
			jsonObj = (JSONObject) (parser.parse(line.toString()));
			secCode = (String) jsonObj.get("Sec_Code");
			
			// 2. do selection
			// 2.1 get mapping recipe for this specific section code
			// 2.2 apply filter criteria
			// 2.3 (optional) retrieve selection constants from cache
			// 2.4 do mapping according to recipe and generate a new JSON object as output
			if (secCode.equals("A")){
				String lineAttr1 = (String) jsonObj.get("Line_Attr_1");
				if (selectPattern.equals(lineAttr1))
					selected_line = line;
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// System.out.println("in mapper current doc_ref_no: " + curDocRefNo + " with content: " + sb.toString());
		context.write(new Text((String) jsonObj.get("Sec_Code")), new Text(selected_line));
	}
}
