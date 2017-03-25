package com.gdf.fdf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class GDFTransformMap extends Mapper<LongWritable, Text, Text, Text> {
	Map<String, JSONObject> constTables = null;
	JSONObject mappingRecipe = null;
	Map<String, JSONArray> cachedMappingRecipes = new HashMap<>();
	JSONParser parser = new JSONParser();
	
	@SuppressWarnings("unchecked")
	private MappingKeyValPair doRecipeMapping(String line) {
		String selected_line = "no use";
		JSONObject curRecordObj = null;
		String secCode = null;
		String recordKey = "default";
		JSONObject mappedResultObj = new JSONObject();
		try {
			// 1. convert current line to a JSON object
			curRecordObj = (JSONObject) (parser.parse(line.toString()));
			secCode = (String) curRecordObj.get("Sec_Code");

			// DO NOT handle those records without their mapping recipes
			if (!mappingRecipe.containsKey(secCode)) {
				return null;
			}

			// 2. do selection
			// 2.1 get mapping recipes for this specific section code
			JSONArray curMappingRecipes = null;

			if (cachedMappingRecipes.containsKey(secCode)) {
				curMappingRecipes = cachedMappingRecipes.get(secCode);
			} else {
				curMappingRecipes = (JSONArray) mappingRecipe.get(secCode);
				cachedMappingRecipes.put(secCode, curMappingRecipes);
				System.out.println("get current recipe only once for section code " + secCode);
			}
			
			// 2.2 iterate each recipe in current mapping recipes
			Iterator<JSONObject> mappingRecipeIt = curMappingRecipes.iterator();
			while(mappingRecipeIt.hasNext()) {
				JSONObject curMappingRecipe = mappingRecipeIt.next();
				
				// 2.3 apply filter criteria
				JSONObject filterCriteria = (JSONObject) curMappingRecipe.get("filter_criteria");
				String filterFieldVal = (String) filterCriteria.get("field_val");
				String filterRule = (String) filterCriteria.get("rule");
				String filterFieldKey = (String) filterCriteria.get("field_name");
				// apply mapping rule
				if (filterRule.equals("equal")) {
					String lineAttr1 = (String) curRecordObj.get(filterFieldKey);
					if (lineAttr1.equals(filterFieldVal)){
						// 2.4 (optional) retrieve selection constants from cache
						// build a hash for next use in real mapping stage
						Map<String, JSONObject> constMappingResults = new HashMap<>();
						if (curMappingRecipe.containsKey("constant_tables")) {
							// 2.4.1 retrieve the constant table recipe
							JSONArray constTableItems = (JSONArray) curMappingRecipe.get("constant_tables");
							Iterator<JSONObject> constTableIt = constTableItems.iterator();
							// 2.4.2 iterate each constant table mapping for current recipe
				            while (constTableIt.hasNext()) {
				                // System.out.println(iterator.next());
				            	JSONObject constItem = constTableIt.next();
				            	// 2.4.3 get the name of specific constant table, e.g. tier_mapping
				            	String constTableName = (String) constItem.get("src");
				            	// the prefix for the key
				            	String constMappingKeyPrefix = "constant_tables" + "." + constTableName + ".";
				            	// 2.4.4 get the selection criteria
				            	JSONArray constSelectionItems = (JSONArray) constItem.get("selection");
				            	Iterator<JSONObject> constSelectionIt = constSelectionItems.iterator();
				            	// iterate each selection
				            	while(constSelectionIt.hasNext()) {
				            		JSONObject selectionItem = constSelectionIt.next();
				            		// the key for the hash is ready
				            		String constMappingKey = constMappingKeyPrefix + (String) selectionItem.get("dest");
				            		// get the constant table from cache
				            		JSONObject resultConstTable = constTables.get(constTableName);
				            		// retrieve the constant table's JSON object according to select_key value in the selection
				            		JSONObject constMappingVal = (JSONObject) resultConstTable.get((String) selectionItem.get("select_key"));
				            		// put the <key,value> into the hash, e.g. constant_tables.tier_mapping.tier1={"tier_name":"New Charges (incl GST)","tier_sort":"20","remit_sort":null}
				            		constMappingResults.put(constMappingKey, constMappingVal);
				            	}
				            }
							// System.out.println("constant mapping result: " + constMappingResults);
						}
						
						// 2.5 do mapping according to recipe and generate a new JSON object as output
						JSONArray mappingFields = (JSONArray) curMappingRecipe.get("fields");
						Iterator<JSONObject> mappingFieldsIt = mappingFields.iterator();
						while(mappingFieldsIt.hasNext()) {
							// 2.5.1 get each mapping field
							JSONObject mappingField = mappingFieldsIt.next();
							String mappingFieldName = (String) mappingField.get("name");
							String mappingFieldSrc = (String) mappingField.get("src");
							if (mappingFieldSrc.equals("constant")) {
								// constant value
								mappedResultObj.put(mappingFieldName, mappingField.get("val"));
							} else if (mappingFieldSrc.startsWith("constant_tables")) {
								// value from constant tables
								// get the value part from constMappingResults hash
								JSONObject valFromConstTables = constMappingResults.get(mappingField.get("src"));
								mappedResultObj.put(mappingFieldName, valFromConstTables.get(mappingField.get("val")));
							} else {
								// from current source record
								// by default the source record is curRecordObj
								mappedResultObj.put(mappingFieldName, curRecordObj.get(mappingField.get("src_field")));
							}
						}
						System.out.println("key: " + secCode + "--" + curMappingRecipe.get("report") + " obj: " + mappedResultObj);
						recordKey = secCode + "--" + curMappingRecipe.get("report");
						selected_line = mappedResultObj.toJSONString();
					}
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return new MappingKeyValPair(recordKey, selected_line);

	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 0. initialize the cache for constant_tables
		if (constTables == null) {
			System.out.println("get constant_tables ready only once.");
			constTables = Util.getConstantTables("./recipes/constant_tables.json");
		}

		// retrieve the entire mapping recipe
		if (mappingRecipe == null) {
			mappingRecipe = (JSONObject) Util.readJSONFromFile("./recipes/mapping.json");
			System.out.println("get the mapping recipe only once");
			// System.out.println(mappingRecipe);
		}

		String line = value.toString();
		MappingKeyValPair result = doRecipeMapping(line);
		if(result == null)
			return;
				
		context.write(new Text(result.recordKey), new Text(result.selectedLine));
	}
}

class MappingKeyValPair{
	String recordKey;
	String selectedLine;
	MappingKeyValPair(String recordKey, String selectedLine) {
		this.recordKey = recordKey;
		this.selectedLine = selectedLine;
	}
}
