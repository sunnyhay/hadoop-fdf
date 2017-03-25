package com.gdf.fdf;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Util {
	private static JSONParser parser = new JSONParser();
	
	@SuppressWarnings("unchecked")
	public static JSONObject combineJsonObj(JSONObject appendObj, JSONObject originObj) {
		JSONObject targetObj = new JSONObject();
		Set<String> keys = originObj.keySet();
		
		for(String key: keys){
			targetObj.put(key, originObj.get(key));
		}
		
		keys = appendObj.keySet();
		for(String key: keys){
			targetObj.put(key, appendObj.get(key));
		}
		
		return targetObj;
	}
	
	@SuppressWarnings("unchecked")
	public static Map<String, String> getJsonObjMap(String str) throws ParseException {
		Map<String, String> map = new HashMap<>();
		JSONParser parser = new JSONParser();
		Object obj = parser.parse(str.toString());
		JSONObject jsonObj = (JSONObject) obj;
		Set<String> keys = jsonObj.keySet();
		
		for(String key: keys){
			map.put(key, (String) jsonObj.get(key));
		}

		return map;
	}
	
	@SuppressWarnings("unchecked")
	public static JSONObject convertJsonObj(String postfix, JSONObject obj) {
		JSONObject jsonObj = new JSONObject();
		Set<String> keys = obj.keySet();
		
		for(String key: keys){
			jsonObj.put(key + postfix, obj.get(key));
		}
		
		return jsonObj;
	}
	
	public static JSONObject readJSONFromFile(String filename) {
		Object obj = null;
		try {
			obj = parser.parse(new FileReader(filename));
		} catch (IOException | ParseException e) {
			System.out.println("Util.readJSONFromFile: error in parsing.");
			e.printStackTrace();
		}
		JSONObject jsonObj = (JSONObject) obj;
		return jsonObj;
	}
	
	@SuppressWarnings("unchecked")
	public static Map<String, JSONObject> getConstantTables(String filename) {
		Map<String, JSONObject> constTableMap = new HashMap<>();
		Object obj;
		try {
			obj = parser.parse(new FileReader(filename));
			JSONObject constantTablesObj = (JSONObject) obj;
			JSONArray constantTables = (JSONArray) constantTablesObj.get("constant_tables");
			
			Iterator<JSONObject> it = constantTables.iterator();
			while (it.hasNext()) {
				JSONObject table = it.next();
				String tableName = (String) table.get("name");
				JSONArray tableContent = (JSONArray) table.get("content");
				JSONObject tableObj = new JSONObject();
				
				
				Iterator<JSONObject> mappingIt = tableContent.iterator();
				while(mappingIt.hasNext()) {
					JSONObject item = mappingIt.next();
					JSONObject itemBody = new JSONObject();
					String itemLabel = (String) item.get("tier_label");
					itemBody.put("tier_name", item.get("tier_name"));
					itemBody.put("tier_sort", item.get("tier_sort"));
					itemBody.put("remit_sort", item.get("remit_sort"));
					tableObj.put(itemLabel, itemBody);
				}
				
				constTableMap.put(tableName, tableObj);
			}
			
		} catch (ParseException e) {
			System.out.println("error in initializing constant_tables cache.");
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return constTableMap;
	}
	

	public static void main(String[] args) throws ParseException {
		/*
		String str = "{\"Doc_Header_Record_Code\":\"1\",\"Hier_ID\":\"3937764900\",\"Gen_Business_Arrangement_Txt\":\"3815418020\",\"Business_Arrangement_Type\":\"I\",\"Doc_Ref_No\":\"P030931801\",\"Finance_Company_Code\":\"SMEF\",\"Finance_Centre\":\"MGRMEM\",\"Relative_Doc_No\":\"000000001\",\"Sec_Array\":\"HAR6KV2BUSEN1LOQYICD\",\"GBA_Check_Digit\":\"9\",\"CIDN\":\"6805679926\",\"Repository_Flag\":\"R\",\"line_no\":\"2\"}";
		Map<String, String> map = getJsonObjMap(str);
		for (String key : map.keySet())
			System.out.println(key + " : " + map.get(key));
		*/
		/*
		String str = "{\"Doc_Header_Record_Code\":\"1\",\"Hier_ID\":\"3937764900\",\"Gen_Business_Arrangement_Txt\":\"3815418020\",\"Business_Arrangement_Type\":\"I\",\"Doc_Ref_No\":\"P030931801\",\"Finance_Company_Code\":\"SMEF\",\"Finance_Centre\":\"MGRMEM\",\"Relative_Doc_No\":\"000000001\",\"Sec_Array\":\"HAR6KV2BUSEN1LOQYICD\",\"GBA_Check_Digit\":\"9\",\"CIDN\":\"6805679926\",\"Repository_Flag\":\"R\",\"line_no\":\"2\"}";
		JSONParser parser = new JSONParser();
		Object obj = parser.parse(str.toString());
		JSONObject jsonObj = (JSONObject) obj;
		String postfix = "_R1";
		
		JSONObject obj2 = convertJsonObj(postfix, jsonObj);
		Set<String> keys = obj2.keySet();
		
		for(String key: keys){
			System.out.println(key + " : " + obj2.get(key));
		}
		*/
		/*
		String str = "{\"Doc_Header_Record_Code\":\"1\",\"Hier_ID\":\"3937764900\",\"Gen_Business_Arrangement_Txt\":\"3815418020\",\"Business_Arrangement_Type\":\"I\",\"Doc_Ref_No\":\"P030931801\",\"Finance_Company_Code\":\"SMEF\",\"Finance_Centre\":\"MGRMEM\",\"Relative_Doc_No\":\"000000001\",\"Sec_Array\":\"HAR6KV2BUSEN1LOQYICD\",\"GBA_Check_Digit\":\"9\",\"CIDN\":\"6805679926\",\"Repository_Flag\":\"R\",\"line_no\":\"2\"}";
		String str1 = "{\"X_R1\":\"jerry\"}";
		JSONParser parser = new JSONParser();
		Object obj = parser.parse(str);
		JSONObject jsonObj1 = (JSONObject) obj;
		Object obj2 = parser.parse(str1);
		JSONObject jsonObj2 = (JSONObject) obj2;
		JSONObject resultObj = combineJsonObj(jsonObj2, jsonObj1);
		
		Set<String> keys = resultObj.keySet();
		
		for(String key: keys){
			System.out.println(key + " : " + resultObj.get(key));
		}*/
		
		String filename = "./recipes/constant_tables.json";
		Map<String, JSONObject> constTableMap = getConstantTables(filename);
		for(String key: constTableMap.keySet()) {
			System.out.println(key + " : " + constTableMap.get(key));
		}
	}

}
