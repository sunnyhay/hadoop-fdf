package com.gdf.fdf;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonParser {
	public static ParsingRuleMap getRuleMap(String file) {
		JSONParser parser = new JSONParser();
		ParsingRuleMap map = new ParsingRuleMap();

		try {
			Object obj = parser.parse(new FileReader(file));
			JSONObject jsonObj = (JSONObject) obj;
			JSONObject descElem = null;
			// System.out.println(jsonObj);

			@SuppressWarnings("unchecked")
			Set<String> keySet = jsonObj.keySet();
			for (String key : keySet) {
				// System.out.println("key: " + key + " with val: " +
				// jsonObj.get(key));
				Object elemObj = jsonObj.get(key);
				if (elemObj instanceof String) {
					String elem = (String) elemObj;
					map.feedName = elem;
					// System.out.println(elem);
				} else if (elemObj instanceof JSONObject) {
					descElem = (JSONObject) elemObj;
					// System.out.println(descElem);
				} else {
					JSONArray elem = (JSONArray) elemObj;
					@SuppressWarnings("unchecked")
					Iterator<JSONObject> it = elem.iterator();
					List<ParserComponent> list = new ArrayList<>();
					while (it.hasNext()) {
						JSONObject component = it.next();
						ParserComponent pCom = new ParserComponent((long) component.get("len"),
								(String) component.get("name"), (long) component.get("start"));
						list.add(pCom);
						// System.out.println("len: " + pCom.getLen() + " name:
						// " + pCom.getName() + " start: " + pCom.getStart());
					}
					map.componentMap.put(key, list);
				}
			}
			for (String key1 : map.componentMap.keySet()) {
				String val = (String) descElem.get(key1);
				if (val != null) {
					// System.out.println("key: " + key1 + " val: " + val);
					map.descMap.put(key1, val);
				}
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return map;
	}

	public static void main(String[] args) {
		String file = "./fdf.json";
		ParsingRuleMap map = getRuleMap(file);
		System.out.println(map.feedName);
		for (String key : map.componentMap.keySet())
			System.out.println(key);
		for (String val : map.descMap.values())
			System.out.println(val);
	}

}

class ParsingRuleMap {
	String feedName;
	Map<String, String> descMap;
	Map<String, List<ParserComponent>> componentMap;

	public ParsingRuleMap() {
		descMap = new HashMap<>();
		componentMap = new HashMap<>();
	}
}
