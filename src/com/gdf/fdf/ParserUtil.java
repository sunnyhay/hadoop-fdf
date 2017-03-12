package com.gdf.fdf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONObject;

public class ParserUtil {
	private static final String[] exclude = { "R0", "R1", "R3", "R6", "R9", "X" };
	private static final String R1POSTFIX = "_R1", HPOSTFIX = "_H";
	private static final Set<String> EXCLUDE_COMMON = new HashSet<>(Arrays.asList(exclude));

	// build the parsing rule
	public static Map<String, List<ParserComponent>> buildParsingRule(String file) {
		Map<String, List<ParserComponent>> dict = new HashMap<>();

		ParsingRuleMap map = JsonParser.getRuleMap(file);
		Map<String, String> descMap = map.descMap;
		Map<String, List<ParserComponent>> comMap = map.componentMap;
		List<ParserComponent> common = comMap.get("Common");

		for (String key : descMap.keySet()) {
			if (key.startsWith("M") && !key.equals("M"))
				continue;

			List<ParserComponent> list = comMap.get(key);
			List<ParserComponent> l = new ArrayList<>();
			if (!EXCLUDE_COMMON.contains(key)) {
				l.addAll(common);
			}
			l.addAll(list);
			dict.put(key, l);
		}

		return dict;
	}

	// find the section code
	public static String getSectionCode(String line) {
		String recordType = line.substring(0, 1);
		if (!recordType.equals("2"))
			return "R" + recordType;

		// Determine subtype and return
		String markerType = line.substring(20, 21).toUpperCase();
		String sectionCode = line.substring(21, 22).toUpperCase();
		String dataType = line.substring(22, 23).toUpperCase();

		// Marker Record
		if (markerType.equals("M"))
			return "M" + dataType;

		// Enquiry Section
		if (sectionCode.equals("6"))
			return "R6";

		// Bill Line
		if (dataType.equals("X"))
			return "X";

		// Data section
		return sectionCode;
	}

	public static void processByDocRefNo(String filename, String configFile) {
		// rule dictionary
		Map<String, List<ParserComponent>> dict = buildParsingRule(configFile);
		int lineNo = 0;
		String path = "./input/";
		Map<String, PrintWriter> map = new HashMap<>();
		String curDocRefNo = "others";

		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			String line;
			while ((line = br.readLine()) != null) {
				String sectionCode = getSectionCode(line);

				if (lineNo % 10000 == 0)
					System.out.println("processed data lines " + lineNo);

				lineNo++;

				// System.out.println(sectionCode);
				if (sectionCode.startsWith("M") && !sectionCode.equals("M"))
					continue;
				List<ParserComponent> rule = dict.get(sectionCode);
				// System.out.println("section code: " + sectionCode + " in
				// line: " + lineNo);
				if (!dict.keySet().contains(sectionCode)) {
					System.out.println("no such section code: " + sectionCode);
					continue;
				}

				StringBuffer sb = new StringBuffer();
				sb.append("{\"");
				for (ParserComponent pCom : rule) {
					// System.out.println("start: " + pCom.getStart() + " len: "
					// + pCom.getLen());
					int len = (int) pCom.getStart() + (int) pCom.getLen();
					int start = (int) pCom.getStart();
					// System.out.print(pCom.getName() + " : " +
					// line.substring(start, len > line.length() ? line.length()
					// : len).trim() + " , ");
					sb.append(pCom.getName());
					sb.append("\":\"");
					String val = line.substring(start, len > line.length() ? line.length() : len).trim();
					sb.append(val);
					if (sectionCode.equals("R1") && pCom.getName().equals("Doc_Ref_No")) {
						curDocRefNo = val;
					}
					sb.append("\",\"");
				}
				sb.append("line_no\":\"" + lineNo + "\"");
				// System.out.println();
				// sb.deleteCharAt(sb.length() - 1);
				// sb.deleteCharAt(sb.length() - 1);
				sb.append("}");
				// System.out.println(sb.toString());

				PrintWriter pw;
				if (map.containsKey(curDocRefNo + sectionCode))
					pw = map.get(curDocRefNo + sectionCode);
				else {
					String dirName = path + "/" + curDocRefNo + "/";
					File directory = new File(String.valueOf(dirName));
					if (!directory.exists()) {
						directory.mkdir();
					}

					pw = new PrintWriter(new BufferedWriter(new FileWriter(dirName + sectionCode + ".json", true)));
					map.put(curDocRefNo + sectionCode, pw);
				}
				pw.println(sb.toString());
			}
			System.out.println("total processed data line: " + lineNo);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			for (String key : map.keySet()) {
				map.get(key).close();
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static void processAsWhole(String filename, String configFile) {
		String[] x = { "X", "R0", "R1", "R3", "R6", "R9" };
		Set<String> set = new HashSet<>();
		for (String e : x)
			set.add(e);

		// rule dictionary
		Map<String, List<ParserComponent>> dict = buildParsingRule(configFile);
		int lineNo = 0;
		String path = "./inputwhole/";
		PrintWriter pw = null;
		JSONObject curR1Obj = new JSONObject();
		JSONObject curHObj = new JSONObject();

		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			pw = new PrintWriter(new BufferedWriter(new FileWriter(path + filename + ".json", true)));
			String line;
			while ((line = br.readLine()) != null) {
				String sectionCode = getSectionCode(line);

				if (lineNo % 10000 == 0)
					System.out.println("processed data lines " + lineNo);

				lineNo++;

				// System.out.println(sectionCode);
				// skip marker records
				if (sectionCode.startsWith("M") && !sectionCode.equals("M"))
					continue;

				// skip R0 records
				if (sectionCode.equals("R0")) {
					System.out.println("skip R0 record in line " + lineNo);
					continue;
				}

				List<ParserComponent> rule = dict.get(sectionCode);
				// System.out.println("section code: " + sectionCode + " in
				// line: " + lineNo);
				if (!dict.keySet().contains(sectionCode)) {
					System.out.println("no such section code: " + sectionCode);
					continue;
				}

				JSONObject obj = new JSONObject();

				// append section_code to R0, R1, R3, R6, R9 and X
				if (set.contains(sectionCode)) {
					obj.put("Sec_Code", sectionCode);
				}

				for (ParserComponent pCom : rule) {
					// System.out.println("start: " + pCom.getStart() + " len: "
					// + pCom.getLen());
					int len = (int) pCom.getStart() + (int) pCom.getLen();
					int start = (int) pCom.getStart();
					// System.out.print(pCom.getName() + " : " +
					// line.substring(start, len > line.length() ? line.length()
					// : len).trim() + " , ");

					String val = line.substring(start, len > line.length() ? line.length() : len).trim();
					obj.put(pCom.getName(), val);
				}
				obj.put("Line_No", "" + lineNo);

				// append current R1
				if (sectionCode.equals("R1")) {
					curR1Obj = obj;
				} else {
					JSONObject postfixObj = Util.convertJsonObj(R1POSTFIX, curR1Obj);
					obj = Util.combineJsonObj(obj, postfixObj);
				}

				// append current H
				if (sectionCode.equals("H")) {
					curHObj = obj;
				} else if (!sectionCode.equals("R1")) {
					JSONObject postfixObj = Util.convertJsonObj(HPOSTFIX, curHObj);
					obj = Util.combineJsonObj(obj, postfixObj);
				}

				pw.println(obj.toJSONString());
				// pw.println(sb.toString());
			}
			System.out.println("total processed data line: " + lineNo);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			pw.close();
		}
	}

	public static void main(String[] args) {

		String file = "./fdf.json";
		// String dataFile = "./fdfpbill.20161205.02.00";
		String dataFile = "./fdfpbill.20161205.04.00";
		processAsWhole(dataFile, file);

	}

}
