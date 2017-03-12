package com.gdf.fdf;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GDFParserMap extends Mapper<LongWritable, Text, Text, Text> {
	String[] x = {"X", "R0", "R1", "R3", "R6", "R9"};
	String curDocRefNo = "others";
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		Set<String> set = new HashSet<>();
		for(String e : x)
			set.add(e);
		
		String line = value.toString();
		String configFile = "./fdf.json";
		Map<String, List<ParserComponent>> dict = ParserUtil.buildParsingRule(configFile);
		String sectionCode = ParserUtil.getSectionCode(line);
		
		if (sectionCode.startsWith("M") && !sectionCode.equals("M"))
			return;
		
		List<ParserComponent> rule = dict.get(sectionCode);
		if (!dict.keySet().contains(sectionCode)) {
			System.out.println("no such section code: " + sectionCode);
			return;
		}
		
		StringBuffer sb = new StringBuffer();
		sb.append("{\"");
		
		// append section_code to R0, R1, R3, R6, R9 and X
		if (set.contains(sectionCode)) {
			sb.append("Sec_Code\":\"" + sectionCode + "\",\"");
		}
		
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
		// attach current Doc_Ref_No into the JSON string since R6 and X has no such element.
		if (sectionCode.equals("X") || sectionCode.equals("R6")) {
			sb.append("Doc_Ref_No\":\"" + curDocRefNo + "\",\"");
		}
		
		// System.out.println();
		sb.deleteCharAt(sb.length() - 1);
		sb.deleteCharAt(sb.length() - 1);
		sb.append("}");
		
		
		System.out.println("in mapper current doc_ref_no: " + curDocRefNo + " with content: " + sb.toString());
		context.write(new Text(curDocRefNo), new Text(sb.toString()));
	}
}
