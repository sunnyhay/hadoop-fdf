package com.gdf.fdf;

class ParserComponent {
	private long len;
	private String name;
	private long start;
	
	public ParserComponent(long len, String name, long start) {
		this.len = len;
		this.name = name;
		this.start = start;
	}
	
	public long getLen() {
		return len;
	}
	public void setLen(long len) {
		this.len = len;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public long getStart() {
		return start;
	}
	public void setStart(long start) {
		this.start = start;
	}
	
}
