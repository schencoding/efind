package com.hp.hplc.indeximpl.iplookup;

import java.io.Serializable;

public class IPRange implements Serializable{
	private static final long serialVersionUID = 7714025235350268366L;
	private int start, end;
	private String info;
	
	public IPRange(int start, int end, String info) {
		this.start = start;
		this.end = end;
		this.info = info;
	}
	
	public void setStart(int start) {
		this.start = start;
	}
	
	public int getStart() {
		return (start);
	}
	
	public void setEnd(int end) {
		this.end = end;
	}
	
	public int getEnd() {
		return (end);
	}
	
	public void setInfo(String info) {
		this.info = info;
	}
	
	public String getInfo() {
		return (info);
	}
}
