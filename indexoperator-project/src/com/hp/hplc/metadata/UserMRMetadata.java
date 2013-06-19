package com.hp.hplc.metadata;

import java.util.Iterator;
import java.util.List;

import org.jdom.Element;

public class UserMRMetadata {
	public static final String USER_MAPREDUCE = "user_mapreduce";
	public static final String USER_MR_NAME = "user_mr_name";
	public static final String USER_MR_PROD = "user_mr_productivity";
	public static final String USER_MR_AVGSIZE = "user_mr_avgsize";

	private String mrName;
	private double productivity;
	private int avgSize;
	
	public UserMRMetadata(String name){
		this.mrName = name;
	}
	
	public UserMRMetadata(String name, double productivity, int avgSize){
		this.mrName = name;
		this.productivity = productivity;
		this.avgSize = avgSize;
	}

	public String getMrName() {
		return mrName;
	}

	public void setMrName(String mrName) {
		this.mrName = mrName;
	}

	public double getProductivity() {
		return productivity;
	}

	public void setProductivity(double productivity) {
		this.productivity = productivity;
	}
	
	public int getAvgSize(){
		return avgSize;
	}
	
	public void setAvgSize(int avgSize){
		this.avgSize = avgSize;
	}
	
	
	public void composeXmlElement(Element idxOpChapter) {
		Element idxOpElement = new Element(USER_MAPREDUCE);
		idxOpChapter.addContent(idxOpElement);
		
		idxOpElement.setAttribute(USER_MR_NAME,this.mrName);
		idxOpElement.setAttribute(USER_MR_PROD, Double.toString(this.productivity));
		idxOpElement.setAttribute(USER_MR_AVGSIZE, Integer.toString(this.avgSize));
	}
	
	public static UserMRMetadata buildMetadata(Element userMR) {
		String mrNameStr = userMR.getAttributeValue(USER_MR_NAME);
		double dprod = Double.parseDouble(userMR.getAttributeValue(USER_MR_PROD));
		int iavgSize = Integer.parseInt(userMR.getAttributeValue(USER_MR_AVGSIZE));
		UserMRMetadata mrMeta = new UserMRMetadata(mrNameStr, dprod, iavgSize);
		
		return mrMeta;
	}

}
