package com.hp.hplc.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.jobconf.IndexJobConf;

public class InputAndIndexMapping {
	
	private HashMap<String, List<String>> mapping = new HashMap<String, List<String>>();
	
	
	public void addMapping(String inputFileName, String indexNodeName){
		List<String> list = mapping.get(inputFileName);
		if(list == null){
			list = new ArrayList<String>();
		}
		list.add(indexNodeName);	
		mapping.put(inputFileName, list);
	}
	
	public void setMapping(HashMap<String, List<String>> mapping){
		this.mapping = mapping;
	}
	
	public List<String> getMapping(String inputFileName){
		return mapping.get(inputFileName);
	}
	
	public String pack(){
		 StringBuffer buf = new StringBuffer();
	      
		 Set<String> set = mapping.keySet();
		 Iterator<String> it = set.iterator();
		 while(it.hasNext()){
			 String key = it.next();
			 buf.append(key + "#");
			 List<String> list = mapping.get(key);
			 if(list != null){
				 Iterator<String> itValue = list.iterator();
				 while(itValue.hasNext()){
					 String value = itValue.next();
					 buf.append(value + ",");
				 }
			 }
			 buf.append("\n");
		 }
	      return buf.toString();
	}
	
	public HashMap<String, List<String>> unpack(String mappingStr){
		HashMap<String, List<String>> newMapping = new HashMap<String, List<String>>();
		String[] lines = mappingStr.split("\n");
		for(int i=0; i<lines.length; i++){
			String[] strs = lines[i].split("#");
			if(strs.length>1){
				String inputFileName = strs[0];
				String[] values = strs[1].split(",");
				List<String> list = new ArrayList<String>();
				for(int j = 0; j < values.length; j++){
					list.add(values[j]);
				}
				newMapping.put(inputFileName, list);
			}			
		}
		return newMapping;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		InputAndIndexMapping inim = new InputAndIndexMapping();
		
		inim.addMapping("File1", "Node1");
		inim.addMapping("File1", "Node2");
		inim.addMapping("File2", "Node1");
		inim.addMapping("File2", "Node2");
		inim.addMapping("File2", "Node2");
		inim.addMapping("File3", "Node3");
		
		System.out.println(inim.pack());
		
		inim.setMapping(inim.unpack(inim.pack()));
		
		System.out.println(inim.pack());

	}

}
