package com.hp.hplc.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class PartitionAndIndexMapping {
	
	private HashMap<Integer, List<String>> mapping = new HashMap<Integer, List<String>>();
	
	public void addMapping(int partition, String indexNodeName){
		List<String> list = mapping.get(partition);
		if(list == null){
			list = new ArrayList<String>();
		}
		list.add(indexNodeName);	
		mapping.put(partition, list);
	}
	
	public void setMapping(HashMap<Integer, List<String>> mapping){
		this.mapping = mapping;
	}
	
	public List<String> getMapping(int partition){
		return mapping.get(partition);
	}
	
	public String pack(){
		 StringBuffer buf = new StringBuffer();
	      
		 Set<Integer> set = mapping.keySet();
		 Iterator<Integer> it = set.iterator();
		 while(it.hasNext()){
			 Integer key = it.next();
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
	
	public HashMap<Integer, List<String>> unpack(String mappingStr){
		HashMap<Integer, List<String>> newMapping = new HashMap<Integer, List<String>>();
		String[] lines = mappingStr.split("\n");
		for(int i=0; i<lines.length; i++){
			String[] strs = lines[i].split("#");
			if(strs.length>1){
				int partition = Integer.parseInt(strs[0]);
				String[] values = strs[1].split(",");
				List<String> list = new ArrayList<String>();
				for(int j = 0; j < values.length; j++){
					list.add(values[j]);
				}
				newMapping.put(partition, list);
			}			
		}
		return newMapping;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PartitionAndIndexMapping paim = new PartitionAndIndexMapping();
		
		paim.addMapping(0, "Node1");
		paim.addMapping(0, "Node2");
		paim.addMapping(1, "Node1");
		paim.addMapping(1, "Node2");
		paim.addMapping(2, "Node1");
		paim.addMapping(2, "Node2");
		
		String str = paim.pack();
		System.out.println(str);
		paim.setMapping(paim.unpack(str));
		System.out.println(paim.pack());
		

	}

}
