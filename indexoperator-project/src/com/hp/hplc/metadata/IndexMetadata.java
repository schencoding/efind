package com.hp.hplc.metadata;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;

import org.jdom.Element;

public class IndexMetadata {
	public static final String INDEX = "index";
	public static final String INDEX_INPUT_KEY_CLASS = "index_input_key_class";
	public static final String INDEX_OUTPUT_VALUE_CLASS = "index_output_value_class";
	public static final String INDEX_NAME = "index_name";
	private String idxName;
	
	private Class inputKeyClass;
	private Class outputValueClass;
	private int avgKeySize = 0;
	private int avgValueSize = 0;
	
	public void print() {
		System.out.println("---------" + this.idxName + "----------");
		System.out.println("\t" + "inputKeyClass = " + inputKeyClass);
		System.out.println("\t" + "outputValueClass = " + outputValueClass);
		System.out.println("\t" + "avgKeySize = " + avgKeySize);
		System.out.println("\t" + "avgValueSize = " + avgValueSize);
	}
	
	private Map<String, IndexPartition> idxPartitionMapping= new HashMap<String, IndexPartition>();
	
	public IndexMetadata(){
		
	}
	
	public IndexMetadata(String idxName){
		this.idxName = idxName;
	}
	
	public void addPartitionMapping(String key, IndexPartition partition){
		idxPartitionMapping.put(key, partition);
	}
	
	public IndexPartition getPartition(String key){
		return idxPartitionMapping.get(key);		
	}	
	
	public String getIdxName() {
		return idxName;
	}

	public void setIdxName(String idxName) {
		this.idxName = idxName;
	}

	public Class getInputKeyClass() {
		return inputKeyClass;
	}

	public void setInputKeyClass(Class inputKeyClass) {
		this.inputKeyClass = inputKeyClass;
	}

	public Class getOutputValueClass() {
		return outputValueClass;
	}

	public void setOutputValueClass(Class outputValueClass) {
		this.outputValueClass = outputValueClass;
	}

	public Map<String, IndexPartition> getIdxPartitionMapping() {
		return idxPartitionMapping;
	}
	
	

	public void setIdxPartitionMapping(Map<String, IndexPartition> idxPartitionMapping) {
		this.idxPartitionMapping = idxPartitionMapping;
	}
	
	
	
	public int getAvgKeySize() {
		return avgKeySize;
	}

	public void setAvgKeySize(int avgKeySize) {
		this.avgKeySize = avgKeySize;
	}

	public int getAvgValueSize() {
		return avgValueSize;
	}

	public void setAvgValueSize(int avgValueSize) {
		this.avgValueSize = avgValueSize;
	}

	@Override
	public String toString() {
		String result = "";
		result += idxName + ":";
		Object[] a = idxPartitionMapping.values().toArray();
		for(int i=0; i<idxPartitionMapping.size(); i++){
			result += ((IndexPartition)a[i]).getPartitionId();
			if(i<idxPartitionMapping.size()-1){
				result += ",";
			}
		}
		return result;
	}

	public String pack(){
		String result = "";
		Object[] a = idxPartitionMapping.values().toArray();
		for(int i=0; i<idxPartitionMapping.size(); i++){
			result += ((IndexPartition)a[i]).toString();
			if(i<idxPartitionMapping.size()-1){
				result += "#";
			}
		}
		
		return result;
	}




	public class IndexPartition{
		private int partitionId;
		private String[] hosts;
		
		public IndexPartition(){
			
		}
		
		public IndexPartition(int partitionId, String[] hosts){
			this.partitionId = partitionId;
			this.hosts = hosts;
		}

		public int getPartitionId() {
			return partitionId;
		}

		public void setPartitionId(int partitionId) {
			this.partitionId = partitionId;
		}

		public String[] getHosts() {
			return hosts;
		}

		public void setHosts(String[] hosts) {
			this.hosts = hosts;
		}		
		
		
		
		@Override
		public String toString() {
			String result = "";

			result += hosts.length + ":";
			for (int i = 0; i < hosts.length; i++) {
				result += hosts[i];
				if (i < hosts.length - 1) {
					result += ",";
				}
			}

			return result;
		}

		public String pack(){
			String result = "";
			
			result += hosts.length + ":";
			for(int i=0; i< hosts.length; i++){
				result += hosts[i];
				if(i<hosts.length-1){
					result += ",";
				}
			}
			
			return result;
		}
	}




	public void composeXmlElement(Element idxChapter) {
		Element idxElement = new Element(INDEX);
		idxChapter.addContent(idxElement);
		idxElement.setAttribute(INDEX_NAME, this.idxName);
		Element indexKeyClsElement = new Element(this.INDEX_INPUT_KEY_CLASS);
		indexKeyClsElement.addContent(this.inputKeyClass.getName().toString());
		idxElement.addContent(indexKeyClsElement);
		
		Element indexValueClsElement = new Element(this.INDEX_OUTPUT_VALUE_CLASS);
		indexValueClsElement.addContent(this.outputValueClass.getName().toString());
		idxElement.addContent(indexValueClsElement);
		
	}

	public static IndexMetadata buildMetadata(Element idx) throws ClassNotFoundException {
		String indexName = idx.getAttributeValue(INDEX_NAME);
		IndexMetadata idxMeta = new IndexMetadata(indexName);
		Element indexKeyClsElement = idx.getChild(INDEX_INPUT_KEY_CLASS);
		String keyClsName = indexKeyClsElement.getText();
		Class inputKeyClass = Class.forName(keyClsName);
		idxMeta.setInputKeyClass(inputKeyClass);
		
		Element indexValueClsElement = idx.getChild(INDEX_OUTPUT_VALUE_CLASS);
		String valueClsName = indexValueClsElement.getText();
		Class outputValueClass = Class.forName(valueClsName);
		idxMeta.setOutputValueClass(outputValueClass);
		
		return idxMeta;
	}



	
	
	
	
	
	
	
	
	
	
	
	
	

	

}
