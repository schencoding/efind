package com.hp.hplc.metadata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.jdom.Element;

public class IndexOperatorMetadata {
	
	public static final String INDEX_OPERATOR = "Index_Operator";
	public static final String INDEX_OPERATOR_NAME = "Index_Operator_Name";
	public static final String B_META_AVAILABLE = "bMetaAvailable";
	public static final String NUM_OF_INPUT_RECORD = "numOfInputRecord";
	public static final String SIZE_OF_INPUT_RECORD = "sizeOfInputRecord";
	public static final String SELE_OF_PRE_PROC = "seleOfPrePro";
	public static final String SIZE_OF_VVALUE_AFTER_PRE_PROC = "sizeOfValueAfterPrePro";
	public static final String NUM_OF_INDEXES = "numOfIndexes";
	public static final String NUM_OF_INDEX_KEY_PER_RECORD = "numOfIndexKeyPerRecord";
	public static final String SIZE_OF_INDEX_KEY = "sizeOfIndexKey";
	public static final String SIZE_OF_INDEX_VALUE = "sizeOfIndexValue";
	public static final String CARDINALITY_OF_INDEX_KEY = "cardinalityOfIndexKey";
	public static final String INDEX_STAT = "index_stat";
	public static final String INDEX_ID = "index_ID";
	public static final String AVG_INDEX_LOOKUP_TIME = "avg_indexlookup_time";
	public static final String CACHE_HIT_RATE = "cache_hit_rate";
	
	public static final String POST_PRODUCTIVITY = "post_productivity";
	public static final String POST_AVGSIZE = "post_avgsize";

	private String 	idxOpName;
	private boolean bMetaAvailable = false;
	private long 	numOfInputRecord;
	private int 	sizeOfInputRecord;
	private double 	seleOfPrePro = 1.0;
	private int 	sizeOfValueAfterPrePro;
	private double 	postProductivity = 1.0;
	private int		sizeOfValueAfterPostPro;
	
	
	private int numOfIndexes = 1;
	
	private List<Integer> numOfIndexKeyPerRecord = new ArrayList<Integer>(5);	
	private List<Integer> sizeOfIndexKey = new ArrayList<Integer>(5);
	private List<Integer> sizeOfIndexValue = new ArrayList<Integer>(5);	
	private List<Integer> cardinalityOfIndexKey = new ArrayList<Integer>(5);
	private List<Double> avgIndexLookupTime = new ArrayList<Double>(5);
	private List<Double> cacheHitRate = new ArrayList<Double>(5);
	
	private List<String> indexPartitionKeys = new ArrayList<String>(5);
	private List<String> indexPartitionFunctions = new ArrayList<String>(5);
	
	//for parallel index operator
	private List<Double> selectivityOfIdxPost = new ArrayList<Double>(5);
	private List<Double> productivityOfIdxPost = new ArrayList<Double>(5);
	private List<Integer> sizeOfIdxPostOutputRecord = new ArrayList<Integer>(5);
	

	private double productivity = 1.0;
	private int sizeOfOutputRecord;

	
	
	

	
	public void print(){
		System.out.println("------" + idxOpName + "-------");
		System.out.println("\t" + "sizeOfInputRecord = " + sizeOfInputRecord);
		System.out.println("\t" + "seleOfPrePro = " + seleOfPrePro);
		System.out.println("\t" + "sizeOfValueAfterPrePro = " + sizeOfValueAfterPrePro);
		
		for(int index=0; index < numOfIndexes; index++){
			System.out.println("\t\tindex " + index + " numOfIndexKeyPerRecord = " + numOfIndexKeyPerRecord.get(index));
			System.out.println("\t\tindex " + index + " sizeOfIndexKey = " + sizeOfIndexKey.get(index));
			System.out.println("\t\tindex " + index + " sizeOfIndexValue = " + sizeOfIndexValue.get(index));
		}
	}

	public IndexOperatorMetadata(String name) {
		this.idxOpName = name;
	}

	public String getIdxOpName() {
		return idxOpName;
	}

	public void setIdxOpName(String idxOpName) {
		this.idxOpName = idxOpName;
	}

	public long getNumOfInputRecord() {
		return numOfInputRecord;
	}

	public void setNumOfInputRecord(long numOfInputRecord) {
		this.numOfInputRecord = numOfInputRecord;
	}
	
	public int getSizeOfInputRecord() {
		return sizeOfInputRecord;
	}

	public void setSizeOfInputRecord(int sizeOfInputRecord) {
		this.sizeOfInputRecord = sizeOfInputRecord;
	}

	public double getSeleOfPrePro() {
		return seleOfPrePro;
	}

	public void setSeleOfPrePro(double seleOfPrePro) {
		this.seleOfPrePro = seleOfPrePro;
	}

	public int getSizeOfValueAfterPrePro() {
		return sizeOfValueAfterPrePro;
	}

	public void setSizeOfValueAfterPrePro(int sizeOfValueAfterPrePro) {
		this.sizeOfValueAfterPrePro = sizeOfValueAfterPrePro;
	}

	public int getNumOfIndexKeyPerRecord(int index) {
		return numOfIndexKeyPerRecord.get(index);
	}

	public void setNumOfIndexKeyPerRecord(int index, int numOfIndexKeyPerRecord) {
		this.numOfIndexKeyPerRecord.set(index, numOfIndexKeyPerRecord);
	}

	public int getSizeOfIndexKey(int index) {
		return sizeOfIndexKey.get(index);
	}

	public void setSizeOfIndexKey(int index, int sizeOfIndexKey) {
		this.sizeOfIndexKey.set(index, sizeOfIndexKey);
	}

	public int getSizeOfIndexValue(int index) {
		return sizeOfIndexValue.get(index);
	}

	public void setSizeOfIndexValue(int index, int sizeOfIndexValue) {
		this.sizeOfIndexValue.set(index, sizeOfIndexValue);
	}

	public double getProductivity() {
		return productivity;
	}

	public void setProductivity(double productivity) {
		this.productivity = productivity;
	}

	public int getSizeOfOutputRecord() {
		return sizeOfOutputRecord;
	}

	public void setSizeOfOutputRecord(int sizeOfOutputRecord) {
		this.sizeOfOutputRecord = sizeOfOutputRecord;
	}

	public int getNumOfIndexes() {
		return numOfIndexes;
	}

	public void setNumOfIndexes(int numOfIndexes) {
		this.numOfIndexes = numOfIndexes;
		
		for(int i=0; i< numOfIndexes; i++){			
			indexPartitionKeys.add("");
			indexPartitionFunctions.add("");
			numOfIndexKeyPerRecord.add(1);
			cardinalityOfIndexKey.add(1);
			sizeOfIndexKey.add(0);
			sizeOfIndexValue.add(0);
			avgIndexLookupTime.add(0.0);
			cacheHitRate.add(0.0);
		}
	}
	
	public void setCacheHitRate(int index, double value){
		cacheHitRate.set(index, value);
	}
	
	public double getCacheHitRate(int index){
		return cacheHitRate.get(index);
	}

	public void setSelectivityOfIdxPost(int index, double value) {
		this.selectivityOfIdxPost.set(index, value);
	}

	public double getSelectivityOfIdxPost(int index) {
		return this.selectivityOfIdxPost.get(index);
	}

	public void setProductivityOfIdxPost(int index, double value) {
		this.productivityOfIdxPost.set(index, value);
	}

	public double getProductivityOfIdxPost(int index) {
		return this.productivityOfIdxPost.get(index);
	}

	public int getSizeOfIdxPostOutputRecord(int index) {
		return this.sizeOfIdxPostOutputRecord.get(index);
	}

	public void setSizeOfIdxPostOutputRecord(int index, int value) {
		this.sizeOfIdxPostOutputRecord.set(index, value);
	}
	
	public void setCardinalityOfIndexKey(int index, int value){
		this.cardinalityOfIndexKey.set(index, value);
	}
	
	public int getCardinalityOfIndexKey(int index){
		return this.cardinalityOfIndexKey.get(index);
	}
	
	public void setIndexPartitionKey(int index, String key){
		indexPartitionKeys.set(index, key);
	}
	public void setIndexPartitionFunction(int index, String function){
		indexPartitionFunctions.set(index, function) ;
	}
	
	public String getIndexPartitionKey(int index){
		return indexPartitionKeys.get(index);
	}
	
	public String getIndexPartitionFunction(int index){
		return indexPartitionFunctions.get(index);
	}
	
	public boolean checkIfMetaAvailable(){
		return bMetaAvailable;
	}
	
	public void setBMetaAvailalbe(boolean value){
		bMetaAvailable = value;
	}
	
	public void setAvgIndexLookupTime(int index, double timeCost) {
		avgIndexLookupTime.set(index,  timeCost);		
	}
	
	public double getAvgIndexLookupTime(int index){
		return avgIndexLookupTime.get(index);
	}
	
	public double getPostProductivity() {
		return postProductivity;
	}

	public void setPostProductivity(double postProductivity) {
		this.postProductivity = postProductivity;
	}

	public int getSizeOfValueAfterPostPro() {
		return sizeOfValueAfterPostPro;
	}

	public void setSizeOfValueAfterPostPro(int sizeOfValueAfterPostPro) {
		this.sizeOfValueAfterPostPro = sizeOfValueAfterPostPro;
	}



	public void composeXmlElement(Element idxOpChapter) {
		Element idxOpElement = new Element(INDEX_OPERATOR);
		idxOpChapter.addContent(idxOpElement);
		
		idxOpElement.setAttribute(INDEX_OPERATOR_NAME,this.idxOpName);
		idxOpElement.setAttribute(B_META_AVAILABLE, Boolean.toString(this.bMetaAvailable));
		idxOpElement.setAttribute(NUM_OF_INPUT_RECORD, Long.toString(this.numOfInputRecord));
		idxOpElement.setAttribute(SIZE_OF_INPUT_RECORD, Integer.toString(this.sizeOfInputRecord));
		idxOpElement.setAttribute(SELE_OF_PRE_PROC, Double.toString(this.seleOfPrePro));
		idxOpElement.setAttribute(SIZE_OF_VVALUE_AFTER_PRE_PROC, Integer.toString(this.sizeOfValueAfterPrePro));
		idxOpElement.setAttribute(NUM_OF_INDEXES, Integer.toString(this.numOfIndexes));
		idxOpElement.setAttribute(POST_PRODUCTIVITY, Double.toString(this.postProductivity));
		idxOpElement.setAttribute(POST_AVGSIZE, Integer.toString(this.sizeOfValueAfterPostPro));
		
		for(int index = 0; index < numOfIndexes; index ++){
			Element idxElement = new Element(INDEX_STAT);
			idxOpElement.addContent(idxElement);
			idxElement.setAttribute(INDEX_ID, Integer.toString(index));
			idxElement.setAttribute(NUM_OF_INDEX_KEY_PER_RECORD, Integer.toString(numOfIndexKeyPerRecord.get(index)));
			idxElement.setAttribute(SIZE_OF_INDEX_KEY, Integer.toString(this.sizeOfIndexKey.get(index)));
			idxElement.setAttribute(SIZE_OF_INDEX_VALUE, Integer.toString(this.sizeOfIndexValue.get(index)));
			idxElement.setAttribute(CARDINALITY_OF_INDEX_KEY, Integer.toString(this.cardinalityOfIndexKey.get(index)));
			idxElement.setAttribute(AVG_INDEX_LOOKUP_TIME, Double.toString(this.avgIndexLookupTime.get(index)));
			idxElement.setAttribute(CACHE_HIT_RATE, Double.toString(this.cacheHitRate.get(index)));
			
		}		
	}
	
	public static IndexOperatorMetadata buildMetadata(Element idxOp) {
		String idxOpName = idxOp.getAttributeValue(INDEX_OPERATOR_NAME);
		IndexOperatorMetadata idxOpMeta = new IndexOperatorMetadata(idxOpName);
		boolean bMetaAvailable = Boolean.parseBoolean(idxOp.getAttributeValue(B_META_AVAILABLE));
		idxOpMeta.setBMetaAvailalbe(bMetaAvailable);
		long numOfInputRecord = Long.parseLong(idxOp.getAttributeValue(NUM_OF_INPUT_RECORD));
		idxOpMeta.setNumOfInputRecord(numOfInputRecord);
		int sizeOfInputRecord = Integer.parseInt(idxOp.getAttributeValue(SIZE_OF_INPUT_RECORD));
		idxOpMeta.setSizeOfInputRecord(sizeOfInputRecord);
		double seleOfPreProc = Double.parseDouble(idxOp.getAttributeValue(SELE_OF_PRE_PROC));
		idxOpMeta.setSeleOfPrePro(seleOfPreProc);
		int sizeOfValueAfterPrePro= Integer.parseInt(idxOp.getAttributeValue(SIZE_OF_VVALUE_AFTER_PRE_PROC));
		idxOpMeta.setSizeOfValueAfterPrePro(sizeOfValueAfterPrePro);
		
		int numOfIndexes= Integer.parseInt(idxOp.getAttributeValue(NUM_OF_INDEXES));
		idxOpMeta.setNumOfIndexes(numOfIndexes);
		
		double dPostProd= Double.parseDouble(idxOp.getAttributeValue(POST_PRODUCTIVITY));
		idxOpMeta.setPostProductivity(dPostProd);
		
		int iAvgSizeAfterPostPro= Integer.parseInt(idxOp.getAttributeValue(POST_AVGSIZE));
		idxOpMeta.setSizeOfValueAfterPostPro(iAvgSizeAfterPostPro);
		
		List<Element> idxes = idxOp.getChildren();
		Iterator<Element> idxIt = idxes.iterator();
		while(idxIt.hasNext()){
			Element idx = idxIt.next();
			int index = Integer.parseInt(idx.getAttributeValue(INDEX_ID));
			int numOfIndexKeyPerRecord = Integer.parseInt(idx.getAttributeValue(NUM_OF_INDEX_KEY_PER_RECORD));
			idxOpMeta.setNumOfIndexKeyPerRecord(index, numOfIndexKeyPerRecord);
			int sizeOfIndexKey = Integer.parseInt(idx.getAttributeValue(SIZE_OF_INDEX_KEY));
			idxOpMeta.setSizeOfIndexKey(index, sizeOfIndexKey);
			int sizeOfIndexValue = Integer.parseInt(idx.getAttributeValue(SIZE_OF_INDEX_VALUE));
			idxOpMeta.setSizeOfIndexValue(index, sizeOfIndexValue);
			int cardinalityOfIndexKey = Integer.parseInt(idx.getAttributeValue(CARDINALITY_OF_INDEX_KEY));
			idxOpMeta.setCardinalityOfIndexKey(index, cardinalityOfIndexKey);
			double avgIndexLookupTime = Double.parseDouble(idx.getAttributeValue(AVG_INDEX_LOOKUP_TIME));
			idxOpMeta.setAvgIndexLookupTime(index, avgIndexLookupTime);
			double dCacheHitRate = Double.parseDouble(idx.getAttributeValue(CACHE_HIT_RATE));
			idxOpMeta.setCacheHitRate(index, dCacheHitRate);
		}		
		
		return idxOpMeta;
	}

	
	
}
