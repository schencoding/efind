package com.hp.hplc.indexoperator;

public class Triple <K1> {
	private K1 key;	
	private SimpleIndexOperator indexOperator;
	private Object value;
	
	public Triple(K1 key, SimpleIndexOperator idxOp, Object value){
		this.key = key;
		this.indexOperator = idxOp;
		this.value = value;
	}
	
	
	public K1 getKey() {
		return key;
	}
	public void setKey(K1 key) {
		this.key = key;
	}
	public SimpleIndexOperator getIndexOperator() {
		return indexOperator;
	}
	public void setIndexOperator(SimpleIndexOperator indexOperator) {
		this.indexOperator = indexOperator;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
}
