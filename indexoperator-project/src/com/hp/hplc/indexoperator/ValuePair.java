package com.hp.hplc.indexoperator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ValuePair<T1,T2> implements WritableComparable<ValuePair>{
	private T1 one;
	private T2 two;
	
	public void set(T1 one, T2 two){
		this.one = one;
		this.two = two;
	}
	
	public ValuePair(T1 one, T2 two){
		set(one, two);
	}
	
	public T1 getFirst(){
		return one;
	}
	
	public T2 getSecond(){
		return two;
	}
	
	

	@Override
	public void write(DataOutput out) throws IOException {
		((Writable)one).write(out);
		((Writable)two).write(out);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {		
		((Writable)one).readFields(in);
		((Writable)two).readFields(in);	
	}

	@Override
	public int compareTo(ValuePair tp) {
		int cmp = ((Comparable)one).compareTo((Comparable)tp.one);
		if(cmp!=0){
			return cmp;
		}
		return ((Comparable)two).compareTo((Comparable)tp.two);
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof ValuePair){
			ValuePair tp = (ValuePair)obj;
			return one.equals(tp.one)&& two.equals(tp.two);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return one.hashCode()+two.hashCode();
	}

	@Override
	public String toString() {
		return "(" + one + ", " + two + ")";
	}
}
