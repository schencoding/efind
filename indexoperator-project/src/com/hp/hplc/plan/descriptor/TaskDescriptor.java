package com.hp.hplc.plan.descriptor;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.hp.hplc.plan.exception.InvalidPlanException;
import com.hp.hplc.plan.exception.InvalidTaskRequestException;
import com.hp.hplc.util.Pair;

/**
 * Descriptor of a single map or reduce task.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-11
 */
public abstract class TaskDescriptor implements Serializable {
	private static final long serialVersionUID = 3053652552138785427L;

	private TaskType type = null;
	private Class<? extends Writable> inputKeyClass = null;
	private Class<? extends Writable> inputValueClass = null;
	private Class<? extends Writable> outputKeyClass = null;
	private Class<? extends Writable> outputValueClass = null;
	
	private int id = 0;
	
	private List<Pair<String, String> > properties = null;

	public TaskDescriptor(TaskType type, int id) throws InvalidPlanException {
		if (type == null)
			throw new InvalidPlanException("TaskType cannot be null.");
		this.type = type;
		this.id = id;
		this.properties = new LinkedList<Pair<String, String> >();
	}
	
	public TaskDescriptor(TaskType type) throws InvalidPlanException {
		this(type, 0);
	}
	
	public TaskType getType() {
		return (type);
	}
	
	public void setID(int id) {
		this.id = id;
	}
	
	public int getID() {
		return (id);
	}
	
	public void set(String name, String value) {
		properties.add(new Pair<String, String>(name, value));
	}
	
	public List<Pair<String, String> > getProperties() {
		return (properties);
	}
	
	public void setInputKeyClass(Class<? extends Writable> theClass) {
		inputKeyClass = theClass;
	}
	
	public Class<? extends Writable> getInputKeyClass() {
		return (inputKeyClass);
	}
	
	public void setInputValueClass(Class<? extends Writable> theClass) {
		inputValueClass = theClass;
	}
	
	public Class<? extends Writable> getInputValueClass() {
		return (inputValueClass);
	}
	
	public void setOutputKeyClass(Class<? extends Writable> theClass) {
		outputKeyClass = theClass;
	}
	
	public Class<? extends Writable> getOutputKeyClass() {
		return (outputKeyClass);
	}
	
	public void setOutputValueClass(Class<? extends Writable> theClass) {
		outputValueClass = theClass;
	}
	
	public Class<? extends Writable> getOutputValueClass() {
		return (outputValueClass);
	}
	
	public String getDetails() {
		return (toString() + ", " + type + ", {" +
			inputKeyClass.getSimpleName() + ", " +
			inputValueClass.getSimpleName() + "} -> {" +
			outputKeyClass.getSimpleName() + ", " +
			outputValueClass.getSimpleName() + "}");
	}

	public abstract void exec(Writable key, Writable value,
		OutputCollector<Writable, Writable> output, Reporter reporter, boolean count)
		throws InvalidTaskRequestException, InvalidPlanException,
			InstantiationException, IllegalAccessException;
	
	public abstract void exec(Writable key, Iterator<Writable> values,
		OutputCollector<Writable, Writable> output, Reporter reporter, boolean count)
		throws InvalidTaskRequestException, InvalidPlanException,
			InstantiationException, IllegalAccessException;
	
	public void print(){
		
	}
}

