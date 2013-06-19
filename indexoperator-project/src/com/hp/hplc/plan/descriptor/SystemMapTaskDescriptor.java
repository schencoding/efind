package com.hp.hplc.plan.descriptor;

import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Mapper;

import com.hp.hplc.plan.exception.InvalidPlanException;

/**
 * Descriptor of a system map task.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-11
 */
public class SystemMapTaskDescriptor extends UserMapTaskDescriptor implements Serializable {
	private static final long serialVersionUID = -3562173684084783593L;

	public SystemMapTaskDescriptor(
		Mapper<Writable, Writable, Writable, Writable> task, TaskType type, int id)
		throws InvalidPlanException {
		super(task, type, id);
	}
	
	public SystemMapTaskDescriptor(
		Mapper<Writable, Writable, Writable, Writable> task, TaskType type)
		throws InvalidPlanException {
		super(task, type);
	}
	
	public String toString() {
		return ("SystemMapTask");
	}
}
