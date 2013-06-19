package com.hp.hplc.optimizer_old;

import org.apache.hadoop.mapred.Mapper;

public class UserMapTaskPlan extends TaskPlan{
	
	private Mapper mapper = null;
	
	
	public UserMapTaskPlan(Mapper mapper){
		super();
		this.mapper = mapper;
	}
	
	public void print(){
		System.out.print("M  ");
	}

}
