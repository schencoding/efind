package com.hp.hplc.optimizer_old;

import org.apache.hadoop.mapred.Reducer;

public class UserReducerTaskPlan extends TaskPlan {
	
	private Reducer reducer = null;
	
	public UserReducerTaskPlan(Reducer reducer, int planId){
		super();
		this.reducer = reducer;
		this.setPlanId(planId);
	}
	
	public void print(){
		System.out.print("R" + this.getPlanId() + " ");
	}

}
