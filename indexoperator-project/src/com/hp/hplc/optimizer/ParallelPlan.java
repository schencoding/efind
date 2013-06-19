package com.hp.hplc.optimizer;

import java.util.ArrayList;
import java.util.List;

public class ParallelPlan {
	
	private int planId = -1;
	private List<Integer> serialTasks = new ArrayList<Integer>();
	private List<Integer> parallelTasks = new ArrayList<Integer>();
	
	public void addSerialTask(int index){
		serialTasks.add(index);
	}
	public void addSerialTasks(List<Integer> list){
		serialTasks.addAll(list);
	}
	
	public List<Integer> getSerialTasks(){
		return this.serialTasks;
	}
	
	public void addParalleTask(int index){
		parallelTasks.add(index);
	}
	
	public void addParallelTasks(List<Integer> list){
		parallelTasks.addAll(list);
	}
	
	public List<Integer> getParalleTasks(){
		return parallelTasks;
	}
	public void setPlanId(int planId) {
		this.planId = planId;
	}
	
	public int getPlanId(){
		return planId;
	}

}
