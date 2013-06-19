package com.hp.hplc.optimizer_old;

import java.util.ArrayList;
import java.util.List;

public class PlanNode {
	
	private List<PlanNode> children = new ArrayList<PlanNode>();
	private TaskPlan taskPlan = null;
	
	public PlanNode(){
		
	}
	
	public PlanNode(TaskPlan taskPlan){
		this.taskPlan = taskPlan;
	}
	
	public void addPlanNode(PlanNode node){
		children.add(node);
	}
	
	public void setTaskPlan(TaskPlan taskPlan){
		this.taskPlan = taskPlan;
	}
	
	public PlanNode getPlanNode(int index){
		return children.get(index);
	}
	
	public List<PlanNode> getChildren(){
		return children;
	}
	
	public TaskPlan getTaskPlan(){
		return taskPlan;
	}

	public void print() {
		 taskPlan.print();
		
	}
	
	public boolean hasChildren(){
		if(children.size()>0){
			return true;
		}else{
			return false;
		}
	}
	

}
