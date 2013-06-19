package com.hp.hplc.optimizer_old;

import com.hp.hplc.indexoperator.IndexOperator;

public class IndexTaskPlan extends TaskPlan {
	
	private IndexOperator idxOp = null;
	
	public IndexTaskPlan(IndexOperator idxOp, int planId){
		super();
		this.idxOp = idxOp;
		this.setPlanId(planId);
	}
	
	public void print(){
		System.out.print("I" + this.getPlanId() + " ");
	}

	public IndexOperator getIndexOperator(){
		return idxOp;
	}
}
