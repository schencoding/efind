package com.hp.hplc.translator;

public class PlanTask {
	

	private Object idxOp;
	private int action;

	public PlanTask(Object idxOp, int i) {
		this.idxOp = idxOp;
		this.action = i;
	}

	
	public Object getIdxOp() {
		return idxOp;
	}

	public void setIdxOp(Object idxOp) {
		this.idxOp = idxOp;
	}

	public int getAction() {
		return action;
	}

	public void setAction(int action) {
		this.action = action;
	}
	

}
