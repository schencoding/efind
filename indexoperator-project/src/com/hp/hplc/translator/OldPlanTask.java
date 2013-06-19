package com.hp.hplc.translator;

public class OldPlanTask {
	private boolean bNewLeftJob;
	private boolean bNewMiddleJob;
	private boolean bNewRightJob;
	private String leftAction;
	private String middleAction;
	private String rightAction;

	public OldPlanTask(boolean bNewLeftJob,boolean bNewMiddleJob, boolean bNewRightJob,
			String leftAction, String middleAction, String rightAction) {
		super();
		this.bNewLeftJob = bNewLeftJob;
		this.bNewMiddleJob = bNewMiddleJob;
		this.bNewRightJob = bNewRightJob;
		this.leftAction = leftAction;
		this.middleAction = middleAction;
		this.rightAction = rightAction;
	}

	public boolean isbNewLeftJob() {
		return bNewLeftJob;
	}

	public void setbNewLeftJob(boolean bNewLeftJob) {
		this.bNewLeftJob = bNewLeftJob;
	}

	public boolean isbNewRightJob() {
		return bNewRightJob;
	}

	public void setbNewRightJob(boolean bNewRightJob) {
		this.bNewRightJob = bNewRightJob;
	}

	public String getLeftAction() {
		return leftAction;
	}

	public void setLeftAction(String leftAction) {
		this.leftAction = leftAction;
	}

	public String getRightAction() {
		return rightAction;
	}

	public void setRightAction(String rightAction) {
		this.rightAction = rightAction;
	}

	public boolean isbNewMiddleJob() {
		return bNewMiddleJob;
	}

	public void setbNewMiddleJob(boolean bNewMiddleJob) {
		this.bNewMiddleJob = bNewMiddleJob;
	}

	public String getMiddleAction() {
		return middleAction;
	}

	public void setMiddleAction(String middleAction) {
		this.middleAction = middleAction;
	}
	
	

}
