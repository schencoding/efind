package com.hp.hplc.plan.exception;

public class JobEmptyException extends Exception {
	private static final long serialVersionUID = -4315868427893014282L;

	public JobEmptyException() {
	}
	
	public JobEmptyException(String msg) {
		super(msg);
	}
}
