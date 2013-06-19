package com.hp.hplc.plan.exception;

public class InvalidPlanException extends Exception {
	private static final long serialVersionUID = 2524883168605657701L;

	public InvalidPlanException() {
	}
	
	public InvalidPlanException(String msg) {
		super(msg);
	}
}
