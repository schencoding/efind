package com.hp.hplc.plan.exception;

public class TaskNotFoundException extends Exception {
	private static final long serialVersionUID = -5864699475129283643L;

	public TaskNotFoundException() {
	}
	
	public TaskNotFoundException(String msg) {
		super(msg);
	}
}
