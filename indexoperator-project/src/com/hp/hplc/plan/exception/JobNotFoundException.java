package com.hp.hplc.plan.exception;

public class JobNotFoundException extends Exception {
	private static final long serialVersionUID = -1851367986890579745L;

	public JobNotFoundException() {
	}
	
	public JobNotFoundException(String msg) {
		super(msg);
	}
}
