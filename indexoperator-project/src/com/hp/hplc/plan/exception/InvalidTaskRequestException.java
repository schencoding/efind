package com.hp.hplc.plan.exception;

public class InvalidTaskRequestException extends Exception {
	private static final long serialVersionUID = -7527175770299592859L;

	public InvalidTaskRequestException() {
	}
	
	public InvalidTaskRequestException(String msg) {
		super(msg);
	}
}
