package net.fly78.miniSQL.parser;

import java.lang.RuntimeException;;

public class ParserException extends RuntimeException {

	public ParserException() {
		super();
	}

	public ParserException(String message, Throwable cause) {
		super(message, cause);
	}

	public ParserException(String message) {
		super(message);
	}

	public ParserException(Throwable cause) {
		super(cause);
	}
	
}
