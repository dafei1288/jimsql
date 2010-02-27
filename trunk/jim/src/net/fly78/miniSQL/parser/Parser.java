package net.fly78.miniSQL.parser;

import java.io.Reader;

import net.fly78.miniSQL.statement.Statement;

public interface Parser {
	public Statement parse(Reader statementReader) throws ParserException, InstantiationException, IllegalAccessException, ClassNotFoundException;
}
