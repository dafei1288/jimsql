grammar jimsql;

// ---------------------------
// Entry Points
// ---------------------------
sqlscript:
    EOF
  | (simpleStatement) (SEMICOLON_SYMBOL EOF? | EOF)
  | (simpleStatement SEMICOLON_SYMBOL)+
;

simpleStatement: ddl | dml | dql | dcl;

// ---------------------------
// Statement Groups
// ---------------------------
ddl:
    createDatabase
  | dropDatabase
  | useDatabase
  | createTable
  | dropTable
;

dml:
    insertTable
  | deleteTable
  | updateTable
;

dql:
    selectTable
  | explainSelectTable
;

dcl:
    showProcesslist
  | showDatabases
  | showTables
  | showTableDesc
;

// ---------------------------
// DDL
// ---------------------------
createDatabase:
  CREATE_SYMBOL DATABASE_SYMBOL  schemaName
;

schemaName:
  identifier
;

// identifier supports bare, backtick-quoted, and double-quoted names
identifier:
    LETTERS
  | BACKTICK_QUOTED_ID
  | DOUBLE_QUOTED_ID
;

dropDatabase:
  DROP_SYMBOL DATABASE_SYMBOL schemaName
;

useDatabase:
  USE_SYMBOL schemaName
;

createTable:
  CREATE_SYMBOL TABLE_SYMBOL  tableName
;

dropTable:
  DROP_SYMBOL TABLE_SYMBOL tableName
;

tableName:
  identifier
;

// ---------------------------
// DML
// ---------------------------
insertTable:
  // insert into user_info (a,b) values ('00001','20'),('00002','30')
  INSERT_SYMBOL INTO_SYMBOL tableName ( START_PAR_SYMBOL fields CLOSE_PAR_SYMBOL )?  insertValues
;

fields:
    insertIdentifier (COMMA_SYMBOL insertIdentifier)*
;

insertIdentifier:
  identifier
;

insertValues:
    (VALUES_SYMBOL | VALUE_SYMBOL) valueList
;

valueList:
    START_PAR_SYMBOL values? CLOSE_PAR_SYMBOL (
        COMMA_SYMBOL START_PAR_SYMBOL values? CLOSE_PAR_SYMBOL
    )*
;

values:
    expr (COMMA_SYMBOL expr)*
;

// ---------------------------
// Expressions (with precedence)
// ---------------------------
expr:
    numberLiteral
  | stringLiteral
  | booleanLiteral
  | nullLiteral
  | identifier
;

expression:
    orExpr
;

orExpr:
    andExpr (OR_SYMBOL andExpr)*
;

andExpr:
    notExpr (AND_SYMBOL notExpr)*
;

notExpr:
    (NOT_SYMBOL)? predicate
;

predicate:
    valueExpr (
        (EQ_SYMBOL | GT_SYMBOL | LT_SYMBOL | GTE_SYMBOL | LTE_SYMBOL | NE_SYMBOL) valueExpr
      | IS_SYMBOL (NOT_SYMBOL)? NULL_SYMBOL
      | LIKE_SYMBOL valueExpr
      | BETWEEN_SYMBOL valueExpr AND_SYMBOL valueExpr
      | IN_SYMBOL START_PAR_SYMBOL (expr (COMMA_SYMBOL expr)*)? CLOSE_PAR_SYMBOL
    )?
;

// Arithmetic expression precedence
valueExpr:
  addExpr
;

addExpr:
  mulExpr ((PLUS_SYMBOL | MINUS_SYMBOL) mulExpr)*
;

mulExpr:
  unaryExpr ((STAR_SYMBOL | DIV_SYMBOL | MOD_SYMBOL) unaryExpr)*
;

unaryExpr:
  (PLUS_SYMBOL | MINUS_SYMBOL) unaryExpr
  | primary
;

primary:
    START_PAR_SYMBOL expression CLOSE_PAR_SYMBOL
  | expr
;

numberLiteral:
    INT_LITERAL
  | DECIMAL_LITERAL
;

stringLiteral:
    STRING_LITERAL
;

booleanLiteral:
    TRUE_SYMBOL
  | FALSE_SYMBOL
;

nullLiteral:
    NULL_SYMBOL
;

// ---------------------------
// DML (delete/update)
// ---------------------------
deleteTable:
  DELETE_SYMBOL FROM_SYMBOL tableName (WHERE_SYMBOL expression)?
;

// Keep rule name for compatibility; route to single expression now
expressions:
  expression
;

updateTable:
  UPDATE_SYMBOL tableName SET_SYMBOL updateList  (WHERE_SYMBOL expression)?
;

updateList:
  updateItem (COMMA_SYMBOL updateItem )*
;

updateItem:
  columnName EQ_SYMBOL expr
;

// ---------------------------
// DQL (SELECT/UNION)
// ---------------------------
selectTable:
  selectBody (UNION_SYMBOL (ALL_SYMBOL)? selectBody)*
;

selectBody:
  SELECT_SYMBOL (DISTINCT_SYMBOL)? columnList FROM_SYMBOL (tableName | tableSource)
  (WHERE_SYMBOL expression)?
  (GROUP_SYMBOL BY_SYMBOL groupByList)?
  (HAVING_SYMBOL expression)?
  (ORDER_SYMBOL BY_SYMBOL orderItem (COMMA_SYMBOL orderItem)*)?
  (LIMIT_SYMBOL INT_LITERAL (OFFSET_SYMBOL INT_LITERAL)?)?
;

orderItem:
  columnName (ASC_SYMBOL | DESC_SYMBOL)?
;

groupByList:
  columnName (COMMA_SYMBOL columnName)*
;

columnList:
    STAR_SYMBOL? (columnName (COMMA_SYMBOL columnName)* )?
;

columnName:
  identifier
;

// FROM sources and JOINs

tableSource:
  tablePrimary (tableJoin)*
;

tablePrimary:
  tableName (AS_SYMBOL? alias)?
;

alias:
  identifier
;

tableJoin:
  ( (INNER_SYMBOL)? JOIN_SYMBOL tablePrimary ON_SYMBOL expression )
| ( (LEFT_SYMBOL | RIGHT_SYMBOL | FULL_SYMBOL) (OUTER_SYMBOL)? JOIN_SYMBOL tablePrimary ON_SYMBOL expression )
| ( CROSS_SYMBOL JOIN_SYMBOL tablePrimary )
;

explainSelectTable:
  EXPLAIN_SYMBOL selectTable
;

// ---------------------------
// DCL
// ---------------------------
showProcesslist:
  SHOW_SYMBOL PROCESSLIST_SYMBOL
;
showDatabases:
  SHOW_SYMBOL DATABASES_SYMBOL
;
showTables:
  SHOW_SYMBOL TABLES_SYMBOL
;
showTableDesc:
  SHOW_SYMBOL TABLE_SYMBOL DESCRIPT_SYMBOL tableName
  | DESCRIPT_SYMBOL TABLE_SYMBOL tableName
;

// ---------------------------
// Lexer Tokens
// ---------------------------
DOT_SYMBOL:         '.';
SEMICOLON_SYMBOL:   ';';
STAR_SYMBOL:        '*';
COMMA_SYMBOL:       ',';
PLUS_SYMBOL:        '+';
MINUS_SYMBOL:       '-';
DIV_SYMBOL:         '/';
MOD_SYMBOL:         '%';

EQ_SYMBOL:          '=';
GT_SYMBOL:          '>';
LT_SYMBOL:          '<';
GTE_SYMBOL:         '>=';
LTE_SYMBOL:         '<=';
NE_SYMBOL:          '!=';

START_PAR_SYMBOL:   '(';
CLOSE_PAR_SYMBOL:   ')';

// Whitespace & Comments
WHITESPACE: [ \t\f\r\n] -> channel(HIDDEN);
LINE_COMMENT: '--' ~[\r\n]* -> channel(HIDDEN);
HASH_COMMENT: '#' ~[\r\n]* -> channel(HIDDEN);
BLOCK_COMMENT: '/*' .*? '*/' -> channel(HIDDEN);

// Literals
INT_LITERAL: [0-9]+;
DECIMAL_LITERAL: [0-9]+ '.' [0-9]* | '.' [0-9]+;
STRING_LITERAL: '\'' ( '\\' . | ~('\\'|'\''|'\r'|'\n') )* '\'';

// Keyword fragments (A..Z)
fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];

// Keywords (must appear before LETTER/LETTERS)
CREATE_SYMBOL:                   C R E A T E;
DROP_SYMBOL:                     D R O P;
DATABASE_SYMBOL:                 D A T A B A S E;
DATABASES_SYMBOL:                D A T A B A S E S;
USE_SYMBOL:                      U S E;
TABLES_SYMBOL:                   T A B L E S;
TABLE_SYMBOL:                    T A B L E;
SELECT_SYMBOL:                   S E L E C T;
DISTINCT_SYMBOL:                 D I S T I N C T;
FROM_SYMBOL:                     F R O M;
WHERE_SYMBOL:                    W H E R E;
GROUP_SYMBOL:                    G R O U P;
HAVING_SYMBOL:                   H A V I N G;
ORDER_SYMBOL:                    O R D E R;
BY_SYMBOL:                       B Y;
ASC_SYMBOL:                      A S C;
DESC_SYMBOL:                     D E S C;
LIMIT_SYMBOL:                    L I M I T;
OFFSET_SYMBOL:                   O F F S E T;
NOT_SYMBOL:                      N O T;
IS_SYMBOL:                       I S;
LIKE_SYMBOL:                     L I K E;
IN_SYMBOL:                       I N;
BETWEEN_SYMBOL:                  B E T W E E N;
TRUE_SYMBOL:                     T R U E;
FALSE_SYMBOL:                    F A L S E;
NULL_SYMBOL:                     N U L L;
INSERT_SYMBOL:                   I N S E R T;
INTO_SYMBOL:                     I N T O;
VALUE_SYMBOL:                    V A L U E;
VALUES_SYMBOL:                   V A L U E S;
UPDATE_SYMBOL:                   U P D A T E;
SET_SYMBOL:                      S E T;
DELETE_SYMBOL:                   D E L E T E;
OR_SYMBOL:                       O R;
AND_SYMBOL:                      A N D;
SHOW_SYMBOL:                     S H O W;
PROCESSLIST_SYMBOL:              P R O C E S S L I S T;
DESCRIPT_SYMBOL:                 D E S C R I P T;
EXPLAIN_SYMBOL:                  E X P L A I N;
UNION_SYMBOL:                    U N I O N;
ALL_SYMBOL:                      A L L;
JOIN_SYMBOL:                     J O I N;
INNER_SYMBOL:                    I N N E R;
LEFT_SYMBOL:                     L E F T;
RIGHT_SYMBOL:                    R I G H T;
FULL_SYMBOL:                     F U L L;
OUTER_SYMBOL:                    O U T E R;
CROSS_SYMBOL:                    C R O S S;
ON_SYMBOL:                       O N;
AS_SYMBOL:                       A S;

// Identifiers (keep legacy behavior)
LETTER: [a-zA-Z0-9_$\u0080-\uffff];
LETTERS: LETTER+;

// Quoted identifiers
BACKTICK_QUOTED_ID: '`' ( '``' | ~'`' )* '`';
DOUBLE_QUOTED_ID: '"' ( '""' | ~('"'|'\r'|'\n') )* '"';

