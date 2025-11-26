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
  | alterTable
;

// ---------------------------
// DDL
// ---------------------------
createDatabase:
  CREATE_SYMBOL DATABASE_SYMBOL (IF_SYMBOL NOT_SYMBOL EXISTS_SYMBOL)? schemaName
;

dropDatabase:
  DROP_SYMBOL DATABASE_SYMBOL (IF_SYMBOL EXISTS_SYMBOL)? schemaName
;

useDatabase:
  USE_SYMBOL schemaName
;



dropTable:
  DROP_SYMBOL TABLE_SYMBOL tableName
;

createTable:
  CREATE_SYMBOL TABLE_SYMBOL (IF_SYMBOL NOT_SYMBOL EXISTS_SYMBOL)? tableName
  START_PAR_SYMBOL tableElementList CLOSE_PAR_SYMBOL
;

tableElementList:
  tableElement (COMMA_SYMBOL tableElement)*
;

tableElement:
  columnDef
  | tableConstraint
;

columnDef:
  columnName dataType columnDefOption*
;

columnDefOption:
    NULL_SYMBOL
  | NOT_SYMBOL NULL_SYMBOL
  | DEFAULT_SYMBOL defaultValue
  | AUTO_INCREMENT_SYMBOL
  | PRIMARY_SYMBOL KEY_SYMBOL
  | UNIQUE_SYMBOL
  | COMMENT_SYMBOL stringLiteral
;

defaultValue:
    stringLiteral
  | numberLiteral
  | NULL_SYMBOL
  | TRUE_SYMBOL
  | FALSE_SYMBOL
;

numberLiteral:
    INT_LITERAL
  | DECIMAL_LITERAL
;

stringLiteral:
    STRING_LITERAL
;

tableConstraint:
    PRIMARY_SYMBOL KEY_SYMBOL START_PAR_SYMBOL colList CLOSE_PAR_SYMBOL
  | UNIQUE_SYMBOL START_PAR_SYMBOL colList CLOSE_PAR_SYMBOL
  | (CONSTRAINT_SYMBOL identifier)? FOREIGN_SYMBOL KEY_SYMBOL START_PAR_SYMBOL colList CLOSE_PAR_SYMBOL
      REFERENCES_SYMBOL tableName START_PAR_SYMBOL colList CLOSE_PAR_SYMBOL
;

colList:
  columnName (COMMA_SYMBOL columnName)*
;

alterTable:
  ALTER_SYMBOL TABLE_SYMBOL tableName alterSpecification
;

alterSpecification:
    ADD_SYMBOL (COLUMN_SYMBOL)? columnDef positionClause?
  | DROP_SYMBOL (COLUMN_SYMBOL)? columnName
  | MODIFY_SYMBOL (COLUMN_SYMBOL)? columnDef
  | CHANGE_SYMBOL (COLUMN_SYMBOL)? identifier identifier columnDef
  | RENAME_SYMBOL (TO_SYMBOL)? tableName
  | ADD_SYMBOL (UNIQUE_SYMBOL)? (INDEX_SYMBOL | KEY_SYMBOL) identifier START_PAR_SYMBOL colList CLOSE_PAR_SYMBOL
  | DROP_SYMBOL (INDEX_SYMBOL | KEY_SYMBOL) identifier
  | ADD_SYMBOL (CONSTRAINT_SYMBOL identifier)? FOREIGN_SYMBOL KEY_SYMBOL START_PAR_SYMBOL colList CLOSE_PAR_SYMBOL REFERENCES_SYMBOL tableName START_PAR_SYMBOL colList CLOSE_PAR_SYMBOL
  | DROP_SYMBOL FOREIGN_SYMBOL KEY_SYMBOL identifier
  | ADD_SYMBOL PRIMARY_SYMBOL KEY_SYMBOL START_PAR_SYMBOL colList CLOSE_PAR_SYMBOL
  | DROP_SYMBOL PRIMARY_SYMBOL KEY_SYMBOL
;

positionClause:
    FIRST_SYMBOL
  | AFTER_SYMBOL identifier
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

tableName:
  identifier
;

columnName:
  identifier (DOT_SYMBOL identifier)*
;

// ---------------------------
// DML
// ---------------------------

dml:
  insertTable
  | deleteTable
  | updateTable
;
insertTable:
  INSERT_SYMBOL INTO_SYMBOL tableName
    (
      (START_PAR_SYMBOL fields CLOSE_PAR_SYMBOL)? insertValues
    | (START_PAR_SYMBOL fields CLOSE_PAR_SYMBOL)? insertSelect
    | SET_SYMBOL setAssignments
    )
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

// INSERT ... SELECT support
insertSelect:
  selectBody (UNION_SYMBOL (ALL_SYMBOL)? selectBody)*
;

// INSERT ... SET support
setAssignments:
  setAssignment (COMMA_SYMBOL setAssignment)*
;

setAssignment:
  insertIdentifier EQ_SYMBOL expr
;

// ---------------------------
// Expressions (with precedence)
// ---------------------------
expr:
    numberLiteral
  | stringLiteral
  | booleanLiteral
  | nullLiteral
  | qualifiedName
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
  | functionCall
  | expr
;


booleanLiteral:
    TRUE_SYMBOL
  | FALSE_SYMBOL
;

nullLiteral:
    NULL_SYMBOL
;

// ---------------------------
// Data Types
// ---------------------------

dataType:
    integerType
  | floatDoubleType
  | decimalType
  | charVarType
  | binaryVarType
  | textBlobType
  | dateTimeType
  | jsonType
  | boolType
  | enumType
  | setType
;

integerType:
  (TINYINT_SYMBOL | SMALLINT_SYMBOL | MEDIUMINT_SYMBOL | INT_SYMBOL | BIGINT_SYMBOL) (UNSIGNED_SYMBOL)?
;

floatDoubleType:
  FLOAT_SYMBOL | DOUBLE_SYMBOL
;

decimalType:
  DECIMAL_T_SYMBOL START_PAR_SYMBOL INT_LITERAL (COMMA_SYMBOL INT_LITERAL)? CLOSE_PAR_SYMBOL
;

charVarType:
  (CHAR_SYMBOL | VARCHAR_SYMBOL) START_PAR_SYMBOL INT_LITERAL CLOSE_PAR_SYMBOL
;

binaryVarType:
  (BINARY_SYMBOL | VARBINARY_SYMBOL) START_PAR_SYMBOL INT_LITERAL CLOSE_PAR_SYMBOL
;

textBlobType:
  TINYTEXT_SYMBOL | TEXT_SYMBOL | MEDIUMTEXT_SYMBOL | LONGTEXT_SYMBOL | TINYBLOB_SYMBOL | BLOB_SYMBOL | MEDIUMBLOB_SYMBOL | LONGBLOB_SYMBOL
;

dateTimeType:
  DATE_SYMBOL | TIME_SYMBOL | DATETIME_SYMBOL | TIMESTAMP_SYMBOL | YEAR_SYMBOL
;

jsonType:
  JSON_SYMBOL
;

boolType:
  BOOL_SYMBOL | BOOLEAN_SYMBOL
;

enumType:
  ENUM_SYMBOL START_PAR_SYMBOL stringList CLOSE_PAR_SYMBOL
;

setType:
  SET_SYMBOL START_PAR_SYMBOL stringList CLOSE_PAR_SYMBOL
;

stringList:
  STRING_LITERAL (COMMA_SYMBOL STRING_LITERAL)*
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

dql:
  selectTable
  | explainSelectTable
;
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
    STAR_SYMBOL
  | selectItems
;

selectItems:
  selectItem (COMMA_SYMBOL selectItem)*
;

selectItem:
    columnName
  | valueExpr (AS_SYMBOL? alias)?
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

dcl:
  showProcesslist
  | showDatabases
  | showTables
  | showTableDesc
  | showCreateTable
  | describeTable
;
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

showCreateTable:
  SHOW_SYMBOL CREATE_SYMBOL TABLE_SYMBOL tableName
;

describeTable:
  DESCRIBE_SYMBOL tableName
  | DESC_SYMBOL TABLE_SYMBOL? tableName
  | SHOW_SYMBOL COLUMNS_SYMBOL FROM_SYMBOL tableName
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
ALTER_SYMBOL:                    A L T E R;
ADD_SYMBOL:                      A D D;
MODIFY_SYMBOL:                   M O D I F Y;
CHANGE_SYMBOL:                   C H A N G E;
RENAME_SYMBOL:                   R E N A M E;
TO_SYMBOL:                       T O;
IF_SYMBOL:                       I F;
EXISTS_SYMBOL:                   E X I S T S;
DATABASE_SYMBOL:                 D A T A B A S E;
DATABASES_SYMBOL:                D A T A B A S E S;
USE_SYMBOL:                      U S E;
TABLES_SYMBOL:                   T A B L E S;
TABLE_SYMBOL:                    T A B L E;
COLUMN_SYMBOL:                   C O L U M N;
FIRST_SYMBOL:                    F I R S T;
AFTER_SYMBOL:                    A F T E R;
INDEX_SYMBOL:                    I N D E X;
KEY_SYMBOL:                      K E Y;
PRIMARY_SYMBOL:                  P R I M A R Y;
UNIQUE_SYMBOL:                   U N I Q U E;
FOREIGN_SYMBOL:                  F O R E I G N;
REFERENCES_SYMBOL:               R E F E R E N C E S;
CONSTRAINT_SYMBOL:               C O N S T R A I N T;
AUTO_INCREMENT_SYMBOL:           A U T O '_' I N C R E M E N T;
DEFAULT_SYMBOL:                  D E F A U L T;
COMMENT_SYMBOL:                  C O M M E N T;
UNSIGNED_SYMBOL:                 U N S I G N E D;
CHAR_SYMBOL:                     C H A R;
VARCHAR_SYMBOL:                  V A R C H A R;
BINARY_SYMBOL:                   B I N A R Y;
VARBINARY_SYMBOL:                V A R B I N A R Y;
TINYTEXT_SYMBOL:                 T I N Y T E X T;
TEXT_SYMBOL:                     T E X T;
MEDIUMTEXT_SYMBOL:               M E D I U M T E X T;
LONGTEXT_SYMBOL:                 L O N G T E X T;
TINYBLOB_SYMBOL:                 T I N Y B L O B;
BLOB_SYMBOL:                     B L O B;
MEDIUMBLOB_SYMBOL:               M E D I U M B L O B;
LONGBLOB_SYMBOL:                 L O N G B L O B;
DATE_SYMBOL:                     D A T E;
TIME_SYMBOL:                     T I M E;
DATETIME_SYMBOL:                 D A T E T I M E;
TIMESTAMP_SYMBOL:                T I M E S T A M P;
YEAR_SYMBOL:                     Y E A R;
JSON_SYMBOL:                     J S O N;
BOOL_SYMBOL:                     B O O L;
BOOLEAN_SYMBOL:                  B O O L E A N;
ENUM_SYMBOL:                     E N U M;
FLOAT_SYMBOL:                    F L O A T;
DOUBLE_SYMBOL:                   D O U B L E;
DECIMAL_T_SYMBOL:                D E C I M A L;
TINYINT_SYMBOL:                  T I N Y I N T;
SMALLINT_SYMBOL:                 S M A L L I N T;
MEDIUMINT_SYMBOL:                M E D I U M I N T;
INT_SYMBOL:                      I N T;
BIGINT_SYMBOL:                   B I G I N T;
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
DESCRIBE_SYMBOL:                 D E S C R I B E;
COLUMNS_SYMBOL:                  C O L U M N S;

// Identifiers (keep legacy behavior)
LETTER: [a-zA-Z0-9_$\u0080-\uffff];
LETTERS: LETTER+;

// Quoted identifiers
BACKTICK_QUOTED_ID: '`' ( '``' | ~'`' )* '`';
DOUBLE_QUOTED_ID: '"' ( '""' | ~('"'|'\r'|'\n') )* '"';




qualifiedName:
  identifier (DOT_SYMBOL identifier)*
;

functionCall:
  identifier START_PAR_SYMBOL (expression (COMMA_SYMBOL expression)*)? CLOSE_PAR_SYMBOL
;

