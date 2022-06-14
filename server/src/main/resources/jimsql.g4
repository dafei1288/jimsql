grammar jimsql;

sqlscript:
    EOF
    | (simpleStatement) (SEMICOLON_SYMBOL EOF? | EOF)
    | (simpleStatement SEMICOLON_SYMBOL)+
;

simpleStatement: ddl
  | dml
  | dql
  | dcl
;

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
;

dcl:
    showProcesslist
  | showDatabases
  | showTables
  | showTableDesc
;


createDatabase:
  CREATE_SYMBOL DATABASE_SYMBOL  schemaName
;

schemaName:
  identifier
;

identifier:
  LETTERS
;

number:
  DIGIT
  | DIGITS
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


insertTable:
//insert into  user_info (user_account,user_name,user_age,user_class) values ('00001', '张三 ','20','计算机系'),
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
    (expr) (COMMA_SYMBOL (expr))*
;

expr:
  number
  | identifier

;



deleteTable:
  DELETE_SYMBOL FROM_SYMBOL tableName (WHERE_SYMBOL expressions)?
;

expressions:
  expression ( (OR_SYMBOL | AND_SYMBOL ) expression )*
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

//Address = 'Zhongshan 23', City = 'Nanjing'

selectTable:
  SELECT_SYMBOL columnList FROM_SYMBOL tableName (WHERE_SYMBOL expression)?
;

columnList:
    STAR_SYMBOL? (columnName (COMMA_SYMBOL columnName)* )?
;

columnName:
  identifier
;

expression:
  expr (EQ_SYMBOL | GT_SYMBOL | LT_SYMBOL | GTE_SYMBOL | LTE_SYMBOL | NE_SYMBOL ) expr
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




DOT_SYMBOL:         '.';
SEMICOLON_SYMBOL:   ';';
STAR_SYMBOL:        '*';
COMMA_SYMBOL:       ',';

EQ_SYMBOL:          '=';
GT_SYMBOL:          '>';
LT_SYMBOL:          '<';
GTE_SYMBOL:         '>=';
LTE_SYMBOL:         '<=';
NE_SYMBOL:          '!=';


START_PAR_SYMBOL:   '(';
CLOSE_PAR_SYMBOL:   ')';
// White space handling
WHITESPACE: [ \t\f\r\n] -> channel(HIDDEN); // Ignore whitespaces.



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

fragment DIGIT:    [0-9];
fragment DIGITS:   DIGIT+;
fragment HEXDIGIT: [0-9a-fA-F];





CREATE_SYMBOL:                   C R E A T E;
DROP_SYMBOL:                     D R O P;
DATABASE_SYMBOL:                 D A T A B A S E;
DATABASES_SYMBOL:                D A T A B A S E S;
USE_SYMBOL:                      U S E;
TABLES_SYMBOL:                   T A B L E S;
TABLE_SYMBOL:                    T A B L E;
SELECT_SYMBOL:                   S E L E C T;
FROM_SYMBOL:                     F R O M;
WHERE_SYMBOL:                    W H E R E;
INNER_SYMBOL:                    I N N E R ;
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

LETTER: [a-zA-Z0-9_$\u0080-\uffff];
LETTERS: LETTER+;

