CREATE NAMESPACE "/rep";
CREATE NAMESPACE "/source";
USE "/source";
CREATE TABLE NoRepl (c);
CREATE TABLE Repl (c) REPLICATE TO "cluster";
