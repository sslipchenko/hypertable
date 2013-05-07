CREATE NAMESPACE "/rep";
CREATE NAMESPACE "/source";
USE "/source";
CREATE TABLE Repl (c) REPLICATE TO "cluster";
ALTER TABLE Repl ADD (d, e, f);
