use "/";
drop table if exists Tabletest08;
create table Tabletest08 (
  Field1,
  Field2,
  Field3
) REPLICATE TO "test12";
