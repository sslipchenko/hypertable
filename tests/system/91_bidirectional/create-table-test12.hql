use "/";
drop table if exists Tabletest12;
create table Tabletest12 (
  Field1,
  Field2,
  Field3
) REPLICATE TO "test08";
