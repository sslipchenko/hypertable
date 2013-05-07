CREATE NAMESPACE "/rep";
USE "/";

CREATE TABLE LoadTest1 (
  Field, 
  ACCESS GROUP default BLOCKSIZE=1000 (Field)
) COMPRESSOR="none" REPLICATE TO "cluster";

CREATE TABLE LoadTest2 (
  Field, 
  ACCESS GROUP default BLOCKSIZE=1000 (Field)
) COMPRESSOR="none" REPLICATE TO "cluster";
