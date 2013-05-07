CREATE NAMESPACE "/rep";
USE "/";

CREATE TABLE NotReplicated (
  Field, 
  ACCESS GROUP default BLOCKSIZE=1000 (Field)
) COMPRESSOR="none";

CREATE TABLE LoadTest (
  Field, 
  ACCESS GROUP default BLOCKSIZE=1000 (Field)
) COMPRESSOR="none" REPLICATE TO "cluster";
