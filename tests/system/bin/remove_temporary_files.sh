#!/usr/bin/env bash

find . -name '*.output' -print -exec rm -f {} \;
find . -name 'dbdump' -print -exec rm -f {} \;
find . -name 'nohup.out' -print -exec rm -f {} \;
find . -name 'hypertable.cfg' -print -exec rm -f {} \;

/bin/rm -rf 80_10TB_load/10TB_load.cfg 6*/reports 70_balance/mml 70_balance/last.seed
/bin/rm -rf 90_replication/*.txt
/bin/rm -rf 91_bidirectional/*.txt
/bin/rm -rf 92_unidir_break/*.txt
/bin/rm -rf 93_bidir_break/*.txt
