#!/usr/bin/env bash

cd `dirname $0`

HYPERTABLE_HOME=/opt/hypertable/$USER/current
REPORT_FILE=`pwd`/archive/report_`date +"%Y%m%d%H%M%S"`.txt
REPORT_RECIPIENTS="doug@hypertable.com chris@hypertable.com"

let ntests=0
let index=1
let fail_hard=0

run_test() {
  if [ $ntests -gt 10 ] ; then
    echo -n " "
  fi
  echo -e "\033[1m$index/$ntests START $1\033[0m"

  START=`date +"%s"`
  env HYPERTABLE_HOME=$HYPERTABLE_HOME $1/run.sh
  RVAL=$?
  END=`date +"%s"`

  DURATION=`bin/duration.sh $START $END`

  echo ""
  if [ $ntests -gt 10 ] ; then
    echo -n " "
    echo -n " " >> $REPORT_FILE
  fi
  echo -en "\033[1m$index/$ntests RESULT $1 "
  echo -n "$index/$ntests RESULT `basename $1` " >> $REPORT_FILE

  # print dots ....
  let ndots=30-${#dir}
  for ((i=0; i<$ndots; i++)); do
    echo -n "."
    echo -n "." >> $REPORT_FILE
  done

  if [ $RVAL == 0 ] ; then
    echo -e " [$DURATION] "'\E[37;32m'"Passed\033[0m";
    echo " [$DURATION] Passed" >> $REPORT_FILE
  elif [ $RVAL == 2 ] ; then
    echo -e " [$DURATION] "'\E[37;32m'"Failed hard\033[0m";
    echo " [$DURATION] Failed hard" >> $REPORT_FILE
    let fail_hard=1
  else
    echo -e " [$DURATION] "'\E[37;31m'"Failed\033[0m"
    echo " [$DURATION] Failed" >> $REPORT_FILE
  fi
  tput sgr0

  echo | tee -a $REPORT_FILE
  if [ -e $1/summary.output ]; then
    cat $1/summary.output >> $REPORT_FILE
  fi

  let index=index+1
}

mkdir -p archive
/bin/rm -f $REPORT_FILE
VERSION=`/opt/hypertable/$USER/current/bin/ht version | head -1`

echo "Version: $VERSION" > $REPORT_FILE
echo >> $REPORT_FILE

blacklist=("bin" "archive" "80_10TB_load")

if [ ${#} -eq 0 ]; then

  # Set ntests to the number of test subdirs (ignore bin)
  for dir in `/bin/ls -1F | fgrep "/" | sed 's/\///g'` ; do
    go=true
    for ((i=0; i<${#blacklist}; i++)); do
      if [ $dir == ${blacklist[$i]} ] ; then
        go=false
        break
      fi
    done
    if $go ; then
      let ntests=ntests+1
    fi
  done

  for dir in `/bin/ls -1F | fgrep "/" | sed 's/\///g'` ; do
    skip=false
    for ((i=0; i<${#blacklist}; i++)); do
      if [ $dir == ${blacklist[$i]} ] ; then
        skip=true
        break
      fi
    done
    if $skip ; then
      continue;
    fi
    run_test `pwd`/$dir
    if [ $fail_hard == 1 ] ; then
      break
    fi
  done

else

  let ntests=${#}
  for dir in "$@" ; do
    run_test `pwd`/$dir
    if [ $fail_hard == 1 ] ; then
      break
    fi
  done

fi

SUBJECT="Hypertable System Test Report - $VERSION"

bin/send_report.sh "\"$SUBJECT\"" $REPORT_FILE $REPORT_RECIPIENTS
