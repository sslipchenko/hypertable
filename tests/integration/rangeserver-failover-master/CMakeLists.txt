#
# The following 23 tests have the same flow:
#
# 2 RangeServers; 1 crashes, and the Master crashes during recovery via
# the FailureInducer
#
add_test(RangeServer-failover-master-1 env TEST=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-2 env TEST=2 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-3 env TEST=3 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-4 env TEST=4 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-5 env TEST=5 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-6 env TEST=6 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-7 env TEST=7 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-8 env TEST=8 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-9 env TEST=9 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-10 env TEST=10 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-11 env TEST=11 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-12 env TEST=12 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-13 env TEST=13 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-14 env TEST=14 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-15 env TEST=15 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-16 env TEST=16 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-17 env TEST=17 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-18 env TEST=18 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-19 env TEST=19 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-20 env TEST=20 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-21 env TEST=21 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-22 env TEST=22 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-23 env TEST=23 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-24 env TEST=24 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-25 env TEST=25 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-26 env TEST=26 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)


#
# The following 6 tests have the same flow:
#
# 3 RangeServers; 1 crashes, and the Master crashes during recovery via
# the FailureInducer. Another RangeServer crashes as well, and all 
# ranges are then assigned to the remaining RangeServer.
#
add_test(RangeServer-failover-master-24 env TEST_24=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-25 env TEST_25=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-26 env TEST_26=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-27 env TEST_27=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-28 env TEST_28=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-29 env TEST_29=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

#
# The following 13 tests have the same flow:
#
# 3 RangeServers; 1 crashes to trigger the recovery process, and another
# one crashes via the FailureInducer. All ranges are then assigned to 
# the remaining RangeServer.
#
add_test(RangeServer-failover-master-30 env TEST_30=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-31 env TEST_31=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-32 env TEST_32=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-33 env TEST_33=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-34 env TEST_34=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-35 env TEST_35=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-36 env TEST_36=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-37 env TEST_37=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-38 env TEST_38=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-39 env TEST_39=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-40 env TEST_40=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-41 env TEST_41=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-42 env TEST_42=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

#
# The following 4 tests have the same flow:
#
# 3 RangeServers. During recovery the master crashes. Before the master 
# comes up again, the second range server will go down and force the master 
# to re-create the recovery plan.
#
add_test(RangeServer-failover-master-43 env TEST_43=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-44 env TEST_44=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-45 env TEST_45=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-46 env TEST_46=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

#
# The following 5 tests have the same flow:
#
# 3 RangeServers; 1 crashes, and the Master crashes during recovery via
# the FailureInducer. Another RangeServer crashes as well, and all 
# ranges are then assigned to the remaining RangeServer.
#
add_test(RangeServer-failover-master-47 env TEST_47=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-48 env TEST_48=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-49 env TEST_49=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-50 env TEST_50=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-51 env TEST_51=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-52 env TEST_52=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-53 env TEST_53=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-54 env TEST_54=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-55 env TEST_55=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-56 env TEST_56=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-57 env TEST_57=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-58 env TEST_58=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-59 env TEST_59=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(RangeServer-failover-master-60 env TEST_60=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

