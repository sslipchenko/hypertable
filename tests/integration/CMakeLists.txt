# CMake file for integration tests

#add_subdirectory(scanner-create-failure)
add_subdirectory(hyperspace-dump)
add_subdirectory(balance)
add_subdirectory(balance-inactive)
add_subdirectory(balance-new-server)
#add_subdirectory(balance-offload)
add_subdirectory(block-cache-uncompressed)
add_subdirectory(load-balancer)
add_subdirectory(scanner-abrupt-end)
add_subdirectory(scanner-failure)
add_subdirectory(future-abrupt-end)
add_subdirectory(future-mutator-cancel)
add_subdirectory(random)
add_subdirectory(mutator-no-log-sync)
add_subdirectory(cellstore-gc)
add_subdirectory(commit-log-gc)
add_subdirectory(ag-garbage-compaction)
add_subdirectory(dual-instances)
add_subdirectory(load-exception-after-split)
add_subdirectory(master-failover)
add_subdirectory(move-master-failure)
add_subdirectory(metadata-split)
add_subdirectory(split-failover)
add_subdirectory(multiple-maintenance-threads)
add_subdirectory(row-overflow)
add_subdirectory(split-offsets)
add_subdirectory(split-indices)
add_subdirectory(prefix-indices)
add_subdirectory(purge-indices)
add_subdirectory(fail-index-mutator)
add_subdirectory(rowkey-ag-imbalance)
#add_subdirectory(scan-concurrency)
add_subdirectory(sequential-load)
add_subdirectory(split-recovery)
add_subdirectory(split-merge-loop10)
add_subdirectory(group-commit-split)
#comment this out for now: doesn't seem worth the 60s it adds to regression runtime
#add_subdirectory(metadata-update-failure) 
add_subdirectory(bloomfilter)
add_subdirectory(scan-limit)
add_subdirectory(thrift-reconnect-hyperspace)
add_subdirectory(thrift-table-refresh)
add_subdirectory(defects/issue444)
add_subdirectory(defects/issue719)
add_subdirectory(defects/issue720)
add_subdirectory(defects/issue777)
add_subdirectory(defects/issue736)
add_subdirectory(defects/issue779)
add_subdirectory(defects/issue783)
add_subdirectory(defects/issue819)
add_subdirectory(defects/issue827)
add_subdirectory(defects/issue833)
add_subdirectory(defects/issue842)
add_subdirectory(defects/issue855)
add_subdirectory(defects/issue866)
add_subdirectory(defects/issue873)
add_subdirectory(defects/issue890)
add_subdirectory(defects/issue895)
add_subdirectory(defects/issue910)
add_subdirectory(defects/issue921)
add_subdirectory(defects/issue922)
add_subdirectory(replication-hql-tests)
add_subdirectory(rangeserver-failover-basic)
add_subdirectory(rangeserver-failover-master)

add_subdirectory(replication-scripts)
add_subdirectory(replication-master-failover)
add_subdirectory(replication-schema-shipping-1)
add_subdirectory(replication-schema-shipping-2)
add_subdirectory(replication-schema-shipping-3)

# basic replication with 1 master/1 slave each
add_subdirectory(replication-basic-1)

# basic replication with 1 master/1 slave each
add_subdirectory(replication-basic-2)

# 2 rangeservers, 1 master/1 slave each
add_subdirectory(replication-basic-3)

# 2 rangeservers, 1 master/2 slaves each
add_subdirectory(replication-basic-4)

# 2 rangeservers, 1 master/2 slaves each; second local slave crashes after
# transmitting the first fragment
add_subdirectory(replication-basic-5)

# 2 rangeservers, 1 master/2 slaves each; one of the remote slaves crashes
# while receiving updates
add_subdirectory(replication-basic-6)

# 2 rangeservers, 2 masters/2 slaves each; one of the local masters crashes
add_subdirectory(replication-basic-7)

# 2 rangeservers, 2 masters/2 slaves each; they start up server by server,
# all the while the user inserts data
add_subdirectory(replication-basic-8)

# 2 tables, both are replicated
add_subdirectory(replication-basic-9)

# Same as test -6, but both remote slaves crash; the local master moves all
# failed fragments to /hypertable/servers/replicator/errors
add_subdirectory(replication-basic-10)

# 2 rangeservers, 1 master/2 slaves each; second local slave receives sigstop
add_subdirectory(replication-sigstop)
