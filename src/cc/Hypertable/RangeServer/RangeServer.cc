/*
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/** @file
 * Definitions for RangeServer.
 * This file contains method and type definitions for the RangeServer
 */


#include "Common/Compat.h"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <fstream>
#include <sstream>

#include <boost/algorithm/string.hpp>

extern "C" {
#include <fcntl.h>
#include <math.h>
#include <sys/time.h>
#include <sys/resource.h>
}

#if defined(TCMALLOC)
#include <google/tcmalloc.h>
#include <google/heap-checker.h>
#include <google/heap-profiler.h>
#include <google/malloc_extension.h>
#endif

#include "Common/FailureInducer.h"
#include "Common/FileUtils.h"
#include "Common/HashMap.h"
#include "Common/md5.h"
#include "Common/Random.h"
#include "Common/StringExt.h"
#include "Common/SystemInfo.h"
#include "Common/ScopeGuard.h"

#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/MetaLogDefinition.h"
#include "Hypertable/Lib/MetaLogReader.h"
#include "Hypertable/Lib/MetaLogWriter.h"
#include "Hypertable/Lib/PseudoTables.h"
#include "Hypertable/Lib/RangeServerProtocol.h"
#include "Hypertable/Lib/RangeRecoveryReceiverPlan.h"

#include "DfsBroker/Lib/Client.h"

#include "FillScanBlock.h"
#include "Global.h"
#include "GroupCommit.h"
#include "HandlerFactory.h"
#include "LocationInitializer.h"
#include "MaintenanceQueue.h"
#include "MaintenanceScheduler.h"
#include "MaintenanceTaskCompaction.h"
#include "MaintenanceTaskSplit.h"
#include "MaintenanceTaskRelinquish.h"
#include "MergeScanner.h"
#include "MergeScannerRange.h"
#include "MetaLogEntityRange.h"
#include "MetaLogEntityRemoveOkLogs.h"
#include "MetaLogEntityTask.h"
#include "RangeServer.h"
#include "ScanContext.h"
#include "UpdateThread.h"
#include "ReplayBuffer.h"
#include "MetaLogDefinitionRangeServer.h"
#include "TableSchemaCache.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;
using namespace Hypertable::Property;

RangeServer::RangeServer(PropertiesPtr &props, ConnectionManagerPtr &conn_mgr,
    ApplicationQueuePtr &app_queue, Hyperspace::SessionPtr &hyperspace)
  : m_update_commit_queue_count(0), m_root_replay_finished(false),
    m_metadata_replay_finished(false), m_system_replay_finished(false),
    m_replay_finished(false), m_props(props), m_verbose(false),
    m_shutdown(false), m_comm(conn_mgr->get_comm()), m_conn_manager(conn_mgr),
    m_app_queue(app_queue), m_hyperspace(hyperspace), m_timer_handler(0),
    m_group_commit_timer_handler(0), m_query_cache(0),
    m_last_revision(TIMESTAMP_MIN), m_last_metrics_update(0),
    m_loadavg_accum(0.0), m_page_in_accum(0), m_page_out_accum(0),
    m_metric_samples(0), m_maintenance_pause_interval(0), m_pending_metrics_updates(0),
    m_profile_query(false)
{

  uint16_t port;
  m_cores = System::cpu_info().total_cores;
  HT_ASSERT(m_cores != 0);
  SubProperties cfg(props, "Hypertable.RangeServer.");

  m_verbose = props->get_bool("verbose");
  Global::row_size_unlimited = cfg.get_bool("Range.RowSize.Unlimited", false);
  Global::failover_timeout = props->get_i32("Hypertable.Failover.Timeout");
  Global::range_split_size = cfg.get_i64("Range.SplitSize");
  Global::range_maximum_size = cfg.get_i64("Range.MaximumSize");
  Global::range_metadata_split_size = cfg.get_i64("Range.MetadataSplitSize",
          Global::range_split_size);
  Global::access_group_garbage_compaction_threshold =
      cfg.get_i32("AccessGroup.GarbageThreshold.Percentage");
  Global::access_group_max_mem = cfg.get_i64("AccessGroup.MaxMemory");
  Global::enable_shadow_cache = cfg.get_bool("AccessGroup.ShadowCache");
  Global::cellstore_target_size_min = cfg.get_i64("CellStore.TargetSize.Minimum");
  Global::cellstore_target_size_max = Global::cellstore_target_size_min
      + cfg.get_i64("CellStore.TargetSize.Window");
  Global::pseudo_tables = PseudoTables::instance();
  m_scanner_buffer_size = cfg.get_i64("Scanner.BufferSize");
  port = cfg.get_i16("Port");
  m_update_coalesce_limit = cfg.get_i64("UpdateCoalesceLimit");
  m_maintenance_pause_interval = cfg.get_i32("Testing.MaintenanceNeeded.PauseInterval");

  m_control_file_check_interval = cfg.get_i32("ControlFile.CheckInterval");
  boost::xtime_get(&m_last_control_file_check, boost::TIME_UTC_);

  /** Compute maintenance threads **/
  uint32_t maintenance_threads;
  {
    int32_t disk_count = System::get_drive_count();
    maintenance_threads = std::max(((disk_count*3)/2), (int32_t)m_cores);
    if (maintenance_threads < 2)
      maintenance_threads = 2;
    maintenance_threads = cfg.get_i32("MaintenanceThreads", maintenance_threads);
    cout << "drive count = " << disk_count << "\nmaintenance threads = "
        << maintenance_threads << endl;
  }

  Global::toplevel_dir = props->get_str("Hypertable.Directory");
  boost::trim_if(Global::toplevel_dir, boost::is_any_of("/"));
  Global::toplevel_dir = String("/") + Global::toplevel_dir;

  Global::merge_cellstore_run_length_threshold = cfg.get_i32("CellStore.Merge.RunLengthThreshold");
  Global::ignore_clock_skew_errors = cfg.get_bool("IgnoreClockSkewErrors");

  int64_t interval = (int64_t)cfg.get_i32("Maintenance.Interval");

  Global::load_statistics = new LoadStatistics(interval);

  m_stats = new StatsRangeServer(m_props);

  m_namemap = new NameIdMapper(m_hyperspace, Global::toplevel_dir);

  m_group_commit = new GroupCommit(this);

  m_scanner_ttl = (time_t)cfg.get_i32("Scanner.Ttl");

  Global::metrics_interval = props->get_i32("Hypertable.LoadMetrics.Interval");
  if (HT_FAILURE_SIGNALLED("report-metrics-immediately")) {
    m_next_metrics_update = time(0);
  }
  else {
    m_next_metrics_update = time(0) + Global::metrics_interval;
    m_next_metrics_update -= m_next_metrics_update % Global::metrics_interval;
    // randomly pick a time within 5 minutes of the next update
    m_next_metrics_update = (m_next_metrics_update - 150)
        + (Random::number32() % 300);
  }

  Global::cell_cache_scanner_cache_size =
    cfg.get_i32("AccessGroup.CellCache.ScannerCacheSize");

  if (m_scanner_ttl < (time_t)10000) {
    HT_WARNF("Value %u for Hypertable.RangeServer.Scanner.ttl is too small, "
             "setting to 10000", (unsigned int)m_scanner_ttl);
    m_scanner_ttl = (time_t)10000;
  }

  const MemStat &mem_stat = System::mem_stat();
  if (cfg.has("MemoryLimit"))
    Global::memory_limit = cfg.get_i64("MemoryLimit");
  else {
    double pct = std::max(1.0, std::min((double)cfg.get_i32("MemoryLimit.Percentage"), 99.0)) / 100.0;
    Global::memory_limit = (int64_t)(mem_stat.ram * Property::MiB * pct);
  }

  if (cfg.has("MemoryLimit.EnsureUnused"))
    Global::memory_limit_ensure_unused = cfg.get_i64("MemoryLimit.EnsureUnused");
  else if (cfg.has("MemoryLimit.EnsureUnused.Percentage")) {
    double pct = std::max(1.0, std::min((double)cfg.get_i32("MemoryLimit.EnsureUnused.Percentage"), 99.0)) / 100.0;
    Global::memory_limit_ensure_unused = (int64_t)(mem_stat.ram * Property::MiB * pct);
  }

  if (Global::memory_limit_ensure_unused) {
    // adjust current limit according to the actual memory situation
    int64_t free_memory_50pct = (int64_t)(0.5 * mem_stat.free * Property::MiB);
    Global::memory_limit_ensure_unused_current = std::min(free_memory_50pct, Global::memory_limit_ensure_unused);
    if (Global::memory_limit_ensure_unused_current < Global::memory_limit_ensure_unused)
      HT_NOTICEF("Start up in low memory condition (free memory %.2fMB)", mem_stat.free);
  }

  m_max_clock_skew = cfg.get_i32("ClockSkew.Max");

  m_update_delay = cfg.get_i32("UpdateDelay", 0);

  int64_t block_cache_min = cfg.get_i64("BlockCache.MinMemory");
  int64_t block_cache_max = cfg.get_i64("BlockCache.MaxMemory");
  if (block_cache_max == -1) {
    double physical_ram = mem_stat.ram * Property::MiB;
    block_cache_max = (int64_t)physical_ram;
  }
  if (block_cache_min > block_cache_max)
    block_cache_min = block_cache_max;

  if (block_cache_max > 0)
    Global::block_cache = new FileBlockCache(block_cache_min, block_cache_max,
					     cfg.get_bool("BlockCache.Compressed"));

  int64_t query_cache_memory = cfg.get_i64("QueryCache.MaxMemory");
  if (query_cache_memory > 0) {
    // reduce query cache if required
    if ((double)query_cache_memory > (double)Global::memory_limit * 0.2) {
      query_cache_memory = (int64_t)((double)Global::memory_limit * 0.2);
      props->set("Hypertable.RangeServer.QueryCache.MaxMemory", query_cache_memory);
      HT_INFOF("Maximum size of query cache has been reduced to %.2fMB", (double)query_cache_memory / Property::MiB);
    }
    m_query_cache = new QueryCache(query_cache_memory);
  }

  Global::memory_tracker = new MemoryTracker(Global::block_cache, m_query_cache);

  Global::protocol = new Hypertable::RangeServerProtocol();

  DfsBroker::Client *dfsclient = new DfsBroker::Client(conn_mgr, props);

  int dfs_timeout;
  if (props->has("DfsBroker.Timeout"))
    dfs_timeout = props->get_i32("DfsBroker.Timeout");
  else
    dfs_timeout = props->get_i32("Hypertable.Request.Timeout");

  if (!dfsclient->wait_for_connection(dfs_timeout))
    HT_THROW(Error::REQUEST_TIMEOUT, "connecting to DFS Broker");

  Global::dfs = dfsclient;

  m_log_roll_limit = cfg.get_i64("CommitLog.RollLimit");

  m_dropped_table_id_cache = new TableIdCache(50);

  /**
   * Check for and connect to commit log DFS broker
   */
  if (cfg.has("CommitLog.DfsBroker.Host")) {
    String loghost = cfg.get_str("CommitLog.DfsBroker.Host");
    uint16_t logport = cfg.get_i16("CommitLog.DfsBroker.Port");
    InetAddr addr(loghost, logport);

    dfsclient = new DfsBroker::Client(conn_mgr, addr, dfs_timeout);

    if (!dfsclient->wait_for_connection(30000))
      HT_THROW(Error::REQUEST_TIMEOUT, "connecting to commit log DFS broker");

    Global::log_dfs = dfsclient;
  }
  else
    Global::log_dfs = Global::dfs;

  // Create the maintenance queue
  Global::maintenance_queue = new MaintenanceQueue(maintenance_threads);

  m_live_map = new TableInfoMap();

  /**
   * Listen for incoming connections
   */
  ConnectionHandlerFactoryPtr chfp =
      new HandlerFactory(m_comm, m_app_queue, this);

  InetAddr listen_addr(INADDR_ANY, port);
  try {
    m_comm->listen(listen_addr, chfp);
  }
  catch (Exception &e) {
    HT_ERRORF("Unable to listen on port %u - %s - %s",
              port, Error::get_text(e.code()), e.what());
    _exit(0);
  }

  Global::location_initializer = new LocationInitializer(m_props);

  if(Global::location_initializer->is_removed(Global::toplevel_dir+"/servers", m_hyperspace)) {
    HT_ERROR_OUT << "location " << Global::location_initializer->get()
        << " has been marked removed in hyperspace" << HT_END;
    _exit(1);
  }

  // Create Master client
  int timeout = props->get_i32("Hypertable.Request.Timeout");
  ApplicationQueueInterfacePtr aq = m_app_queue;
  m_master_connection_handler = new ConnectionHandler(m_comm, m_app_queue, this);
  m_master_client = new MasterClient(m_conn_manager, m_hyperspace,
                                     Global::toplevel_dir, timeout, aq,
                                     m_master_connection_handler,
                                     Global::location_initializer);
  Global::master_client = m_master_client;

  Global::location_initializer->wait_for_handshake();

  initialize(props);

  Global::log_prune_threshold_min = cfg.get_i64("CommitLog.PruneThreshold.Min");

  uint32_t max_memory_percentage =
    cfg.get_i32("CommitLog.PruneThreshold.Max.MemoryPercentage");

  HT_ASSERT(max_memory_percentage >= 0 && max_memory_percentage <= 100);

  double max_memory_ratio = (double)max_memory_percentage / 100.0;

  int64_t threshold_max = (int64_t)(mem_stat.ram *
                                    max_memory_ratio * (double)MiB);

  Global::log_prune_threshold_max = cfg.get_i64("CommitLog.PruneThreshold.Max", threshold_max);

  /**
   * Create maintenance scheduler
   */
  m_maintenance_scheduler = new MaintenanceScheduler(Global::maintenance_queue, m_live_map);

  // Install maintenance timer
  m_timer_handler = new TimerHandler(m_comm, this);

  // Create "update" threads
  for (int i=0; i<3; i++)
    m_update_threads.push_back( new Thread(UpdateThread(this, i)) );

  local_recover();

  m_timer_handler->start();

  HT_INFOF("Prune thresholds - min=%lld, max=%lld", (Lld)Global::log_prune_threshold_min,
           (Lld)Global::log_prune_threshold_max);

  m_group_commit_timer_handler = new GroupCommitTimerHandler(m_comm, this, m_app_queue);

}

void RangeServer::shutdown() {

  try {

    // stop maintenance timer
    if (m_timer_handler) {
      m_timer_handler->shutdown();
      m_timer_handler->wait_for_shutdown();
    }

    // stop maintenance queue
    Global::maintenance_queue->shutdown();
    //Global::maintenance_queue->join();

    // stop application queue
    m_app_queue->stop();
    boost::xtime deadline;
    boost::xtime_get(&deadline, boost::TIME_UTC_);
    deadline.sec += 30;  // wait no more than 30 seconds
    m_app_queue->wait_for_idle(deadline, 1);

    ScopedLock lock(m_stats_mutex);

    if (m_group_commit_timer_handler)
      m_group_commit_timer_handler->shutdown();

    // Kill update threads
    m_shutdown = true;
    m_update_qualify_queue_cond.notify_all();
    m_update_commit_queue_cond.notify_all();
    m_update_response_queue_cond.notify_all();
    foreach_ht (Thread *thread, m_update_threads)
      thread->join();

    Global::range_locator = 0;

    if (Global::rsml_writer) {
      Global::rsml_writer->close();
      Global::rsml_writer = 0;
    }
    if (Global::root_log) {
      Global::root_log->close();
      /*
      delete Global::root_log;
      Global::root_log = 0;
      */
    }
    if (Global::metadata_log) {
      Global::metadata_log->close();
      /*
      delete Global::metadata_log;
      Global::metadata_log = 0;
      */
    }
    if (Global::system_log) {
      Global::system_log->close();
      /*
      delete Global::system_log;
      Global::system_log = 0;
      */
    }
    if (Global::user_log) {
      Global::user_log->close();
      /*
      delete Global::user_log;
      Global::user_log = 0;
      */
    }

    if (Global::block_cache) {
      delete Global::block_cache;
      Global::block_cache = 0;
    }

    if (m_query_cache) {
       delete m_query_cache;
      m_query_cache = 0;
    }

    /*
    Global::maintenance_queue = 0;
    Global::metadata_table = 0;
    Global::rs_metrics_table = 0;
    Global::hyperspace = 0;

    Global::log_dfs = 0;
    Global::dfs = 0;

    delete Global::memory_tracker;
    Global::memory_tracker = 0;

    delete Global::protocol;
    Global::protocol = 0;
    */

    m_app_queue->shutdown();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }

}

RangeServer::~RangeServer() {
}


/**
 * - Figure out and create range server directory
 * - Clear any Range server state (including any partially created commit logs)
 * - Open the commit log
 */
void RangeServer::initialize(PropertiesPtr &props) {
  String top_dir = Global::toplevel_dir + "/servers/" + Global::location_initializer->get();
  uint32_t lock_status;
  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;

  m_existence_file_handle = m_hyperspace->open(top_dir, oflags);

  while (true) {
    lock_status = 0;

    m_hyperspace->try_lock(m_existence_file_handle, LOCK_MODE_EXCLUSIVE,
                           &lock_status, &m_existence_file_sequencer);

    if (lock_status == LOCK_STATUS_GRANTED)
      break;

    HT_INFOF("Waiting for exclusive lock on hyperspace:/%s ...",
             top_dir.c_str());
    poll(0, 0, 5000);
  }

  Global::log_dir = top_dir + "/log";

  /**
   * Create log directories
   */
  String path;
  try {
    path = Global::log_dir + "/user";
    Global::log_dfs->mkdirs(path);
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Problem creating commit log directory '%s': %s",
               path.c_str(), e.what());
  }

  HT_INFOF("log_dir=%s", Global::log_dir.c_str());
}


namespace {

  struct ByFragmentNumber {
    bool operator()(const String &x, const String &y) const {
      int num_x = atoi(x.c_str());
      int num_y = atoi(y.c_str());
      return num_x < num_y;
    }
  };

  void add_mark_file_to_commit_logs(const String &logname) {
    vector<String> listing;
    vector<String> listing2;
    String logdir = Global::log_dir + "/" + logname;

    try {
      if (!Global::log_dfs->exists(logdir))
        return;
      Global::log_dfs->readdir(logdir, listing);
    }
    catch (Hypertable::Exception &e) {
      HT_FATALF("Unable to read log directory '%s'", logdir.c_str());
    }

    if (listing.size() == 0)
      return;

    sort(listing.begin(), listing.end(), ByFragmentNumber());

    // Remove zero-length files
    foreach_ht (String &entry, listing) {
      String fragment_file = logdir + "/" + entry;
      try {
        if (Global::log_dfs->length(fragment_file) == 0) {
          HT_INFOF("Removing log fragment '%s' because it has zero length",
                  fragment_file.c_str());
          Global::log_dfs->remove(fragment_file);
        }
        else
          listing2.push_back(entry);
      }
      catch (Hypertable::Exception &e) {
        HT_FATALF("Unable to check fragment file '%s'", fragment_file.c_str());
      }
    }

    if (listing2.size() == 0)
      return;

    char *endptr;
    long num = strtol(listing2.back().c_str(), &endptr, 10);
    String mark_filename = logdir + "/" + (int64_t)num + ".mark";

    try {
      int fd = Global::log_dfs->create(mark_filename, 0, -1, -1, -1);
      StaticBuffer buf(1);
      *buf.base = '0';
      Global::log_dfs->append(fd, buf);
      Global::log_dfs->close(fd);
    }
    catch (Hypertable::Exception &e) {
      HT_FATALF("Unable to create file '%s'", mark_filename.c_str());
    }
  }

}


void RangeServer::local_recover() {
  MetaLog::DefinitionPtr rsml_definition =
      new MetaLog::DefinitionRangeServer(Global::location_initializer->get().c_str());
  MetaLog::ReaderPtr rsml_reader;
  CommitLogReaderPtr root_log_reader;
  CommitLogReaderPtr system_log_reader;
  CommitLogReaderPtr metadata_log_reader;
  CommitLogReaderPtr user_log_reader;
  Ranges ranges;
  std::vector<MetaLog::EntityPtr> entities, stripped_entities;
  MetaLogEntityRange *range_entity;
  StringSet transfer_logs;
  TableInfoMap replay_map(new TableSchemaCache(m_hyperspace, Global::toplevel_dir));
  int priority = 0;
  bool found_remove_ok_logs = false;

  try {
    std::vector<MaintenanceTask*> maintenance_tasks;
    boost::xtime now;
    boost::xtime_get(&now, boost::TIME_UTC_);

    rsml_reader = new MetaLog::Reader(Global::log_dfs, rsml_definition,
            Global::log_dir + "/" + rsml_definition->name());

    rsml_reader->get_entities(entities);

    if (!entities.empty()) {
      HT_DEBUG_OUT << "Found "<< Global::log_dir << "/"
          << rsml_definition->name() <<", start recovering"<< HT_END;

      // Temporary code to handle upgrade from RANGE to RANGE2
      // Metalog entries.  Should be removed around 12/2013
      if (MetaLogEntityRange::encountered_upgrade) {
        add_mark_file_to_commit_logs("root");
        add_mark_file_to_commit_logs("metadata");
        add_mark_file_to_commit_logs("system");
        add_mark_file_to_commit_logs("user");
      }

      // Populated Global::work_queue and strip out PHANTOM ranges
      {
        MetaLog::EntityTask *task_entity;
        foreach_ht(MetaLog::EntityPtr &entity, entities) {
          if ((task_entity = dynamic_cast<MetaLog::EntityTask *>(entity.get())))
            Global::add_to_work_queue(task_entity);
          else if ((range_entity = dynamic_cast<MetaLogEntityRange *>(entity.get())) != 0 &&
                   (range_entity->get_state() & RangeState::PHANTOM) != 0) {
            // If log was created originally for the phantom range, remove it
            String transfer_log = range_entity->get_transfer_log();
            if (strstr(transfer_log.c_str(), "/phantom-") != 0) {
	      try {
		Global::log_dfs->rmdir(transfer_log);
	      }
	      catch (Exception &e) {
		HT_WARNF("Problem removing phantom log %s - %s", transfer_log.c_str(),
			 Error::get_text(e.code()));
	      }
	    }
            continue;
          }
          else if (dynamic_cast<MetaLogEntityRemoveOkLogs *>(entity.get())) {
            ScopedLock glock(Global::mutex);
            Global::remove_ok_logs = (MetaLogEntityRemoveOkLogs *)entity.get();
	    found_remove_ok_logs = true;
          }
          stripped_entities.push_back(entity);
        }
      }

      if (!found_remove_ok_logs) {
	ScopedLock glock(Global::mutex);
	Global::remove_ok_logs = new MetaLogEntityRemoveOkLogs();
      }

      entities.swap(stripped_entities);

      Global::rsml_writer = new MetaLog::Writer(Global::log_dfs,
              rsml_definition, Global::log_dir + "/" + rsml_definition->name(),
              entities);

      replay_map.clear();
      foreach_ht(MetaLog::EntityPtr &entity, entities) {
        if ((range_entity = dynamic_cast<MetaLogEntityRange *>(entity.get()))) {
          TableIdentifier table;
          String end_row = range_entity->get_end_row();
          range_entity->get_table_identifier(table);
          if (table.is_metadata() && !end_row.compare(Key::END_ROOT_ROW))
            replay_load_range(replay_map, range_entity);
        }
      }

      if (!replay_map.empty()) {
        root_log_reader = new CommitLogReader(Global::log_dfs,
                                              Global::log_dir + "/root");
        replay_log(replay_map, root_log_reader);

        root_log_reader->get_linked_logs(transfer_logs);

        // Perform any range specific post-replay tasks
        ranges.array.clear();
        replay_map.get_ranges(ranges);
        foreach_ht(RangeData &rd, ranges.array) {
          rd.range->recovery_finalize();
          if (rd.range->get_state() == RangeState::SPLIT_LOG_INSTALLED ||
              rd.range->get_state() == RangeState::SPLIT_SHRUNK)
            maintenance_tasks.push_back(new MaintenanceTaskSplit(0, priority++, now, rd.range));
          else if (rd.range->get_state() == RangeState::RELINQUISH_LOG_INSTALLED)
            maintenance_tasks.push_back(new MaintenanceTaskRelinquish(0, priority++, now, rd.range));
          else
            HT_ASSERT(rd.range->get_state() == RangeState::STEADY);
        }
      }

      m_live_map->merge(&replay_map);

      if (root_log_reader)
        Global::root_log = new CommitLog(Global::log_dfs, Global::log_dir
                                         + "/root", m_props, root_log_reader.get());

      {
        ScopedLock lock(m_mutex);
        m_root_replay_finished = true;
        m_root_replay_finished_cond.notify_all();
      }

      // Finish mid-maintenance
      if (!maintenance_tasks.empty()) {
        for (size_t i=0; i<maintenance_tasks.size(); i++)
          Global::maintenance_queue->add(maintenance_tasks[i]);
        maintenance_tasks.clear();
      }

      replay_map.clear();
      foreach_ht (MetaLog::EntityPtr &entity, entities) {
        if ((range_entity = dynamic_cast<MetaLogEntityRange *>(entity.get()))) {
          TableIdentifier table;
          String end_row = range_entity->get_end_row();
          range_entity->get_table_identifier(table);
          if (table.is_metadata() && end_row.compare(Key::END_ROOT_ROW))
            replay_load_range(replay_map, range_entity);
        }
      }

      if (!replay_map.empty()) {
        metadata_log_reader =
          new CommitLogReader(Global::log_dfs, Global::log_dir + "/metadata");

        replay_log(replay_map, metadata_log_reader);

        metadata_log_reader->get_linked_logs(transfer_logs);

        // Perform any range specific post-replay tasks
        ranges.array.clear();
        replay_map.get_ranges(ranges);
        foreach_ht(RangeData &rd, ranges.array) {
          rd.range->recovery_finalize();
          if (rd.range->get_state() == RangeState::SPLIT_LOG_INSTALLED ||
              rd.range->get_state() == RangeState::SPLIT_SHRUNK)
            maintenance_tasks.push_back(new MaintenanceTaskSplit(1, priority++, now, rd.range));
          else if (rd.range->get_state() == RangeState::RELINQUISH_LOG_INSTALLED)
            maintenance_tasks.push_back(new MaintenanceTaskRelinquish(1, priority++, now, rd.range));
          else
            HT_ASSERT(rd.range->get_state() == RangeState::STEADY);
        }
      }

      m_live_map->merge(&replay_map);

      if (metadata_log_reader)
        Global::metadata_log = new CommitLog(Global::log_dfs,
                                             Global::log_dir + "/metadata",
                                             m_props, metadata_log_reader.get());
      {
        ScopedLock lock(m_mutex);
        m_metadata_replay_finished = true;
        m_metadata_replay_finished_cond.notify_all();
      }

      // Finish mid-maintenance
      if (!maintenance_tasks.empty()) {
        for (size_t i=0; i<maintenance_tasks.size(); i++)
          Global::maintenance_queue->add(maintenance_tasks[i]);
        maintenance_tasks.clear();
      }

      replay_map.clear();
      foreach_ht (MetaLog::EntityPtr &entity, entities) {
        if ((range_entity = dynamic_cast<MetaLogEntityRange *>(entity.get()))) {
          TableIdentifier table;
          range_entity->get_table_identifier(table);
          if (table.is_system() && !table.is_metadata())
            replay_load_range(replay_map, range_entity);
        }
      }

      if (!replay_map.empty()) {
        system_log_reader =
          new CommitLogReader(Global::log_dfs, Global::log_dir + "/system");

        replay_log(replay_map, system_log_reader);

        system_log_reader->get_linked_logs(transfer_logs);

        // Perform any range specific post-replay tasks
        ranges.array.clear();
        replay_map.get_ranges(ranges);
        foreach_ht (RangeData &rd, ranges.array) {
          rd.range->recovery_finalize();
          if (rd.range->get_state() == RangeState::SPLIT_LOG_INSTALLED ||
              rd.range->get_state() == RangeState::SPLIT_SHRUNK)
            maintenance_tasks.push_back(new MaintenanceTaskSplit(2, priority++, now, rd.range));
          else if (rd.range->get_state() == RangeState::RELINQUISH_LOG_INSTALLED)
            maintenance_tasks.push_back(new MaintenanceTaskRelinquish(2, priority++, now, rd.range));
          else
            HT_ASSERT(rd.range->get_state() == RangeState::STEADY);
        }
      }

      m_live_map->merge(&replay_map);

      // Create system log and wake up anybody waiting for system replay to
      // complete
      if (system_log_reader)
        Global::system_log = new CommitLog(Global::log_dfs,
                                           Global::log_dir + "/system", m_props,
                                           system_log_reader.get());

      {
        ScopedLock lock(m_mutex);
        m_system_replay_finished = true;
        m_system_replay_finished_cond.notify_all();
      }

      // Finish mid-maintenance
      if (!maintenance_tasks.empty()) {
        for (size_t i=0; i<maintenance_tasks.size(); i++)
          Global::maintenance_queue->add(maintenance_tasks[i]);
        maintenance_tasks.clear();
      }

      if (m_props->get_bool("Hypertable.RangeServer.LoadSystemTablesOnly"))
        return;

      replay_map.clear();
      foreach_ht (MetaLog::EntityPtr &entity, entities) {
        if ((range_entity = dynamic_cast<MetaLogEntityRange *>(entity.get()))) {
          TableIdentifier table;
          range_entity->get_table_identifier(table);
          if (!table.is_system())
            replay_load_range(replay_map, range_entity);
        }
      }

      if (!replay_map.empty()) {
        user_log_reader = new CommitLogReader(Global::log_dfs,
                                              Global::log_dir + "/user");

        replay_log(replay_map, user_log_reader);

        user_log_reader->get_linked_logs(transfer_logs);

        // Perform any range specific post-replay tasks
        ranges.array.clear();
        replay_map.get_ranges(ranges);
        foreach_ht(RangeData &rd, ranges.array) {
          rd.range->recovery_finalize();
          if (rd.range->get_state() == RangeState::SPLIT_LOG_INSTALLED ||
              rd.range->get_state() == RangeState::SPLIT_SHRUNK)
            maintenance_tasks.push_back(new MaintenanceTaskSplit(3, priority++, now, rd.range));
          else if (rd.range->get_state() == RangeState::RELINQUISH_LOG_INSTALLED)
            maintenance_tasks.push_back(new MaintenanceTaskRelinquish(3, priority++, now, rd.range));
          else
            HT_ASSERT((rd.range->get_state() & ~RangeState::PHANTOM)
                    == RangeState::STEADY);
        }
      }

      m_live_map->merge(&replay_map);

      Global::user_log = new CommitLog(Global::log_dfs, Global::log_dir
                                       + "/user", m_props, user_log_reader.get(), false);

      {
        ScopedLock lock(m_mutex);
        m_replay_finished = true;
        m_replay_finished_cond.notify_all();
      }

      // Finish mid-maintenance
      if (!maintenance_tasks.empty()) {
        for (size_t i=0; i<maintenance_tasks.size(); i++)
          Global::maintenance_queue->add(maintenance_tasks[i]);
        maintenance_tasks.clear();
      }

      HT_NOTICE("Replay finished");

    }
    else {
      ScopedLock lock(m_mutex);

      // Create RemoveOkLogs entity
      {
	ScopedLock glock(Global::mutex);
	Global::remove_ok_logs = new MetaLogEntityRemoveOkLogs();
      }

      /**
       *  Create the logs
       */

      if (root_log_reader)
        Global::root_log = new CommitLog(Global::log_dfs, Global::log_dir
            + "/root", m_props, root_log_reader.get());

      if (metadata_log_reader)
        Global::metadata_log = new CommitLog(Global::log_dfs, Global::log_dir
            + "/metadata", m_props, metadata_log_reader.get());

      if (system_log_reader)
        Global::system_log = new CommitLog(Global::log_dfs, Global::log_dir
            + "/system", m_props, system_log_reader.get());

      Global::user_log = new CommitLog(Global::log_dfs, Global::log_dir
          + "/user", m_props, user_log_reader.get(), false);

      Global::rsml_writer = new MetaLog::Writer(Global::log_dfs, rsml_definition,
                                                Global::log_dir + "/" + rsml_definition->name(),
                                                entities);

      m_root_replay_finished = true;
      m_metadata_replay_finished = true;
      m_system_replay_finished = true;
      m_replay_finished = true;

      m_root_replay_finished_cond.notify_all();
      m_metadata_replay_finished_cond.notify_all();
      m_system_replay_finished_cond.notify_all();
      m_replay_finished_cond.notify_all();

    }

    if (!found_remove_ok_logs) {
      Global::remove_ok_logs->insert(transfer_logs);
      Global::rsml_writer->record_state(Global::remove_ok_logs.get());
    }

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    HT_ABORT;
  }
}


void
RangeServer::replay_load_range(TableInfoMap &replay_map,
                               MetaLogEntityRange *range_entity) {
  SchemaPtr schema;
  TableInfoPtr table_info, live_table_info;
  TableIdentifier table;
  RangeSpecManaged range_spec;
  RangePtr range;

  HT_DEBUG_OUT << "replay_load_range " << *(MetaLog::Entity *)range_entity
      << HT_END;

  range_entity->get_table_identifier(table);
  range_entity->get_range_spec(range_spec);

  try {

    replay_map.get(table.id, table_info);

    m_live_map->get(table.id, live_table_info);

    // Range should not already be loaded
    HT_ASSERT(!live_table_info->get_range(&range_spec, range));

    // Check table generation.  If table generation obtained from TableInfoMap
    // is greater than the table generation in the range entity, then
    // automatically upgrade to new generation
    uint32_t generation = live_table_info->get_schema()->get_generation();
    if (generation > table.generation) {
      range_entity->set_table_generation(generation);
      table.generation = generation;
    }
    HT_ASSERT(generation == table.generation);

    /**
     * Lazily create sys/METADATA table pointer
     */
    if (!Global::metadata_table) {
      ScopedLock lock(Global::mutex);
      uint32_t timeout_ms = m_props->get_i32("Hypertable.Request.Timeout");
      if (!Global::range_locator)
        Global::range_locator = new Hypertable::RangeLocator(m_props,
                m_conn_manager, Global::hyperspace, timeout_ms);
      ApplicationQueueInterfacePtr aq = m_app_queue;
      Global::metadata_table = new Table(m_props, Global::range_locator,
              m_conn_manager, Global::hyperspace, aq, m_namemap,
                           TableIdentifier::METADATA_NAME, 0, timeout_ms);
    }

    schema = table_info->get_schema();

    range = new Range(m_master_client, schema, range_entity,
                      live_table_info.get());

    range->recovery_initialize();

    table_info->add_range(range);

    HT_INFOF("Successfully replay loaded range %s", range->get_name().c_str());

  }
  catch (Hypertable::Exception &e) {
    if (e.code() == Error::RANGESERVER_TABLE_NOT_FOUND && !table.is_system()) {
      HT_WARNF("Skipping recovery of %s[%s..%s] - %s",
               table.id, range_spec.start_row, range_spec.end_row,
               e.what());
      return;
    }
    HT_FATAL_OUT << "Problem loading range during replay - " << e << HT_END;
  }
}


void RangeServer::replay_log(TableInfoMap &replay_map,
                             CommitLogReaderPtr &log_reader) {
  BlockCompressionHeaderCommitLog header;
  uint8_t *base;
  size_t len;
  TableIdentifier table_id;
  DynamicBuffer dbuf;
  const uint8_t *ptr, *end;
  int64_t revision;
  TableInfoPtr table_info;
  SerializedKey key;
  ByteString value;
  uint32_t block_count = 0;

  while (log_reader->next((const uint8_t **)&base, &len, &header)) {

    revision = header.get_revision();

    ptr = base;
    end = base + len;

    table_id.decode(&ptr, &len);

    // Fetch table info
    if (!replay_map.lookup(table_id.id, table_info))
      continue;

    dbuf.clear();
    dbuf.ensure(table_id.encoded_length() + 12 + len);

    dbuf.ptr += 4;  // skip size
    encode_i64(&dbuf.ptr, revision);
    table_id.encode(&dbuf.ptr);
    base = dbuf.ptr;

    while (ptr < end) {

      // extract the key
      key.ptr = ptr;
      ptr += key.length();
      if (ptr > end)
        HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding key");

      // extract the value
      value.ptr = ptr;
      ptr += value.length();
      if (ptr > end)
        HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding value");

      // Look for containing range, add to stop mods if not found
      if (!table_info->includes_row(key.row()))
        continue;

      // add key/value pair to buffer
      memcpy(dbuf.ptr, key.ptr, ptr-key.ptr);
      dbuf.ptr += ptr-key.ptr;

    }

    uint32_t block_size = dbuf.ptr - base;
    base = dbuf.base;
    encode_i32(&base, block_size);

    replay_update(replay_map, dbuf.base, dbuf.fill());
    block_count++;
  }

  HT_INFOF("Replayed %u blocks of updates from '%s'", block_count,
           log_reader->get_log_dir().c_str());
}

void
RangeServer::compact(ResponseCallback *cb, const TableIdentifier *table,
                     const char *row, uint32_t flags) {
  Ranges ranges;
  TableInfoPtr table_info;
  String start_row, end_row;
  RangePtr range;
  size_t range_count = 0;

  if (table)
    HT_INFOF("compacting table ID=%s ROW=%s", table->id, row ? row : "");
  else
    HT_INFOF("compacting ranges FLAGS=%s", RangeServerProtocol::compact_flags_to_string(flags).c_str());

  if (!m_replay_finished) {
    if (!RangeServer::wait_for_recovery_finish(cb->get_event()->expiration_time()))
      return;
  }

  try {

    if (table) {

      if (!m_live_map->lookup(table->id, table_info)) {
        cb->error(Error::TABLE_NOT_FOUND, table->id);
        return;
      }

      if (row) {
        if (!table_info->find_containing_range(row, range, start_row, end_row)) {
          cb->error(Error::RANGESERVER_RANGE_NOT_FOUND, 
                    format("Unable to find range for row '%s'", row));
          return;
        }
        range->set_needs_compaction(true);
        range_count = 1;
      }
      else {
        ranges.array.clear();
        table_info->get_ranges(ranges);
        foreach_ht(RangeData &rd, ranges.array)
          rd.range->set_needs_compaction(true);
        range_count = ranges.array.size();
      }
    }
    else {
      std::vector<TableInfoPtr> tables;

      m_live_map->get_all(tables);

      for (size_t i=0; i<tables.size(); i++) {

        if (tables[i]->identifier().is_metadata()) {

          if ((flags & RangeServerProtocol::COMPACT_FLAG_METADATA) ==
              RangeServerProtocol::COMPACT_FLAG_METADATA) {
            ranges.array.clear();
            tables[i]->get_ranges(ranges);
            foreach_ht(RangeData &rd, ranges.array)
              rd.range->set_needs_compaction(true);
            range_count += ranges.array.size();
          }
          else if ((flags & RangeServerProtocol::COMPACT_FLAG_ROOT) ==
                   RangeServerProtocol::COMPACT_FLAG_ROOT) {
            ranges.array.clear();
            tables[i]->get_ranges(ranges);
            foreach_ht(RangeData &rd, ranges.array) {
              if (rd.range->is_root()) {
                rd.range->set_needs_compaction(true);
                range_count++;
                break;
              }
            }
          }
        }
        else if (tables[i]->identifier().is_system()) {
          if ((flags & RangeServerProtocol::COMPACT_FLAG_SYSTEM) == RangeServerProtocol::COMPACT_FLAG_SYSTEM) {
            ranges.array.clear();
            tables[i]->get_ranges(ranges);
            foreach_ht(RangeData &rd, ranges.array)
              rd.range->set_needs_compaction(true);
            range_count += ranges.array.size();
          }
        }
        else {
          if ((flags & RangeServerProtocol::COMPACT_FLAG_USER) == RangeServerProtocol::COMPACT_FLAG_USER) {
            ranges.array.clear();
            tables[i]->get_ranges(ranges);
            foreach_ht(RangeData &rd, ranges.array)
              rd.range->set_needs_compaction(true);
            range_count += ranges.array.size();
          }
        }
      }
    }

    HT_INFOF("Compaction scheduled for %d ranges", (int)range_count);

    cb->response_ok();

  }
  catch (Hypertable::Exception &e) {
    int error;
    HT_ERROR_OUT << e << HT_END;
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}

namespace {

  void do_metadata_sync(RangeData &rd, TableMutatorPtr &mutator,
                        const char *table_id, bool do_start_row, bool do_location) {
    String metadata_key_str;
    String start_row, end_row;
    KeySpec key;

    rd.range->get_boundary_rows(start_row, end_row);

    metadata_key_str = String(table_id) + ":" + end_row;
    key.row = metadata_key_str.c_str();
    key.row_len = metadata_key_str.length();
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;

    if (do_start_row) {
      key.column_family = "StartRow";
      mutator->set(key, start_row);
    }
    if (do_location) {
      key.column_family = "Location";
      mutator->set(key, Global::location_initializer->get());
    }

  }

  void do_metadata_sync(Ranges &ranges, TableMutatorPtr &mutator,
                        const char *table_id, bool do_start_row, bool do_location) {
    foreach_ht(RangeData &rd, ranges.array)
      do_metadata_sync(rd, mutator, table_id, do_start_row, do_location);
  }

}

void
RangeServer::metadata_sync(ResponseCallback *cb, const char *table_id,
        uint32_t flags, std::vector<const char *> columns) {
  Ranges ranges;
  TableInfoPtr table_info;
  size_t range_count = 0;
  TableMutatorPtr mutator;
  bool do_start_row = true;
  bool do_location = true;
  String columns_str;

  if (!columns.empty()) {
    columns_str = String("COLUMNS=") + columns[0];
    for (size_t i=1; i<columns.size(); i++)
      columns_str += String(",") + columns[i];
  }

  if (*table_id)
    HT_INFOF("metadata sync table ID=%s %s", table_id, columns_str.c_str());
  else
    HT_INFOF("metadata sync ranges FLAGS=%s %s",
             RangeServerProtocol::compact_flags_to_string(flags).c_str(),
             columns_str.c_str());

  if (!m_replay_finished) {
    if (!RangeServer::wait_for_recovery_finish(cb->get_event()->expiration_time()))
      return;
  }

  if (!Global::metadata_table) {
    ScopedLock lock(Global::mutex);
    // double-check locking (works fine on x86 and amd64 but may fail
    // on other archs without using a memory barrier
    if (!Global::metadata_table)
      Global::metadata_table = new Table(m_props, m_conn_manager,
                                         Global::hyperspace, m_namemap,
                                         TableIdentifier::METADATA_NAME);
  }

  if (!columns.empty()) {
    do_start_row = do_location = false;
    for (size_t i=0; i<columns.size(); i++) {
      if (!strcmp(columns[i], "StartRow"))
        do_start_row = true;
      else if (!strcmp(columns[i], "Location"))
        do_location = true;
      else
        HT_WARNF("Unsupported METADATA column:  %s", columns[i]);
    }
  }

  try {

    if (*table_id) {

      if (!m_live_map->lookup(table_id, table_info)) {
        cb->error(Error::TABLE_NOT_FOUND, table_id);
        return;
      }

      mutator = Global::metadata_table->create_mutator();

      ranges.array.clear();
      table_info->get_ranges(ranges);

      do_metadata_sync(ranges, mutator, table_id, do_start_row, do_location);
      range_count = ranges.array.size();

    }
    else {
      std::vector<TableInfoPtr> tables;

      m_live_map->get_all(tables);

      mutator = Global::metadata_table->create_mutator();

      for (size_t i=0; i<tables.size(); i++) {

        if (tables[i]->identifier().is_metadata()) {

          ranges.array.clear();
          tables[i]->get_ranges(ranges);

          if (!ranges.array.empty()) {
            if (ranges.array[0].range->is_root() &&
                (flags & RangeServerProtocol::COMPACT_FLAG_ROOT) == RangeServerProtocol::COMPACT_FLAG_ROOT) {
              do_metadata_sync(ranges.array[0], mutator, table_id, do_start_row, do_location);
              range_count++;
            }
            if ((flags & RangeServerProtocol::COMPACT_FLAG_METADATA) ==
                RangeServerProtocol::COMPACT_FLAG_METADATA) {
              do_metadata_sync(ranges, mutator, table_id, do_start_row, do_location);
              range_count += ranges.array.size();
            }
          }
        }
        else if (tables[i]->identifier().is_system()) {
          if ((flags & RangeServerProtocol::COMPACT_FLAG_SYSTEM) ==
              RangeServerProtocol::COMPACT_FLAG_SYSTEM) {
            ranges.array.clear();
            tables[i]->get_ranges(ranges);
            do_metadata_sync(ranges, mutator, table_id, do_start_row, do_location);
            range_count += ranges.array.size();
          }
        }
        else if (tables[i]->identifier().is_user()) {
          if ((flags & RangeServerProtocol::COMPACT_FLAG_USER) ==
              RangeServerProtocol::COMPACT_FLAG_USER) {
            ranges.array.clear();
            tables[i]->get_ranges(ranges);
            do_metadata_sync(ranges, mutator, table_id, do_start_row, do_location);
            range_count += ranges.array.size();
          }
        }
      }
    }

    if (range_count)
      mutator->flush();

    HT_INFOF("METADATA sync'd for %d ranges", (int)range_count);

    cb->response_ok();

  }
  catch (Hypertable::Exception &e) {
    int error;
    HT_ERROR_OUT << e << HT_END;
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}

void
RangeServer::create_scanner(ResponseCallbackCreateScanner *cb,
        const TableIdentifier *table, const RangeSpec *range_spec,
        const ScanSpec *scan_spec, QueryCache::Key *cache_key) {
  int error = Error::OK;
  String errmsg;
  TableInfoPtr table_info;
  RangePtr range;
  CellListScannerPtr scanner;
  bool more = true;
  uint32_t id = 0;
  SchemaPtr schema;
  ScanContextPtr scan_ctx;
  bool decrement_needed=false;

  HT_DEBUG_OUT <<"Creating scanner:\n"<< *table << *range_spec
               << *scan_spec << HT_END;

  if (!m_replay_finished) {
    if (!wait_for_recovery_finish(table, range_spec, cb->get_event()->expiration_time()))
      return;
  }

  try {
    DynamicBuffer rbuf;

    HT_MAYBE_FAIL("create-scanner-1");
    HT_MAYBE_FAIL_X("create-scanner-user-1", !table->is_system());
    if (scan_spec->row_intervals.size() > 0) {
      if (scan_spec->row_intervals.size() > 1 && !scan_spec->scan_and_filter_rows)
        HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC,
                 "can only scan one row interval");
      if (scan_spec->cell_intervals.size() > 0)
        HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC,
                 "both row and cell intervals defined");
    }

    if (scan_spec->cell_intervals.size() > 1)
      HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC,
               "can only scan one cell interval");

    if (!m_live_map->lookup(table->id, table_info))
      HT_THROW(Error::TABLE_NOT_FOUND, table->id);

    if (!table_info->get_range(range_spec, range))
      HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND, "(a) %s[%s..%s]",
                table->id, range_spec->start_row, range_spec->end_row);

    schema = table_info->get_schema();

    // verify schema
    if (schema->get_generation() != table->generation) {
      HT_THROWF(Error::RANGESERVER_GENERATION_MISMATCH,
                "RangeServer Schema generation for table '%s'"
                " is %lld but supplied is %lld",
                table->id, (Lld)schema->get_generation(),
                (Lld)table->generation);
    }

    range->deferred_initialization(cb->get_event()->header.timeout_ms);

    if (!range->increment_scan_counter())
      HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND,
                "Range %s[%s..%s] dropped or relinquished",
                table->id, range_spec->start_row, range_spec->end_row);

    decrement_needed = true;

    String start_row, end_row;
    range->get_boundary_rows(start_row, end_row);

    // Check to see if range just shrunk
    if (strcmp(start_row.c_str(), range_spec->start_row) ||
        strcmp(end_row.c_str(), range_spec->end_row))
      HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND, "(b) %s[%s..%s]",
                table->id, range_spec->start_row, range_spec->end_row);

    // check query cache
    if (cache_key && m_query_cache && !table->is_metadata()) {
      boost::shared_array<uint8_t> ext_buffer;
      uint32_t ext_len;
      if (m_query_cache->lookup(cache_key, ext_buffer, &ext_len)) {
        // The first argument to the response method is flags and the
        // 0th bit is the EOS (end-of-scan) bit, hence the 1
        if ((error = cb->response(1, id, ext_buffer, ext_len, 0, 0))
                != Error::OK)
          HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
        range->decrement_scan_counter();
        decrement_needed = false;
        return;
      }
    }

    scan_ctx = new ScanContext(range->get_scan_revision(cb->get_event()->header.timeout_ms),
                               scan_spec, range_spec, schema);
    scan_ctx->timeout_ms = cb->get_event()->header.timeout_ms;

    scanner = range->create_scanner(scan_ctx);

    range->decrement_scan_counter();
    decrement_needed = false;

    uint64_t cells_scanned, cells_returned, bytes_scanned, bytes_returned;

    more = FillScanBlock(scanner, rbuf, m_scanner_buffer_size);

    MergeScanner *mscanner = dynamic_cast<MergeScanner*>(scanner.get());

    assert(mscanner);

    mscanner->get_io_accounting_data(&bytes_scanned, &bytes_returned,
                                     &cells_scanned, &cells_returned);

    {
      Locker<LoadStatistics> lock(*Global::load_statistics);
      Global::load_statistics->add_scan_data(1, cells_scanned, bytes_scanned);
      range->add_read_data(cells_scanned, cells_returned, bytes_scanned, bytes_returned,
                           more ? 0 : mscanner->get_disk_read());
    }

    if (more) {
      scan_ctx->deep_copy_specs();
      id = Global::scanner_map.put(scanner, range, table);
    }
    else
      id = 0;

    MergeScannerRange *rscan=dynamic_cast<MergeScannerRange *>(scanner.get());
    int skipped_rows = rscan ? rscan->get_skipped_rows() : 0;
    int skipped_cells = rscan ? rscan->get_skipped_cells() : 0;

    if (table->is_metadata())
      HT_INFOF("Successfully created scanner (id=%u) on table '%s', returning "
               "%lld k/v pairs, more=%lld", id, table->id, (Lld)cells_returned, (Lld) more);

    /**
     *  Send back data
     */
    if (cache_key && m_query_cache && !table->is_metadata() && !more) {
      const char *cache_row_key = scan_spec->cache_key();
      char *row_key_ptr, *tablename_ptr;
      uint8_t *buffer = new uint8_t [ rbuf.fill() + strlen(cache_row_key) + strlen(table->id) + 2 ];
      memcpy(buffer, rbuf.base, rbuf.fill());
      row_key_ptr = (char *)buffer + rbuf.fill();
      strcpy(row_key_ptr, cache_row_key);
      tablename_ptr = row_key_ptr + strlen(row_key_ptr) + 1;
      strcpy(tablename_ptr, table->id);
      boost::shared_array<uint8_t> ext_buffer(buffer);
      if ((error = cb->response(1, id, ext_buffer, rbuf.fill(),
             skipped_rows, skipped_cells)) != Error::OK) {
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
      }
      m_query_cache->insert(cache_key, tablename_ptr, row_key_ptr, ext_buffer, rbuf.fill());
    }
    else {
      short moreflag = more ? 0 : 1;
      StaticBuffer ext(rbuf);
      if ((error = cb->response(moreflag, id, ext, skipped_rows, skipped_cells))
             != Error::OK) {
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
      }
    }

  }
  catch (Hypertable::Exception &e) {
    int error;
    if (decrement_needed)
      range->decrement_scan_counter();
    if (e.code() == Error::RANGESERVER_RANGE_NOT_FOUND)
      HT_INFOF("Range not found - %s", e.what());
    else
      HT_ERROR_OUT << e << HT_END;
    if ((error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}

void
RangeServer::destroy_scanner(ResponseCallback *cb, uint32_t scanner_id) {
  HT_DEBUGF("destroying scanner id=%u", scanner_id);
  Global::scanner_map.remove(scanner_id);
  cb->response_ok();
}

void
RangeServer::fetch_scanblock(ResponseCallbackFetchScanblock *cb,
        uint32_t scanner_id) {
  String errmsg;
  int error = Error::OK;
  CellListScannerPtr scanner;
  RangePtr range;
  bool more = true;
  DynamicBuffer rbuf;
  TableInfoPtr table_info;
  TableIdentifierManaged scanner_table;
  SchemaPtr schema;

  HT_DEBUG_OUT <<"Scanner ID = " << scanner_id << HT_END;

  try {

    if (!Global::scanner_map.get(scanner_id, scanner, range, scanner_table))
      HT_THROW(Error::RANGESERVER_INVALID_SCANNER_ID,
               format("scanner ID %d", scanner_id));

    HT_MAYBE_FAIL_X("fetch-scanblock-user-1", !scanner_table.is_system());

    if (!m_live_map->lookup(scanner_table.id, table_info))
      HT_THROWF(Error::TABLE_NOT_FOUND, "%s", scanner_table.id);

    schema = table_info->get_schema();

    // verify schema
    if (schema->get_generation() != scanner_table.generation) {
      Global::scanner_map.remove(scanner_id);
      HT_THROW(Error::RANGESERVER_GENERATION_MISMATCH,
               format("RangeServer Schema generation for table '%s' is %d but "
                      "scanner has generation %d", scanner_table.id,
                      schema->get_generation(), scanner_table.generation));
    }

    uint64_t cells_scanned, cells_returned, bytes_scanned, bytes_returned;

    more = FillScanBlock(scanner, rbuf, m_scanner_buffer_size);

    MergeScanner *mscanner = dynamic_cast<MergeScanner*>(scanner.get());

    assert(mscanner);

    mscanner->get_io_accounting_data(&bytes_scanned, &bytes_returned,
                                     &cells_scanned, &cells_returned);

    {
      Locker<LoadStatistics> lock(*Global::load_statistics);
      Global::load_statistics->add_scan_data(0, cells_scanned, bytes_scanned);
      range->add_read_data(cells_scanned, cells_returned, bytes_scanned, bytes_returned,
                           more ? 0 : mscanner->get_disk_read());
    }

    if (!more)
      Global::scanner_map.remove(scanner_id);

    /**
     *  Send back data
     */
    {
      short moreflag = more ? 0 : 1;
      StaticBuffer ext(rbuf);

      error = cb->response(moreflag, scanner_id, ext);
      if (error != Error::OK)
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));

      HT_DEBUGF("Successfully fetched %u bytes (%lld k/v pairs) of scan data",
                ext.size-4, (Lld)cells_returned);
    }

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}

void
RangeServer::load_range(ResponseCallback *cb, const TableIdentifier *table,
    const RangeSpec *range_spec, const RangeState *range_state, bool needs_compaction) {
  int error = Error::OK;
  TableMutatorPtr mutator;
  KeySpec key;
  String metadata_key_str;
  bool is_root;
  String errmsg;
  SchemaPtr schema;
  TableInfoPtr table_info;
  RangePtr range;
  String table_dfsdir;
  String range_dfsdir;
  char md5DigestStr[33];
  String location;

  try {
    // this needs to be here to avoid a race condition with drop_table
    if (m_dropped_table_id_cache->contains(table->id)) {
      HT_WARNF("Table %s has been dropped", table->id);
      cb->error(Error::RANGESERVER_TABLE_DROPPED, table->id);
      return;
    }

    if (!m_replay_finished) {
      if (!wait_for_recovery_finish(table, range_spec,
                  cb->get_event()->expiration_time()))
        return;
    }

    {
      ScopedLock lock(m_drop_table_mutex);

      is_root = table->is_metadata() && (*range_spec->start_row == 0)
          && !strcmp(range_spec->end_row, Key::END_ROOT_ROW);

      std::stringstream sout;
      sout << "Loading range: "<< *table <<" "<< *range_spec << " " << *range_state
           << " needs_compaction=" << needs_compaction;
      HT_INFOF("%s", sout.str().c_str());

      if (m_dropped_table_id_cache->contains(table->id)) {
        HT_WARNF("Table %s has been dropped", table->id);
        cb->error(Error::RANGESERVER_TABLE_DROPPED, table->id);
        return;
      }

      HT_MAYBE_FAIL_X("load-range-1", !table->is_system());

      m_live_map->get(table->id, table_info);

      uint32_t generation = table_info->get_schema()->get_generation();
      if (generation > table->generation) {
        HT_WARNF("Table generation mismatch in load range request (%d < %d),"
                 " automatically upgrading", (int)table->generation, (int)generation);
        ((TableIdentifier *)table)->generation = generation;
      }

      // Make sure this range is not already loaded
      if (table_info->has_range(range_spec)) {
        HT_INFOF("Range %s[%s..%s] already loaded",
                 table->id, range_spec->start_row,
                 range_spec->end_row);
        cb->error(Error::RANGESERVER_RANGE_ALREADY_LOADED, "");
        return;
      }

      m_live_map->stage_range(table, range_spec);

      // Lazily create sys/METADATA table pointer
      if (!Global::metadata_table) {
        ScopedLock lock(Global::mutex);
        // double-check locking (works fine on x86 and amd64 but may fail
        // on other archs without using a memory barrier
        if (!Global::metadata_table)
          Global::metadata_table = new Table(m_props, m_conn_manager,
                  Global::hyperspace, m_namemap,
                  TableIdentifier::METADATA_NAME);
      }

      /**
       * Queue "range_start_row" update for sys/RS_METRICS table
       */
      {
        ScopedLock lock(m_pending_metrics_mutex);
        Cell cell;

        if (m_pending_metrics_updates == 0)
          m_pending_metrics_updates = new CellsBuilder();

        String row = Global::location_initializer->get() + ":" + table->id;
        cell.row_key = row.c_str();
        cell.column_family = "range_start_row";
        cell.column_qualifier = range_spec->end_row;
        cell.value = (const uint8_t *)range_spec->start_row;
        cell.value_len = strlen(range_spec->start_row);

        m_pending_metrics_updates->add(cell);
      }

      schema = table_info->get_schema();

      /**
       * Check for existence of and create, if necessary, range directory (md5
       * of endrow) under all locality group directories for this table
       */
      {
        assert(*range_spec->end_row != 0);
        md5_trunc_modified_base64(range_spec->end_row, md5DigestStr);
        md5DigestStr[16] = 0;
        table_dfsdir = Global::toplevel_dir + "/tables/" + table->id;

        foreach_ht(Schema::AccessGroup *ag, schema->get_access_groups()) {
          // notice the below variables are different "range" vs. "table"
          range_dfsdir = table_dfsdir + "/" + ag->name + "/" + md5DigestStr;
          Global::dfs->mkdirs(range_dfsdir);
        }
      }
    }

    HT_MAYBE_FAIL_X("metadata-load-range-1", table->is_metadata());

    range = new Range(m_master_client, table, schema, range_spec,
            table_info.get(), range_state, needs_compaction);

    HT_MAYBE_FAIL_X("metadata-load-range-2", table->is_metadata());

    /**
     * Create root and/or metadata log if necessary
     */
    if (table->is_metadata()) {
      if (is_root) {
        Global::log_dfs->mkdirs(Global::log_dir + "/root");
        ScopedLock lock(Global::mutex);
        Global::root_log = new CommitLog(Global::log_dfs, Global::log_dir
                                         + "/root", m_props);
      }
      else if (Global::metadata_log == 0) {
        Global::log_dfs->mkdirs(Global::log_dir + "/metadata");
        ScopedLock lock(Global::mutex);
        Global::metadata_log = new CommitLog(Global::log_dfs,
                                             Global::log_dir + "/metadata", m_props);
      }
    }
    else if (table->is_system() && Global::system_log == 0) {
      Global::log_dfs->mkdirs(Global::log_dir + "/system");
      ScopedLock lock(Global::mutex);
      Global::system_log = new CommitLog(Global::log_dfs,
                                         Global::log_dir + "/system", m_props);
    }

    /**
     * Take ownership of the range by writing the 'Location' column in the
     * METADATA table, or /hypertable/root{location} attribute of Hyperspace
     * if it is the root range.
     */

    if (!is_root) {
      metadata_key_str = format("%s:%s", table->id, range_spec->end_row);

      /**
       * Take ownership of the range
       */
      mutator = Global::metadata_table->create_mutator();

      key.row = metadata_key_str.c_str();
      key.row_len = strlen(metadata_key_str.c_str());
      key.column_family = "Location";
      key.column_qualifier = 0;
      key.column_qualifier_len = 0;
      location = Global::location_initializer->get();
      mutator->set(key, location.c_str(), location.length());
      mutator->flush();
    }
    else {  //root
      uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE;

      HT_INFO("Loading root METADATA range");

      try {
        location = Global::location_initializer->get();
        m_hyperspace->attr_set(Global::toplevel_dir + "/root", oflags,
                "Location", location.c_str(), location.length());
      }
      catch (Exception &e) {
        HT_ERROR_OUT << "Problem setting attribute 'location' on Hyperspace "
            "file '" << Global::toplevel_dir << "/root'" << HT_END;
        HT_ERROR_OUT << e << HT_END;
        HT_ABORT;
      }
    }

    HT_MAYBE_FAIL_X("metadata-load-range-3", table->is_metadata());

    // make sure that we don't have a clock skew
    // poll() timeout is in milliseconds, revision and now is in nanoseconds
    int64_t now = Hypertable::get_ts64();
    int64_t revision = range->get_scan_revision(cb->get_event()->header.timeout_ms);
    if (revision > now) {
      int64_t diff = (revision - now) / 1000000;
      HT_WARNF("Clock skew detected when loading range; waiting for %lld "
               "millisec", (long long int)diff);
      poll(0, 0, diff);
    }

    {
      ScopedLock lock(m_drop_table_mutex);

      if (m_dropped_table_id_cache->contains(table->id)) {
        HT_WARNF("Table %s has been dropped", table->id);
        cb->error(Error::RANGESERVER_TABLE_DROPPED, table->id);
        m_live_map->unstage_range(table, range_spec);
        return;
      }
      
      m_live_map->add_staged_range(table, range, range_state->transfer_log);
    }

    HT_MAYBE_FAIL_X("user-load-range-4", !table->is_system());
    HT_MAYBE_FAIL_X("metadata-load-range-4", table->is_metadata());

    if (cb && (error = cb->response_ok()) != Error::OK)
      HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
    else
      HT_INFOF("Successfully loaded range %s[%s..%s]", table->id,
               range_spec->start_row, range_spec->end_row);
  }
  catch (Hypertable::Exception &e) {
    if (e.code() != Error::RANGESERVER_TABLE_DROPPED) {
      HT_ERROR_OUT << e << HT_END;
      if (table_info)
        m_live_map->unstage_range(table, range_spec);
    }
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}

void
RangeServer::acknowledge_load(ResponseCallbackAcknowledgeLoad *cb,
        const vector<QualifiedRangeSpec> &ranges) {
  TableInfoPtr table_info;
  RangePtr range;
  map<QualifiedRangeSpec, int> error_map;

  foreach_ht (const QualifiedRangeSpec &rr, ranges) {

    if (!m_replay_finished) {
      if (!wait_for_recovery_finish(&rr.table, &rr.range, cb->get_event()->expiration_time()))
        return;
    }

    {
      ScopedLock lock(m_drop_table_mutex);

      HT_INFOF("Acknowledging range: %s[%s..%s]", rr.table.id,
               rr.range.start_row, rr.range.end_row);

      if (m_dropped_table_id_cache->contains(rr.table.id)) {
        HT_WARNF("Table %s has been dropped", rr.table.id);
        error_map[rr] = Error::RANGESERVER_TABLE_DROPPED;
        continue;
      }

      if (!m_live_map->lookup(rr.table.id, table_info)) {
        error_map[rr] = Error::TABLE_NOT_FOUND;
        HT_WARN_OUT << "Table " << rr.table.id << " not found" << HT_END;
        continue;
      }

      if (!table_info->get_range(&rr.range, range)) {
        error_map[rr] = Error::RANGESERVER_RANGE_NOT_FOUND;
        HT_WARN_OUT << "Range  " << rr << " not found" << HT_END;
        continue;
      }

      if (range->load_acknowledged()) {
        error_map[rr] = Error::OK;
        HT_WARN_OUT << "Range: " << rr << " already acknowledged" << HT_END;
        continue;
      }

      try {
        range->acknowledge_load(cb->get_event()->header.timeout_ms);
      }
      catch(Exception &e) {
        error_map[rr] = e.code();
        HT_ERROR_OUT << e << HT_END;
        continue;
      }

      HT_MAYBE_FAIL_X("metadata-acknowledge-load", rr.table.is_metadata());

      error_map[rr] = Error::OK;
      std::stringstream sout;
      sout << "Range: " << rr <<" acknowledged";
      HT_INFOF("%s", sout.str().c_str());
    }
  }

  cb->response(error_map);
}

void
RangeServer::update_schema(ResponseCallback *cb, 
        const TableIdentifier *table, const char *schema_str) {
  ScopedLock lock(m_drop_table_mutex);
  TableInfoPtr table_info;
  SchemaPtr schema;

  HT_INFOF("Updating schema for: %s schema = %s", table->id, schema_str);

  try {
    /**
     * Create new schema object and test for validity
     */
    schema = Schema::new_instance(schema_str, strlen(schema_str));

    if (!schema->is_valid()) {
      HT_THROW(Error::RANGESERVER_SCHEMA_PARSE_ERROR,
        (String) "Update schema Parse Error for table '"
        + table->id + "' : " + schema->get_error_string());
    }
    if (schema->need_id_assignment())
      HT_THROW(Error::RANGESERVER_SCHEMA_PARSE_ERROR,
               (String) "Update schema Parse Error for table '"
               + table->id + "' : needs ID assignment");

    if (m_live_map->lookup(table->id, table_info))
      table_info->update_schema(schema);

  }
  catch(Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), e.what());
    return;
  }

  HT_INFOF("Successfully updated schema for: %s", table->id);
  cb->response_ok();
}

void
RangeServer::transform_key(ByteString &bskey, DynamicBuffer *dest_bufp,
                           int64_t auto_revision, int64_t *revisionp,
                           bool timeorder_desc)
{
  size_t len;
  const uint8_t *ptr;

  len = bskey.decode_length(&ptr);

  HT_ASSERT(*ptr == Key::AUTO_TIMESTAMP || *ptr == Key::HAVE_TIMESTAMP);

  // if TIME_ORDER DESC was set for this column then we store the timestamps
  // NOT in 1-complements!
  if (timeorder_desc) {
    // if the timestamp was specified by the user: unpack it and pack it
    // again w/o 1-complement
    if (*ptr == Key::HAVE_TIMESTAMP) {
      uint8_t *p=(uint8_t *)ptr+len-8;
      int64_t ts=Key::decode_ts64((const uint8_t **)&p);
      p=(uint8_t *)ptr+len-8;
      Key::encode_ts64((uint8_t **)&p, ts, false);
    }
  }

  dest_bufp->ensure((ptr-bskey.ptr) + len + 9);
  Serialization::encode_vi32(&dest_bufp->ptr, len+8);
  memcpy(dest_bufp->ptr, ptr, len);
  if (*ptr == Key::AUTO_TIMESTAMP)
    *dest_bufp->ptr = Key::HAVE_REVISION
        | Key::HAVE_TIMESTAMP | Key::REV_IS_TS;
  else
    *dest_bufp->ptr = Key::HAVE_REVISION
        | Key::HAVE_TIMESTAMP;

  // if TIME_ORDER DESC then store a flag in the key
  if (timeorder_desc)
    *dest_bufp->ptr |= Key::TS_CHRONOLOGICAL;

  dest_bufp->ptr += len;
  Key::encode_ts64(&dest_bufp->ptr, auto_revision,
          timeorder_desc ? false : true);
  *revisionp = auto_revision;
  bskey.ptr = ptr + len;
}

void
RangeServer::commit_log_sync(ResponseCallback *cb, 
                             const TableIdentifier *table) {
  String errmsg;
  int error = Error::OK;
  TableUpdate *table_update = new TableUpdate();
  StaticBuffer buffer(0);
  SchemaPtr schema;
  std::vector<TableUpdate *> table_update_vector;

  HT_DEBUG_OUT <<"received commit_log_sync request for table "<< table->id<< HT_END;

  if (!m_replay_finished) {
    if (!wait_for_recovery_finish(cb->get_event()->expiration_time()))
      return;
  }

  try {

    if (!m_live_map->lookup(table->id, table_update->table_info)) {
      if ((error = cb->error(Error::TABLE_NOT_FOUND, table->id)) != Error::OK)
        HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
      return;
    }

    schema = table_update->table_info->get_schema();

    // Check for group commit
    if (schema->get_group_commit_interval() > 0) {
      m_group_commit->add(cb->get_event(), schema, table, 0, buffer, 0);
      return;
    }

    // normal sync...
    UpdateRequest *request = new UpdateRequest();
    table_update->id = *table;
    table_update->commit_interval = 0;
    table_update->total_count = 0;
    table_update->total_buffer_size = 0;;
    table_update->flags = 0;
    table_update->sync = true;
    request->buffer = buffer;
    request->count = 0;
    request->event = cb->get_event();
    table_update->requests.push_back(request);

    table_update_vector.push_back(table_update);

    batch_update(table_update_vector, cb->get_event()->expiration_time());
  }
  catch (Exception &e) {
    HT_ERROR_OUT << "Exception caught: " << e << HT_END;
    error = e.code();
    errmsg = e.what();
    if ((error = cb->error(error, errmsg)) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}

/**
 * TODO:
 *  set commit_interval
 *
 */
void
RangeServer::update(ResponseCallbackUpdate *cb, const TableIdentifier *table,
                    uint32_t count, StaticBuffer &buffer, uint32_t flags) {
  SchemaPtr schema;
  TableUpdate *table_update = new TableUpdate();

  if (m_update_delay)
    poll(0, 0, m_update_delay);

  HT_DEBUG_OUT <<"Update: "<< *table << HT_END;

  if (!m_replay_finished) {
    if (table->is_metadata()) {
      if (!wait_for_root_recovery_finish(cb->get_event()->expiration_time()))
        return;
      table_update->wait_for_metadata_recovery = true;
    }
    else if (table->is_system()) {
      if (!wait_for_metadata_recovery_finish(cb->get_event()->expiration_time()))
        return;
      table_update->wait_for_system_recovery = true;
    }
    else {
      if (!wait_for_recovery_finish(cb->get_event()->expiration_time()))
        return;
    }
  }

  if (!m_live_map->lookup(table->id, table_update->table_info))
    HT_THROW(Error::TABLE_NOT_FOUND, table->id);

  schema = table_update->table_info->get_schema();

  // Check for group commit
  if (schema->get_group_commit_interval() > 0) {
    m_group_commit->add(cb->get_event(), schema, table, count, buffer, flags);
    delete table_update;
    return;
  }

  // normal update ...

  std::vector<TableUpdate *> table_update_vector;

  table_update->id = *table;
  table_update->commit_interval = 0;
  table_update->total_count = count;
  table_update->total_buffer_size = buffer.size;
  table_update->flags = flags;

  UpdateRequest *request = new UpdateRequest();
  request->buffer = buffer;
  request->count = count;
  request->event = cb->get_event();

  table_update->requests.push_back(request);

  table_update_vector.push_back(table_update);

  batch_update(table_update_vector, cb->get_event()->expiration_time());

}

void
RangeServer::batch_update(std::vector<TableUpdate *> &updates, boost::xtime expire_time) {
  UpdateContext *uc = new UpdateContext(updates, expire_time);

  // Enqueue update
  {
    ScopedLock lock(m_update_qualify_queue_mutex);
    HT_ASSERT(!updates.empty());
    if (m_profile_query)
      boost::xtime_get(&uc->start_time, TIME_UTC_);
    m_update_qualify_queue.push_back(uc);
    m_update_qualify_queue_cond.notify_all();
  }

}

namespace {

  struct ltRangePtr {
    bool operator()(const RangePtr &rp1, const RangePtr &rp2) const {
      return rp1.get() < rp2.get();
    }
  };

}

void RangeServer::update_qualify_and_transform() {
  UpdateContext *uc;
  SerializedKey key;
  const uint8_t *mod, *mod_end;
  const char *row;
  String start_row, end_row;
  RangeUpdateList *rulist;
  int error = Error::OK;
  int64_t latest_range_revision;
  RangeTransferInfo transfer_info;
  bool transfer_pending;
  DynamicBuffer *cur_bufp;
  DynamicBuffer *transfer_bufp;
  uint32_t go_buf_reset_offset;
  uint32_t root_buf_reset_offset;
  CommitLogPtr transfer_log;
  RangeUpdate range_update;
  RangePtr range;
  Mutex &mutex = m_update_qualify_queue_mutex;
  boost::condition &cond = m_update_qualify_queue_cond;
  std::list<UpdateContext *> &queue = m_update_qualify_queue;

  while (true) {

    {
      ScopedLock lock(mutex);
      while (queue.empty() && !m_shutdown)
        cond.wait(lock);
      if (m_shutdown)
        return;
      uc = queue.front();
      queue.pop_front();
    }

    rulist = 0;
    transfer_bufp = 0;
    go_buf_reset_offset = 0;
    root_buf_reset_offset = 0;

    // This probably shouldn't happen for group commit, but since
    // it's only for testing purposes, we'll leave it here
    if (m_update_delay)
      poll(0, 0, m_update_delay);

    // Global commit log is only available after local recovery
    uc->auto_revision = Hypertable::get_ts64();

    // TODO: Sanity check mod data (checksum validation)

    // hack to workaround xen timestamp issue
    if (uc->auto_revision < m_last_revision)
      uc->auto_revision = m_last_revision;

    foreach_ht (TableUpdate *table_update, uc->updates) {

      HT_DEBUG_OUT <<"Update: "<< table_update->id << HT_END;

      try {
        if (!m_live_map->lookup(table_update->id.id, table_update->table_info)) {
          table_update->error = Error::TABLE_NOT_FOUND;
          table_update->error_msg = table_update->id.id;
          continue;
        }
      }
      catch (Exception &e) {
        table_update->error = e.code();
        table_update->error_msg = e.what();
        continue;
      }

      // verify schema
      if (table_update->table_info->get_schema()->get_generation() !=
          table_update->id.generation) {
        table_update->error = Error::RANGESERVER_GENERATION_MISMATCH;
        table_update->error_msg =
          format("Update schema generation mismatch for table %s (received %u != %u)",
                 table_update->id.id, table_update->id.generation,
                 table_update->table_info->get_schema()->get_generation());
        continue;
      }

      // Pre-allocate the go_buf - each key could expand by 8 or 9 bytes,
      // if auto-assigned (8 for the ts or rev and maybe 1 for possible
      // increase in vint length)
      table_update->go_buf.reserve(table_update->id.encoded_length() +
                                   table_update->total_buffer_size +
                                   (table_update->total_count * 9));
      table_update->id.encode(&table_update->go_buf.ptr);
      table_update->go_buf.set_mark();

      foreach_ht (UpdateRequest *request, table_update->requests) {
        uc->total_updates++;

        mod_end = request->buffer.base + request->buffer.size;
        mod = request->buffer.base;

        go_buf_reset_offset = table_update->go_buf.fill();
        root_buf_reset_offset = uc->root_buf.fill();

        memset(&uc->send_back, 0, sizeof(uc->send_back));

        while (mod < mod_end) {
          key.ptr = mod;
          row = key.row();

          // error inducer for tests/integration/fail-index-mutator
          if (HT_FAILURE_SIGNALLED("fail-index-mutator-0")) {
            if (!strcmp(row, "1,+9RfmqoH62hPVvDTh6EC4zpTNfzNr8\t01918")) {
              uc->send_back.count++;
              uc->send_back.error = Error::INDUCED_FAILURE;
              uc->send_back.offset = mod - request->buffer.base;
              uc->send_back.len = strlen(row);
              request->send_back_vector.push_back(uc->send_back);
              memset(&uc->send_back, 0, sizeof(uc->send_back));
              key.next(); // skip key
              key.next(); // skip value;
              mod = key.ptr;
              continue;
            }
          }

          // If the row key starts with '\0' then the buffer is probably
          // corrupt, so mark the remaing key/value pairs as bad
          if (*row == 0) {
            uc->send_back.error = Error::BAD_KEY;
            uc->send_back.count = request->count;  // fix me !!!!
            uc->send_back.offset = mod - request->buffer.base;
            uc->send_back.len = mod_end - mod;
            request->send_back_vector.push_back(uc->send_back);
            memset(&uc->send_back, 0, sizeof(uc->send_back));
            mod = mod_end;
            continue;
          }

          // Look for containing range, add to stop mods if not found
          if (!table_update->table_info->find_containing_range(row, range,
                                                        start_row, end_row)) {
            if (uc->send_back.error != Error::RANGESERVER_OUT_OF_RANGE
                && uc->send_back.count > 0) {
              uc->send_back.len = (mod - request->buffer.base) - uc->send_back.offset;
              request->send_back_vector.push_back(uc->send_back);
              memset(&uc->send_back, 0, sizeof(uc->send_back));
            }
            if (uc->send_back.count == 0) {
              uc->send_back.error = Error::RANGESERVER_OUT_OF_RANGE;
              uc->send_back.offset = mod - request->buffer.base;
            }
            key.next(); // skip key
            key.next(); // skip value;
            mod = key.ptr;
            uc->send_back.count++;
            continue;
          }

          if ((rulist = table_update->range_map[range.get()]) == 0) {
            rulist = new RangeUpdateList();
            rulist->range = range;
            table_update->range_map[range.get()] = rulist;
          }

          if (table_update->wait_for_metadata_recovery && !rulist->range->is_root()) {
            if (!wait_for_metadata_recovery_finish(uc->expire_time)) {
              delete uc;
              return;
            }
            table_update->wait_for_metadata_recovery = false;
          }
          else if (table_update->wait_for_system_recovery) {
            if (!wait_for_system_recovery_finish(uc->expire_time)) {
              delete uc;
              return;
            }
            table_update->wait_for_system_recovery = false;
          }

          // See if range has some other error preventing it from receiving updates
          if ((error = rulist->range->get_error()) != Error::OK) {
            if (uc->send_back.error != error && uc->send_back.count > 0) {
              uc->send_back.len = (mod - request->buffer.base) - uc->send_back.offset;
              request->send_back_vector.push_back(uc->send_back);
              memset(&uc->send_back, 0, sizeof(uc->send_back));
            }
            if (uc->send_back.count == 0) {
              uc->send_back.error = error;
              uc->send_back.offset = mod - request->buffer.base;
            }
            key.next(); // skip key
            key.next(); // skip value;
            mod = key.ptr;
            uc->send_back.count++;
            continue;
          }

          if (uc->send_back.count > 0) {
            uc->send_back.len = (mod - request->buffer.base) - uc->send_back.offset;
            request->send_back_vector.push_back(uc->send_back);
            memset(&uc->send_back, 0, sizeof(uc->send_back));
          }

          /*
           *  Increment update count on range
           *  (block if maintenance in progress)
           */
          if (!rulist->range_blocked) {
            if (!rulist->range->increment_update_counter()) {
              uc->send_back.error = error;
              uc->send_back.offset = mod - request->buffer.base;
              uc->send_back.count++;
              key.next(); // skip key
              key.next(); // skip value;
              mod = key.ptr;
              continue;
            }
            rulist->range_blocked = true;
          }

          String range_start_row, range_end_row;
          rulist->range->get_boundary_rows(range_start_row, range_end_row);

          // Make sure range didn't just shrink
          if (range_start_row != start_row || range_end_row != end_row) {
            rulist->range->decrement_update_counter();
            table_update->range_map.erase(rulist->range.get());
            delete rulist;
            continue;
          }

          /** Fetch range transfer information **/
          {
            bool wait_for_maintenance;
            transfer_pending = rulist->range->get_transfer_info(transfer_info, transfer_log,
                                                                &latest_range_revision, wait_for_maintenance);
          }

          if (rulist->transfer_log.get() == 0)
            rulist->transfer_log = transfer_log;

          HT_ASSERT(rulist->transfer_log.get() == transfer_log.get());

          bool in_transferring_region = false;

          // Check for clock skew
          {
            ByteString tmp_key;
            const uint8_t *tmp;
            int64_t difference, tmp_timestamp;
            tmp_key.ptr = key.ptr;
            tmp_key.decode_length(&tmp);
            if ((*tmp & Key::HAVE_REVISION) == 0) {
              if (latest_range_revision > TIMESTAMP_MIN
                  && uc->auto_revision < latest_range_revision) {
                tmp_timestamp = Hypertable::get_ts64();
                if (tmp_timestamp > uc->auto_revision)
                  uc->auto_revision = tmp_timestamp;
                if (uc->auto_revision < latest_range_revision) {
                  difference = (int32_t)((latest_range_revision - uc->auto_revision)
                                         / 1000LL);
                  if (difference > m_max_clock_skew && !Global::ignore_clock_skew_errors) {
                    request->error = Error::RANGESERVER_CLOCK_SKEW;
                    HT_ERRORF("Clock skew of %lld microseconds exceeds maximum "
                              "(%lld) range=%s", (Lld)difference,
                              (Lld)m_max_clock_skew,
                              rulist->range->get_name().c_str());
                    uc->send_back.count = 0;
                    request->send_back_vector.clear();
                    break;
                  }
                }
              }
            }
          }

          if (transfer_pending) {
            transfer_bufp = &rulist->transfer_buf;
            if (transfer_bufp->empty()) {
              transfer_bufp->reserve(table_update->id.encoded_length());
              table_update->id.encode(&transfer_bufp->ptr);
              transfer_bufp->set_mark();
            }
            rulist->transfer_buf_reset_offset = rulist->transfer_buf.fill();
          }
          else {
            transfer_bufp = 0;
            rulist->transfer_buf_reset_offset = 0;
          }

          if (rulist->range->is_root()) {
            if (uc->root_buf.empty()) {
              uc->root_buf.reserve(table_update->id.encoded_length());
              table_update->id.encode(&uc->root_buf.ptr);
              uc->root_buf.set_mark();
              root_buf_reset_offset = uc->root_buf.fill();
            }
            cur_bufp = &uc->root_buf;
          }
          else
            cur_bufp = &table_update->go_buf;

          rulist->last_request = request;

          range_update.bufp = cur_bufp;
          range_update.offset = cur_bufp->fill();

          while (mod < mod_end &&
                 (end_row == "" || (strcmp(row, end_row.c_str()) <= 0))) {

            if (transfer_pending) {

              if (transfer_info.transferring(row)) {
                if (!in_transferring_region) {
                  range_update.len = cur_bufp->fill() - range_update.offset;
                  rulist->add_update(request, range_update);
                  cur_bufp = transfer_bufp;
                  range_update.bufp = cur_bufp;
                  range_update.offset = cur_bufp->fill();
                  in_transferring_region = true;
                }
                table_update->transfer_count++;
              }
              else {
                if (in_transferring_region) {
                  range_update.len = cur_bufp->fill() - range_update.offset;
                  rulist->add_update(request, range_update);
                  cur_bufp = &table_update->go_buf;
                  range_update.bufp = cur_bufp;
                  range_update.offset = cur_bufp->fill();
                  in_transferring_region = false;
                }
              }
            }

            try {
              SchemaPtr schema = table_update->table_info->get_schema();
              uint8_t family=*(key.ptr+1+strlen((const char *)key.ptr+1)+1);
              Schema::ColumnFamily *cf = schema->get_column_family(family);

              // reset auto_revision if it's gotten behind
              if (uc->auto_revision < latest_range_revision) {
                uc->auto_revision = Hypertable::get_ts64();
                if (uc->auto_revision < latest_range_revision) {
                  HT_THROWF(Error::RANGESERVER_REVISION_ORDER_ERROR,
                          "Auto revision (%lld) is less than latest range "
                          "revision (%lld) for range %s",
                          (Lld)uc->auto_revision, (Lld)latest_range_revision,
                          rulist->range->get_name().c_str());
                }
              }

              // This will transform keys that need to be assigned a
              // timestamp and/or revision number by re-writing the key
              // with the added timestamp and/or revision tacked on to the end
              transform_key(key, cur_bufp, ++uc->auto_revision,
                      &m_last_revision, cf ? cf->time_order_desc : false);

              // Validate revision number
              if (m_last_revision < latest_range_revision) {
                if (m_last_revision != uc->auto_revision) {
                  HT_THROWF(Error::RANGESERVER_REVISION_ORDER_ERROR,
                          "Supplied revision (%lld) is less than most recently "
                          "seen revision (%lld) for range %s",
                          (Lld)m_last_revision, (Lld)latest_range_revision,
                          rulist->range->get_name().c_str());
                }
              }
            }
            catch (Exception &e) {
              HT_ERRORF("%s - %s", e.what(), Error::get_text(e.code()));
              request->error = e.code();
              break;
            }

            // Now copy the value (with sanity check)
            mod = key.ptr;
            key.next(); // skip value
            HT_ASSERT(key.ptr <= mod_end);
            cur_bufp->add(mod, key.ptr-mod);
            mod = key.ptr;

            table_update->total_added++;

            if (mod < mod_end)
              row = key.row();
          }

          if (request->error == Error::OK) {

            range_update.len = cur_bufp->fill() - range_update.offset;
            rulist->add_update(request, range_update);

            // if there were transferring updates, record the latest revision
            if (transfer_pending && rulist->transfer_buf_reset_offset < rulist->transfer_buf.fill()) {
              if (rulist->latest_transfer_revision < m_last_revision)
                rulist->latest_transfer_revision = m_last_revision;
            }
          }
          else {
            /*
             * If we drop into here, this means that the request is
             * being aborted, so reset all of the RangeUpdateLists,
             * reset the go_buf and the root_buf
             */
            for (hash_map<Range *, RangeUpdateList *>::iterator iter = table_update->range_map.begin();
                 iter != table_update->range_map.end(); ++iter)
              (*iter).second->reset_updates(request);
            table_update->go_buf.ptr = table_update->go_buf.base + go_buf_reset_offset;
            if (root_buf_reset_offset)
              uc->root_buf.ptr = uc->root_buf.base + root_buf_reset_offset;
            uc->send_back.count = 0;
            mod = mod_end;
          }
          range_update.bufp = 0;
        }

        transfer_log = 0;

        if (uc->send_back.count > 0) {
          uc->send_back.len = (mod - request->buffer.base) - uc->send_back.offset;
          request->send_back_vector.push_back(uc->send_back);
          memset(&uc->send_back, 0, sizeof(uc->send_back));
        }
      }

      HT_DEBUGF("Added %d (%d transferring) updates to '%s'",
                table_update->total_added, table_update->transfer_count,
                table_update->id.id);
      if (!table_update->id.is_metadata())
        uc->total_added += table_update->total_added;
    }

    uc->last_revision = m_last_revision;

    // Enqueue update
    {
      ScopedLock lock(m_update_commit_queue_mutex);
      if (m_profile_query) {
        boost::xtime now;
        boost::xtime_get(&now, TIME_UTC_);
        uc->qualify_time = xtime_diff_millis(uc->start_time, now);
        uc->start_time = now;
      }
      m_update_commit_queue.push_back(uc);
      m_update_commit_queue_cond.notify_all();
      m_update_commit_queue_count++;
    }
  }
}

void RangeServer::update_commit() {
  UpdateContext *uc;
  SerializedKey key;
  std::list<UpdateContext *> coalesce_queue;
  uint64_t coalesce_amount = 0;
  int error = Error::OK;
  uint32_t committed_transfer_data;
  bool user_log_needs_syncing;

  while (true) {

    // Dequeue next update
    {
      ScopedLock lock(m_update_commit_queue_mutex);
      while (m_update_commit_queue.empty() && !m_shutdown)
        m_update_commit_queue_cond.wait(lock);
      if (m_shutdown)
        return;
      uc = m_update_commit_queue.front();
      m_update_commit_queue.pop_front();
      m_update_commit_queue_count--;
    }

    committed_transfer_data = 0;
    user_log_needs_syncing = false;

    /**
     * Commit ROOT mutations
     */
    if (uc->root_buf.ptr > uc->root_buf.mark) {
      if ((error = Global::root_log->write(uc->root_buf, uc->last_revision)) != Error::OK) {
        HT_FATALF("Problem writing %d bytes to ROOT commit log - %s",
                  (int)uc->root_buf.fill(), Error::get_text(error));
      }
    }

    foreach_ht (TableUpdate *table_update, uc->updates) {

      coalesce_amount += table_update->total_buffer_size;

      // Iterate through all of the ranges, committing any transferring updates
      for (hash_map<Range *, RangeUpdateList *>::iterator iter = table_update->range_map.begin(); iter != table_update->range_map.end(); ++iter) {
        if ((*iter).second->transfer_buf.ptr > (*iter).second->transfer_buf.mark) {
          committed_transfer_data += (*iter).second->transfer_buf.ptr - (*iter).second->transfer_buf.mark;
          if ((error = (*iter).second->transfer_log->write((*iter).second->transfer_buf, (*iter).second->latest_transfer_revision)) != Error::OK) {
            table_update->error = error;
            table_update->error_msg = format("Problem writing %d bytes to transfer log",
                                             (int)(*iter).second->transfer_buf.fill());
            HT_ERRORF("%s - %s", table_update->error_msg.c_str(), Error::get_text(error));
            break;
          }
        }
      }

      if (table_update->error != Error::OK)
        continue;

      /**
       * Commit valid (go) mutations
       */
      if (table_update->go_buf.ptr > table_update->go_buf.mark) {
        CommitLog *log = 0;

        bool sync = false;
        if (table_update->id.is_user()) {
          log = Global::user_log;
          if ((table_update->flags & RangeServerProtocol::UPDATE_FLAG_NO_LOG_SYNC) == 0)
            user_log_needs_syncing = true;
        }
        else if (table_update->id.is_metadata()) {
          sync = true;
          log = Global::metadata_log;
        }
        else {
          HT_ASSERT(table_update->id.is_system());
          sync = true;
          log = Global::system_log;
        }

        if ((error = log->write(table_update->go_buf, uc->last_revision, sync)) != Error::OK) {
          table_update->error_msg = format("Problem writing %d bytes to commit log (%s) - %s",
                                           (int)table_update->go_buf.fill(),
                                           log->get_log_dir().c_str(),
                                           Error::get_text(error));
          HT_ERRORF("%s", table_update->error_msg.c_str());
          table_update->error = error;
          continue;
        }
      }
      else if (table_update->sync)
        user_log_needs_syncing = true;

    }

    bool do_sync = false;
    if (user_log_needs_syncing) {
      if (m_update_commit_queue_count > 0 && coalesce_amount < m_update_coalesce_limit) {
        coalesce_queue.push_back(uc);
        continue;
      }
      do_sync = true;
    }
    else if (!coalesce_queue.empty())
      do_sync = true;

    // Now sync the USER commit log if needed
    if (do_sync) {
      size_t retry_count = 0;
      uc->total_syncs++;
      while ((error = Global::user_log->sync()) != Error::OK) {
        HT_ERRORF("Problem sync'ing user log fragment (%s) - %s",
                  Global::user_log->get_current_fragment_file().c_str(),
                  Error::get_text(error));
        if (++retry_count == 6)
          break;
        poll(0, 0, 10000);
      }
    }

    // Enqueue update
    {
      ScopedLock lock(m_update_response_queue_mutex);
      coalesce_queue.push_back(uc);
      while (!coalesce_queue.empty()) {
        uc = coalesce_queue.front();
        if (m_profile_query) {
          boost::xtime now;
          boost::xtime_get(&now, TIME_UTC_);
          uc->commit_time = xtime_diff_millis(uc->start_time, now);
          uc->start_time = now;
        }
        coalesce_queue.pop_front();
        m_update_response_queue.push_back(uc);
      }
      coalesce_amount = 0;
      m_update_response_queue_cond.notify_all();
    }
  }
}

void RangeServer::update_add_and_respond() {
  UpdateContext *uc;
  SerializedKey key;
  int error = Error::OK;

  while (true) {

    // Dequeue next update
    {
      ScopedLock lock(m_update_response_queue_mutex);
      while (m_update_response_queue.empty() && !m_shutdown)
        m_update_response_queue_cond.wait(lock);
      if (m_shutdown)
        return;
      uc = m_update_response_queue.front();
      m_update_response_queue.pop_front();
    }

    /**
     *  Insert updates into Ranges
     */
    foreach_ht (TableUpdate *table_update, uc->updates) {

      // Iterate through all of the ranges, inserting updates
      for (hash_map<Range *, RangeUpdateList *>::iterator iter = table_update->range_map.begin(); iter != table_update->range_map.end(); ++iter) {
        ByteString value;
        Key key_comps;

        foreach_ht (RangeUpdate &update, (*iter).second->updates) {
          Range *rangep = (*iter).first;
          Locker<Range> lock(*rangep);
          uint8_t *ptr = update.bufp->base + update.offset;
          uint8_t *end = ptr + update.len;

          if (!table_update->id.is_metadata())
            uc->total_bytes_added += update.len;

          rangep->add_bytes_written( update.len );
          const char *last_row = "";
          uint64_t count = 0;
          while (ptr < end) {
            key.ptr = ptr;
            key_comps.load(key);
            count++;
            if (key_comps.column_family_code == 0 && key_comps.flag != FLAG_DELETE_ROW) {
              HT_ERRORF("Skipping bad key - column family not specified in non-delete row update on %s row=%s",
                        table_update->id.id, key_comps.row);
            }
            ptr += key_comps.length;
            value.ptr = ptr;
            ptr += value.length();
            rangep->add(key_comps, value);
            // invalidate
            if (m_query_cache && strcmp(last_row, key_comps.row))
              m_query_cache->invalidate(table_update->id.id, key_comps.row);
            last_row = key_comps.row;
          }
          rangep->add_cells_written(count);
        }
      }
    }

    /**
     * Decrement usage counters for all referenced ranges
     */
    foreach_ht (TableUpdate *table_update, uc->updates) {
      for (hash_map<Range *, RangeUpdateList *>::iterator iter = table_update->range_map.begin(); iter != table_update->range_map.end(); ++iter) {
        if ((*iter).second->range_blocked)
          (*iter).first->decrement_update_counter();
      }
    }

    /**
     * wait for these ranges to complete maintenance
     */
    bool maintenance_needed = false;
    foreach_ht (TableUpdate *table_update, uc->updates) {

      /**
       * If any of the newly updated ranges needs maintenance,
       * schedule immediately
       */
      for (hash_map<Range *, RangeUpdateList *>::iterator iter = table_update->range_map.begin(); iter != table_update->range_map.end(); ++iter) {
        if ((*iter).first->need_maintenance() &&
            !Global::maintenance_queue->contains((*iter).first)) {
          ScopedLock lock(m_mutex);
          maintenance_needed = true;
          HT_MAYBE_FAIL_X("metadata-update-and-respond", (*iter).first->is_metadata());
          if (m_timer_handler)
            m_timer_handler->schedule_immediate_maintenance();
          break;
        }
      }

      foreach_ht (UpdateRequest *request, table_update->requests) {
        ResponseCallbackUpdate cb(m_comm, request->event);

        if (table_update->error != Error::OK) {
          if ((error = cb.error(table_update->error, table_update->error_msg)) != Error::OK)
            HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
          continue;
        }

        if (request->error == Error::OK) {
          /**
           * Send back response
           */
          if (!request->send_back_vector.empty()) {
            StaticBuffer ext(new uint8_t [request->send_back_vector.size() * 16],
                             request->send_back_vector.size() * 16);
            uint8_t *ptr = ext.base;
            for (size_t i=0; i<request->send_back_vector.size(); i++) {
              encode_i32(&ptr, request->send_back_vector[i].error);
              encode_i32(&ptr, request->send_back_vector[i].count);
              encode_i32(&ptr, request->send_back_vector[i].offset);
              encode_i32(&ptr, request->send_back_vector[i].len);
              /*
                HT_INFOF("Sending back error %x, count %d, offset %d, len %d, table id %s",
                request->send_back_vector[i].error, request->send_back_vector[i].count,
                request->send_back_vector[i].offset, request->send_back_vector[i].len,
                table_update->id.id);
              */
            }
            if ((error = cb.response(ext)) != Error::OK)
              HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
          }
          else {
            if ((error = cb.response_ok()) != Error::OK)
              HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
          }
        }
        else {
          if ((error = cb.error(request->error, "")) != Error::OK)
            HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
        }
      }

    }

    {
      Locker<LoadStatistics> lock(*Global::load_statistics);
      Global::load_statistics->add_update_data(uc->total_updates, uc->total_added, uc->total_bytes_added, uc->total_syncs);
    }

    if (m_profile_query) {
      ScopedLock lock(m_profile_mutex);
      boost::xtime now;
      boost::xtime_get(&now, TIME_UTC_);
      uc->add_time = xtime_diff_millis(uc->start_time, now);
      m_profile_query_out << now.sec << "\tupdate\t" << uc->qualify_time << "\t" << uc->commit_time << "\t" << uc->add_time << "\n";
    }

    delete uc;

    // For testing
    if (m_maintenance_pause_interval > 0 && maintenance_needed)
      poll(0, 0, m_maintenance_pause_interval);

  }

}

void
RangeServer::drop_table(ResponseCallback *cb, const TableIdentifier *table) {
  TableInfoPtr table_info;
  Ranges ranges;
  String metadata_prefix;
  String metadata_key;
  TableMutatorPtr mutator;

  HT_INFOF("drop table %s", table->id);

  if (table->is_system()) {
    cb->error(Error::NOT_ALLOWED, "system tables cannot be dropped");
    return;
  }

  if (!m_replay_finished) {
    if (!wait_for_recovery_finish(cb->get_event()->expiration_time()))
      return;
  }

  {
    ScopedLock lock(m_drop_table_mutex);

    m_dropped_table_id_cache->insert(table->id);

    if (!m_live_map->remove(table->id, table_info)) {
      HT_WARNF("drop_table '%s' - table not found", table->id);
      cb->error(Error::TABLE_NOT_FOUND, table->id);
      return;
    }
  }

  // Set "drop" bit on all ranges
  ranges.array.clear();
  table_info->get_ranges(ranges);
  foreach_ht(RangeData &rd, ranges.array)
    rd.range->drop();

  // Disable maintenance for range, remove the range's original transfer
  // log, and remove the range from the RSML
  foreach_ht(RangeData &rd, ranges.array) {
    rd.range->disable_maintenance();
    try {
      rd.range->remove_original_transfer_log();
      Global::rsml_writer->record_removal(rd.range->metalog_entity());
    }
    catch (Exception &e) {
      cb->error(e.code(), Global::location_initializer->get());
      return;
    }
  }

  SchemaPtr schema = table_info->get_schema();
  Schema::AccessGroups &ags = schema->get_access_groups();

  // create METADATA table mutator for clearing 'Location' columns
  mutator = Global::metadata_table->create_mutator();

  KeySpec key;

  try {
    // For each range in dropped table, Set the 'drop' bit and clear
    // the 'Location' column of the corresponding METADATA entry
    metadata_prefix = String("") + table->id + ":";
    foreach_ht (RangeData &rd, ranges.array) {
      // Mark Location column
      metadata_key = metadata_prefix + rd.range->end_row();
      key.row = metadata_key.c_str();
      key.row_len = metadata_key.length();
      key.column_family = "Location";
      mutator->set(key, "!", 1);
      for (size_t j=0; j<ags.size(); j++) {
        key.column_family = "Files";
        key.column_qualifier = ags[j]->name.c_str();
        key.column_qualifier_len = ags[j]->name.length();
        mutator->set(key, (uint8_t *)"!", 1);
      }
    }
    mutator->flush();
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << "Problem clearing 'Location' columns of METADATA - " << e << HT_END;
    cb->error(e.code(), "Problem clearing 'Location' columns of METADATA");
    return;
  }

  HT_INFOF("Successfully dropped table '%s'", table->id);

  cb->response_ok();
}

void RangeServer::dump(ResponseCallback *cb, const char *outfile,
                       bool nokeys) {
  Ranges ranges;
  AccessGroup::MaintenanceData *ag_data;
  String str;

  HT_INFO("dump");

  try {
    std::ofstream out(outfile);

    m_live_map->get_ranges(ranges);
    time_t now = time(0);
    foreach_ht (RangeData &rd, ranges.array) {
      rd.data = rd.range->get_maintenance_data(ranges.arena, now);
      out << "RANGE " << rd.range->get_name() << "\n";
      out << *rd.data << "\n";
      for (ag_data = rd.data->agdata; ag_data; ag_data = ag_data->next)
        out << *ag_data << "\n";
    }

    // dump keys
    if (!nokeys) {
      foreach_ht (RangeData &rd, ranges.array)
        for (ag_data = rd.data->agdata; ag_data; ag_data = ag_data->next)
          ag_data->ag->dump_keys(out);
    }

    out << "\nCommit Log Info\n";
    str = "";

    {
      ScopedLock lock(m_drop_table_mutex);
      if (Global::root_log)
        Global::root_log->get_stats("ROOT", str);

      if (Global::metadata_log)
        Global::metadata_log->get_stats("METADATA", str);

      if (Global::system_log)
        Global::system_log->get_stats("SYSTEM", str);

      if (Global::user_log)
        Global::user_log->get_stats("USER", str);
    }
    out << str;

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), "Problem executing dump() command");
    return;
  }
  catch (std::exception &e) {
    cb->error(Error::LOCAL_IO_ERROR, e.what());
    return;
  }
  cb->response_ok();
}

void
RangeServer::dump_pseudo_table(ResponseCallback *cb, const TableIdentifier *table,
                               const char *pseudo_table, const char *outfile) {

  HT_INFOF("dump_psudo_table ID=%s pseudo-table=%s outfile=%s", table->id, pseudo_table, outfile);

  if (!m_replay_finished) {
    if (!RangeServer::wait_for_recovery_finish(cb->get_event()->expiration_time()))
      return;
  }

  try {
    Ranges ranges;
    TableInfoPtr table_info;
    CellListScanner *scanner;
    ScanContextPtr scan_ctx = new ScanContext();
    Key key;
    ByteString value;
    Schema::ColumnFamily *cf;
    const uint8_t *ptr;
    size_t len;

    std::ofstream out(outfile);

    if (!m_live_map->lookup(table->id, table_info)) {
      cb->error(Error::TABLE_NOT_FOUND, table->id);
      return;
    }

    scan_ctx->timeout_ms = cb->get_event()->header.timeout_ms;

    table_info->get_ranges(ranges);
    foreach_ht(RangeData &rd, ranges.array) {
      scanner = rd.range->create_scanner_pseudo_table(scan_ctx, pseudo_table);
      while (scanner->get(key, value)) {
        cf = Global::pseudo_tables->cellstore_index->get_column_family(key.column_family_code);
        if (cf == 0) {
          HT_ERRORF("Column family code %d not found in %s pseudo table schema",
                    key.column_family_code, pseudo_table);
        }
        else {
          out << key.row << "\t" << cf->name;
          if (key.column_qualifier)
            out << ":" << key.column_qualifier;
          out << "\t";
          ptr = value.ptr;
          len = Serialization::decode_vi32(&ptr);
          out.write((const char *)ptr, len);
          out << "\n";
        }
        scanner->forward();
      }
      delete scanner;
    }

    out << std::flush;

    cb->response_ok();

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), e.what());
  }
  catch (std::exception &e) {
    cb->error(Error::LOCAL_IO_ERROR, e.what());
  }

}

void RangeServer::heapcheck(ResponseCallback *cb, const char *outfile) {

  HT_INFO("heapcheck");

#if defined(TCMALLOC)
  HeapLeakChecker::NoGlobalLeaks();
  if (outfile && *outfile) {
    std::ofstream out(outfile);
    char buf[4096];
    MallocExtension::instance()->GetStats(buf, 4096);
    out << buf << std::endl;
  }
  if (IsHeapProfilerRunning())
    HeapProfilerDump("heapcheck");
#else
  HT_WARN("heapcheck not defined for current allocator");
#endif
  cb->response_ok();
}

void RangeServer::get_statistics(ResponseCallbackGetStatistics *cb) {
  ScopedLock lock(m_stats_mutex);
  RangesPtr ranges = Global::get_ranges();
  int64_t timestamp = Hypertable::get_ts64();
  time_t now = (time_t)(timestamp/1000000000LL);
  LoadStatistics::Bundle load_stats;

  HT_INFO("Entering get_statistics()");

  if (m_shutdown) {
    cb->error(Error::SERVER_SHUTTING_DOWN, "");
    return;
  }

  Global::load_statistics->recompute(&load_stats);
  m_stats->system.refresh();

  uint64_t disk_total = 0;
  uint64_t disk_avail = 0;
  foreach_ht (struct FsStat &fss, m_stats->system.fs_stat) {
    disk_total += fss.total;
    disk_avail += fss.avail;
  }

  m_loadavg_accum += m_stats->system.loadavg_stat.loadavg[0];
  m_page_in_accum += m_stats->system.swap_stat.page_in;
  m_page_out_accum += m_stats->system.swap_stat.page_out;
  m_load_factors.bytes_scanned += load_stats.scan_bytes;
  m_load_factors.bytes_written += load_stats.update_bytes;
  m_metric_samples++;

  m_stats->set_location(Global::location_initializer->get());
  m_stats->set_version(version_string());
  m_stats->timestamp = timestamp;
  m_stats->scan_count = load_stats.scan_count;
  m_stats->scanned_cells = load_stats.scan_cells;
  m_stats->scanned_bytes = load_stats.scan_bytes;
  m_stats->update_count = load_stats.update_count;
  m_stats->updated_cells = load_stats.update_cells;
  m_stats->updated_bytes = load_stats.update_bytes;
  m_stats->sync_count = load_stats.sync_count;
  m_stats->tracked_memory = Global::memory_tracker->balance();
  m_stats->cpu_user = m_stats->system.cpu_stat.user;
  m_stats->cpu_sys = m_stats->system.cpu_stat.sys;
  m_stats->live = m_replay_finished;

  if (m_query_cache)
    m_query_cache->get_stats(&m_stats->query_cache_max_memory,
                             &m_stats->query_cache_available_memory,
                             &m_stats->query_cache_accesses,
                             &m_stats->query_cache_hits);

  if (Global::block_cache) {
    Global::block_cache->get_stats(&m_stats->block_cache_max_memory,
                                   &m_stats->block_cache_available_memory,
                                   &m_stats->block_cache_accesses,
                                   &m_stats->block_cache_hits);
  }
  else {
    m_stats->block_cache_max_memory = 0;
    m_stats->block_cache_available_memory = 0;
    m_stats->block_cache_accesses = 0;
    m_stats->block_cache_hits = 0;
  }


  TableMutatorPtr mutator;
  if (now > m_next_metrics_update) {
    if (!Global::rs_metrics_table) {
      ScopedLock lock(Global::mutex);
      try {
        uint32_t timeout_ms = m_props->get_i32("Hypertable.Request.Timeout");
        if (!Global::range_locator)
          Global::range_locator = new Hypertable::RangeLocator(m_props, m_conn_manager,
                                                               Global::hyperspace, timeout_ms);
        ApplicationQueueInterfacePtr aq = m_app_queue;
        Global::rs_metrics_table = new Table(m_props, Global::range_locator, m_conn_manager,
                                             Global::hyperspace, aq, m_namemap,
                                             "sys/RS_METRICS", 0, timeout_ms);
      }
      catch (Hypertable::Exception &e) {
        HT_ERRORF("Unable to open 'sys/RS_METRICS' - %s (%s)",
                  Error::get_text(e.code()), e.what());
      }
    }
    if (Global::rs_metrics_table) {
      CellsBuilder *pending_metrics_updates = 0;
      mutator = Global::rs_metrics_table->create_mutator();

      {
        ScopedLock lock(m_pending_metrics_mutex);
        pending_metrics_updates = m_pending_metrics_updates;
        m_pending_metrics_updates = 0;
      }

      if (pending_metrics_updates) {
        KeySpec key;
        Cells &cells = pending_metrics_updates->get();
        for (size_t i=0; i<cells.size(); i++) {
          key.row = cells[i].row_key;
          key.row_len = strlen(cells[i].row_key);
          key.column_family = cells[i].column_family;
          key.column_qualifier = cells[i].column_qualifier;
          key.column_qualifier_len = strlen(cells[i].column_qualifier);
          mutator->set(key, cells[i].value, cells[i].value_len);
        }
        delete pending_metrics_updates;
      }
    }
  }

  /**
   * Aggregate per-table stats
   */
  CstrToInt32Map table_scanner_count_map;
  StatsTable table_stat;
  StatsTableMap::iterator iter;

  m_stats->tables.clear();

  if (mutator || !ranges) {
    ranges = new Ranges();
    m_live_map->get_ranges(*ranges);
  }
  foreach_ht (RangeData &rd, ranges->array) {

    if (rd.data == 0)
      rd.data = rd.range->get_maintenance_data(ranges->arena, now, mutator.get());

    if (rd.data->table_id == 0) {
      HT_ERROR_OUT << "Range statistics object found without table ID" << HT_END;
      continue;
    }

    if (table_stat.table_id == "")
      table_stat.table_id = rd.data->table_id;
    else if (strcmp(table_stat.table_id.c_str(), rd.data->table_id)) {
      if (table_stat.disk_used > 0)
        table_stat.compression_ratio = (double)table_stat.disk_used / table_stat.compression_ratio;
      else
        table_stat.compression_ratio = 1.0;
      m_stats->tables.push_back(table_stat);
      table_scanner_count_map[table_stat.table_id.c_str()] = 0;
      table_stat.clear();
      table_stat.table_id = rd.data->table_id;
    }

    table_stat.scans += rd.data->load_factors.scans;
    m_load_factors.scans += rd.data->load_factors.scans;
    table_stat.updates += rd.data->load_factors.updates;
    m_load_factors.updates += rd.data->load_factors.updates;
    table_stat.cells_scanned += rd.data->load_factors.cells_scanned;
    m_load_factors.cells_scanned += rd.data->load_factors.cells_scanned;
    table_stat.cells_returned += rd.data->cells_returned;
    table_stat.cells_written += rd.data->load_factors.cells_written;
    m_load_factors.cells_written += rd.data->load_factors.cells_written;
    table_stat.bytes_scanned += rd.data->load_factors.bytes_scanned;
    table_stat.bytes_returned += rd.data->bytes_returned;
    table_stat.bytes_written += rd.data->load_factors.bytes_written;
    table_stat.disk_bytes_read += rd.data->load_factors.disk_bytes_read;
    m_load_factors.disk_bytes_read += rd.data->load_factors.disk_bytes_read;
    table_stat.disk_bytes_read += rd.data->load_factors.disk_bytes_read;
    table_stat.disk_used += rd.data->disk_used;
    table_stat.key_bytes += rd.data->key_bytes;
    table_stat.value_bytes += rd.data->value_bytes;
    table_stat.compression_ratio += (double)rd.data->disk_used / rd.data->compression_ratio;
    table_stat.memory_used += rd.data->memory_used;
    table_stat.memory_allocated += rd.data->memory_allocated;
    table_stat.shadow_cache_memory += rd.data->shadow_cache_memory;
    table_stat.block_index_memory += rd.data->block_index_memory;
    table_stat.bloom_filter_memory += rd.data->bloom_filter_memory;
    table_stat.bloom_filter_accesses += rd.data->bloom_filter_accesses;
    table_stat.bloom_filter_maybes += rd.data->bloom_filter_maybes;
    table_stat.cell_count += rd.data->cell_count;
    table_stat.file_count += rd.data->file_count;
    table_stat.range_count++;
  }

  m_stats->range_count = ranges->array.size();
  if (table_stat.table_id != "") {
    if (table_stat.disk_used > 0)
      table_stat.compression_ratio = (double)table_stat.disk_used / table_stat.compression_ratio;
    else
      table_stat.compression_ratio = 1.0;
    m_stats->tables.push_back(table_stat);
    table_scanner_count_map[table_stat.table_id.c_str()] = 0;
  }

  // collect outstanding scanner count and compute server cellstore total
  m_stats->file_count = 0;
  Global::scanner_map.get_counts(&m_stats->scanner_count, table_scanner_count_map);
  for (size_t i=0; i<m_stats->tables.size(); i++) {
    m_stats->tables[i].scanner_count = table_scanner_count_map[m_stats->tables[i].table_id.c_str()];
    m_stats->file_count += m_stats->tables[i].file_count;
  }

  if (m_query_cache) {
    m_query_cache->get_stats(&m_stats->query_cache_max_memory,
                             &m_stats->query_cache_available_memory,
                             &m_stats->query_cache_accesses,
                             &m_stats->query_cache_hits);
  }
  else {
    m_stats->query_cache_max_memory = 0;
    m_stats->query_cache_available_memory = 0;
    m_stats->query_cache_accesses = 0;
    m_stats->query_cache_hits = 0;
  }

  if (Global::block_cache) {
    Global::block_cache->get_stats(&m_stats->block_cache_max_memory,
                                   &m_stats->block_cache_available_memory,
                                   &m_stats->block_cache_accesses,
                                   &m_stats->block_cache_hits);
  }
  else {
    m_stats->block_cache_max_memory = 0;
    m_stats->block_cache_available_memory = 0;
    m_stats->block_cache_accesses = 0;
    m_stats->block_cache_hits = 0;
  }

  /**
   * If created a mutator above, write data to sys/RS_METRICS
   */
  if (mutator) {
    time_t rounded_time = (now+(Global::metrics_interval/2)) - ((now+(Global::metrics_interval/2))%Global::metrics_interval);
    if (m_last_metrics_update != 0) {
      double time_interval = (double)now - (double)m_last_metrics_update;
      String value = format("3:%ld,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f:%lld:%lld",
                            rounded_time,
                            m_loadavg_accum / (double)(m_metric_samples * m_cores),
                            (double)m_load_factors.disk_bytes_read / time_interval,
                            (double)m_load_factors.bytes_written / time_interval,
                            (double)m_load_factors.bytes_scanned / time_interval,
                            (double)m_load_factors.updates / time_interval,
                            (double)m_load_factors.scans / time_interval,
                            (double)m_load_factors.cells_written / time_interval,
                            (double)m_load_factors.cells_scanned / time_interval,
                            (double)m_page_in_accum / (double)m_metric_samples,
                            (double)m_page_out_accum / (double)m_metric_samples,
                            (Lld)disk_total, (Lld)disk_avail);
      String location = Global::location_initializer->get();
      KeySpec key;
      key.row = location.c_str();
      key.row_len = location.length();
      key.column_family = "server";
      key.column_qualifier = 0;
      key.column_qualifier_len = 0;
      try {
        mutator->set(key, (uint8_t *)value.c_str(), value.length());
        mutator->flush();
      }
      catch (Exception &e) {
        HT_ERROR_OUT << "Problem updating sys/RS_METRICS - " << e << HT_END;
      }
    }
    m_next_metrics_update += Global::metrics_interval;
    m_last_metrics_update = now;
    m_loadavg_accum = 0.0;
    m_page_in_accum = 0;
    m_page_out_accum = 0;
    m_load_factors.reset();
    m_metric_samples = 0;
  }

  {
    StaticBuffer ext(m_stats->encoded_length());
    uint8_t *ptr = ext.base;
    m_stats->encode(&ptr);
    HT_ASSERT((ptr-ext.base) == (ptrdiff_t)ext.size);
    cb->response(ext);
  }

  HT_INFO("Exiting get_statistics()");

  return;
}


void
RangeServer::replay_update(TableInfoMap &replay_map,
                           const uint8_t *data, size_t len) {
  TableIdentifier table_identifier;
  TableInfoPtr table_info;
  SerializedKey serkey;
  ByteString bsvalue;
  Key key;
  const uint8_t *ptr = data;
  const uint8_t *end = data + len;
  const uint8_t *block_end;
  uint32_t block_size;
  size_t remaining = len;
  const char *row;
  String err_msg;
  int64_t revision;
  RangePtr range;
  String start_row, end_row;

  //HT_DEBUGF("replay_update - length=%ld", len);

  try {

    while (ptr < end) {

      // decode key/value block size + revision
      block_size = decode_i32(&ptr, &remaining);
      revision = decode_i64(&ptr, &remaining);

      // decode table identifier
      table_identifier.decode(&ptr, &remaining);

      if (block_size > remaining)
        HT_THROWF(Error::MALFORMED_REQUEST, "Block (size=%lu) exceeds EOM",
                  (Lu)block_size);

      block_end = ptr + block_size;

      // Fetch table info
      if (!replay_map.lookup(table_identifier.id, table_info))
        HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND, "Unable to find "
                  "table info for table name='%s'",
                  table_identifier.id);

      while (ptr < block_end) {

        row = SerializedKey(ptr).row();

        // Look for containing range, add to stop mods if not found
        if (!table_info->find_containing_range(row, range, start_row, end_row))
          HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND, "Unable to find "
                    "range for row '%s'", row);

        serkey.ptr = ptr;

        while (ptr < block_end && (end_row.empty() || (strcmp(row, end_row.c_str()) <= 0))) {

          // extract the key
          ptr += serkey.length();

          if (ptr > end)
            HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding key");

          bsvalue.ptr = ptr;
          ptr += bsvalue.length();

          if (ptr > end)
            HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding value");

          key.load(serkey);

          {
            Locker<Range> lock(*range);
            range->add(key, bsvalue);
          }
          serkey.ptr = ptr;

          if (ptr < block_end)
            row = serkey.row();
        }
      }
    }

  }
  catch (Exception &e) {
    if (e.code() == Error::RANGESERVER_RANGE_NOT_FOUND)
      HT_INFOF("Range not found - %s", e.what());
    else
      HT_ERROR_OUT << e << HT_END;
    HT_THROW(e.code(), e.what());
  }
}


void
RangeServer::drop_range(ResponseCallback *cb, const TableIdentifier *table,
        const RangeSpec *range_spec) {
  TableInfoPtr table_info;
  RangePtr range;
  std::stringstream sout;

  sout << "drop_range\n"<< *table << *range_spec;
  HT_INFOF("%s", sout.str().c_str());

  if (!m_replay_finished) {
    if (table->is_metadata()) {
      if (!wait_for_metadata_recovery_finish(cb->get_event()->expiration_time()))
        return;
    }
    else {
      if (!wait_for_recovery_finish(cb->get_event()->expiration_time()))
        return;
    }
  }

  try {

    if (!m_live_map->lookup(table->id, table_info))
      HT_THROWF(Error::TABLE_NOT_FOUND, "%s", table->id);

    /** Remove the range **/
    if (!table_info->remove_range(range_spec, range))
      HT_THROW(Error::RANGESERVER_RANGE_NOT_FOUND,
               format("%s[%s..%s]", table->id, range_spec->start_row, range_spec->end_row));

    cb->response_ok();
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    int error = 0;
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}

void
RangeServer::relinquish_range(ResponseCallback *cb,
        const TableIdentifier *table, const RangeSpec *range_spec) {
  TableInfoPtr table_info;
  RangePtr range;
  std::stringstream sout;

  sout << "relinquish_range\n" << *table << *range_spec;
  HT_INFOF("%s", sout.str().c_str());

  if (!m_replay_finished) {
    if (table->is_metadata()) {
      if (!wait_for_metadata_recovery_finish(cb->get_event()->expiration_time()))
        return;
    }
    else {
      if (!wait_for_recovery_finish(cb->get_event()->expiration_time()))
        return;
    }
  }

  try {
    if (!m_live_map->lookup(table->id, table_info)) {
      cb->error(Error::TABLE_NOT_FOUND, table->id);
      return;
    }

    if (!table_info->get_range(range_spec, range))
      HT_THROW(Error::RANGESERVER_RANGE_NOT_FOUND,
              format("%s[%s..%s]", table->id, range_spec->start_row,
                  range_spec->end_row));

    range->schedule_relinquish();

    // Wake up maintenance scheduler
    {
      ScopedLock lock(m_mutex);
      if (m_timer_handler)
        m_timer_handler->schedule_immediate_maintenance();
    }

    cb->response_ok();
  }
  catch (Hypertable::Exception &e) {
    int error = 0;
    HT_INFOF("%s - %s", Error::get_text(e.code()), e.what());
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}

void RangeServer::replay_fragments(ResponseCallback *cb, int64_t op_id,
        const String &location, int plan_generation, 
        int type, const vector<uint32_t> &fragments,
        RangeRecoveryReceiverPlan &receiver_plan,
        uint32_t replay_timeout) {
  Timer timer(Global::failover_timeout/2, true);

  HT_INFOF("replay_fragments location=%s, plan_generation=%d, num_fragments=%d",
           location.c_str(), plan_generation, (int)fragments.size());

  CommitLogReaderPtr log_reader;
  String log_dir = Global::toplevel_dir + "/servers/" + location + "/log/" +
      RangeSpec::type_str(type);

  if (!m_replay_finished) {
    if (!wait_for_recovery_finish(cb->get_event()->expiration_time()))
      return;
  }

  HT_INFOF("replay_fragments(id=%lld, %s, plan_generation=%d, num_fragments=%d)",
           (Lld)op_id, location.c_str(), plan_generation, (int)fragments.size());

  cb->response_ok();

  try {
    log_reader = new CommitLogReader(Global::log_dfs, log_dir, fragments);
    StringSet receivers;
    receiver_plan.get_locations(receivers);
    CommAddress addr;
    uint32_t timeout_ms = m_props->get_i32("Hypertable.Request.Timeout");
    Timer timer(replay_timeout, true);
    foreach_ht(const String &receiver, receivers) {
      addr.set_proxy(receiver);
      m_conn_manager->add(addr, timeout_ms, "RangeServer");
      if (!m_conn_manager->wait_for_connection(addr, timer.remaining())) {
        if (timer.expired())
          HT_THROWF(Error::REQUEST_TIMEOUT, "Problem connecting to %s", receiver.c_str());
      }
    }

    BlockCompressionHeaderCommitLog header;
    uint8_t *base;
    size_t len;
    TableIdentifier table_id;
    const uint8_t *ptr, *end;
    SerializedKey key;
    ByteString value;
    uint32_t block_count = 0;
    uint32_t fragment_id;
    uint32_t last_fragment_id = 0;
    bool started = false;
    ReplayBuffer replay_buffer(m_props, m_comm, receiver_plan, location, plan_generation);
    size_t num_kv_pairs=0;

    try {

      while (log_reader->next((const uint8_t **)&base, &len, &header)) {
        fragment_id = log_reader->last_fragment_id();
        if (!started) {
          started = true;
          last_fragment_id = fragment_id;
          replay_buffer.set_current_fragment(fragment_id);
        }
        else if (fragment_id != last_fragment_id) {
          replay_buffer.flush();
          last_fragment_id = fragment_id;
          replay_buffer.set_current_fragment(fragment_id);
        }

        ptr = base;
        end = base + len;

        table_id.decode(&ptr, &len);

        num_kv_pairs = 0;
        while (ptr < end) {
          // extract the key
          key.ptr = ptr;
          ptr += key.length();
          if (ptr > end)
            HT_THROW(Error::RANGESERVER_CORRUPT_COMMIT_LOG, "Problem decoding key");
          // extract the value
          value.ptr = ptr;
          ptr += value.length();
          if (ptr > end)
            HT_THROW(Error::RANGESERVER_CORRUPT_COMMIT_LOG, "Problem decoding value");
          ++num_kv_pairs;
          replay_buffer.add(table_id, key, value);
        }
        HT_INFOF("Replayed %d key/value pairs from fragment %s",
                 (int)num_kv_pairs, log_reader->last_fragment_fname().c_str());
        block_count++;

        // report back status
        if (timer.expired()) {
          try {
            m_master_client->replay_status(op_id, location, plan_generation);
          }
          catch (Exception &ee) {
            HT_ERROR_OUT << ee << HT_END;
          }
          timer.reset(true);
        }
      }

      HT_MAYBE_FAIL_X("replay-fragments-user-0", type==RangeSpec::USER);

    }
    catch (Exception &e){
      HT_ERROR_OUT << log_reader->last_fragment_fname() << ": " << e << HT_END;
      HT_THROWF(e.code(), "%s: %s", log_reader->last_fragment_fname().c_str(), e.what());
    }

    replay_buffer.flush();

    HT_MAYBE_FAIL_X("replay-fragments-user-1", type==RangeSpec::USER);

    HT_INFOF("Finished playing %d fragments from %s",
             (int)fragments.size(), log_dir.c_str());

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    try {
      m_master_client->replay_complete(op_id, location, plan_generation,
                                       e.code(), e.what());
    }
    catch (Exception &ee) {
      HT_ERROR_OUT << ee << HT_END;
    }
    return;
  }

  try {
    m_master_client->replay_complete(op_id, location, plan_generation,
                                     Error::OK, "");
  }
  catch (Exception &e){
    HT_ERROR_OUT << "Unable to call player_complete on master for op_id="
        << op_id << ", type=" << type << ", location=" << location
        << ", plan_generation=" << plan_generation << ", num_fragments="
                 << fragments.size() << " - " << e << HT_END;
  }
}

void RangeServer::phantom_load(ResponseCallback *cb, const String &location,
        int plan_generation,
        const vector<uint32_t> &fragments,
        const vector<QualifiedRangeSpec> &specs,
        const vector<RangeState> &states) {
  TableInfoPtr table_info;
  FailoverPhantomRangeMap::iterator failover_map_it;
  PhantomRangeMapPtr phantom_range_map;
  TableInfoMapPtr phantom_tableinfo_map;
  int error;

  HT_INFOF("phantom_load location=%s, plan_generation=%d, num_fragments=%d,"
           " num_ranges=%d", location.c_str(), plan_generation,
           (int)fragments.size(), (int)specs.size());

  if (!m_system_replay_finished) {
    if (!wait_for_system_recovery_finish(cb->get_event()->expiration_time()))
      return;
  }

  HT_ASSERT(!specs.empty());

  HT_MAYBE_FAIL_X("phantom-load-user", specs[0].table.is_user());

  {
    ScopedLock lock(m_failover_mutex);
    failover_map_it = m_failover_map.find(location);
    if (failover_map_it == m_failover_map.end()) {
      phantom_range_map = new PhantomRangeMap(plan_generation);
      m_failover_map[location] = phantom_range_map;
    }
    else
      phantom_range_map = failover_map_it->second;
  }

  {
    Locker<PhantomRangeMap> lock(*phantom_range_map);

    // check for out-of-order phantom_load requests
    if (plan_generation < phantom_range_map->get_plan_generation())
      return;

    if (plan_generation > phantom_range_map->get_plan_generation())
      phantom_range_map->reset(plan_generation);
    else if (phantom_range_map->loaded()) {
      cb->response_ok();
      return;
    }

    phantom_tableinfo_map = phantom_range_map->get_tableinfo_map();

    try {
      for (size_t i=0; i<specs.size(); i++) {
        const QualifiedRangeSpec &spec = specs[i];
        const RangeState &state = states[i];

        // XXX: TODO: deal with dropped tables
        //if (m_dropped_table_id_cache->contains(range.qualified_range.table.id)) {
        //  HT_WARNF("Table %s has been dropped", range.qualified_range.table.id);
        //  cb->error(Error::RANGESERVER_TABLE_DROPPED, range.qualified_range.table.id);
        //  return;
        //}
        phantom_tableinfo_map->get(spec.table.id, table_info);

        uint32_t generation = table_info->get_schema()->get_generation();
        if (generation > spec.table.generation) {
          HT_WARNF("Table generation mismatch in phantom load request (%d < %d),"
                   " automatically upgrading", (int)spec.table.generation, (int)generation);
          ((QualifiedRangeSpec *)&spec)->table.generation = generation;
        }

        if (!live(spec))
          phantom_range_map->insert(spec, state, table_info->get_schema(), fragments);
      }
    }
    catch (Exception &e) {
      HT_ERROR_OUT << "Phantom load failed - " << e << HT_END;
      if ((error = cb->error(e.code(), e.what())))
        HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
      return;
    }

    phantom_range_map->set_loaded();
  }

  cb->response_ok();
}

void RangeServer::phantom_update(ResponseCallbackPhantomUpdate *cb,
        const String &location, int plan_generation, QualifiedRangeSpec &range,
        uint32_t fragment, EventPtr &event) {
  std::stringstream sout;

  sout << "phantom_update location=" << location << ", fragment="
       << fragment << ", range=" << range;
  HT_INFOF("%s", sout.str().c_str());

  FailoverPhantomRangeMap::iterator failover_map_it;
  PhantomRangeMapPtr phantom_range_map;
  PhantomRangePtr phantom_range;

  HT_MAYBE_FAIL_X("phantom-update-user", range.table.is_user());
  HT_MAYBE_FAIL_X("phantom-update-metadata", range.table.is_metadata());

  {
    ScopedLock lock(m_failover_mutex);
    failover_map_it = m_failover_map.find(location);
    if (failover_map_it == m_failover_map.end()) {
      HT_THROW(Error::RANGESERVER_PHANTOM_RANGE_MAP_NOT_FOUND,
               "no phantom range map found for recovery of " + location);
    }
    phantom_range_map = failover_map_it->second;
  }

  {
    Locker<PhantomRangeMap> lock(*phantom_range_map);

    // verify plan generation
    if (plan_generation != phantom_range_map->get_plan_generation())
      HT_THROWF(Error::RANGESERVER_RECOVERY_PLAN_GENERATION_MISMATCH,
                "supplied = %d, installed == %d", plan_generation,
                phantom_range_map->get_plan_generation());

    if (phantom_range_map->replayed()) {
      cb->response_ok();
      return;
    }

    HT_ASSERT(phantom_range_map->loaded());

    phantom_range_map->get(range, phantom_range);
    if (phantom_range && !phantom_range->replayed() && !phantom_range->add(fragment, event)) {
      String msg = format("fragment %d completely received for range "
                          "%s[%s..%s]", fragment, range.table.id, range.range.start_row,
                          range.range.end_row);
      HT_INFOF("%s", msg.c_str());
      cb->error(Error::RANGESERVER_FRAGMENT_ALREADY_PROCESSED, msg);
      return;
    }

  }

  cb->response_ok();
}

void RangeServer::phantom_prepare_ranges(ResponseCallback *cb, int64_t op_id,
        const String &location, int plan_generation, 
        const vector<QualifiedRangeSpec> &specs) {
  FailoverPhantomRangeMap::iterator failover_map_it;
  TableInfoMapPtr phantom_map;
  PhantomRangeMapPtr phantom_range_map;
  PhantomRangePtr phantom_range;
  TableInfoPtr phantom_table_info;
  vector<MetaLog::Entity *> metalog_entities;
  bool root_log_exists, metadata_log_exists, system_log_exists;
  root_log_exists = metadata_log_exists = system_log_exists = false;

  HT_INFOF("phantom_prepare_ranges op_id=%lld, location=%s, plan_generation=%d,"
           " num_ranges=%d", (Lld)op_id, location.c_str(), plan_generation,
           (int)specs.size());

  if (!m_replay_finished) {
    if (!wait_for_recovery_finish(cb->get_event()->expiration_time()))
      return;
  }

  cb->response_ok();

  {
    ScopedLock lock(m_failover_mutex);
    failover_map_it = m_failover_map.find(location);
    if (failover_map_it == m_failover_map.end()) {
      try {
        String msg = format("No phantom map found for %s", location.c_str());
        HT_INFOF("%s", msg.c_str());
        m_master_client->phantom_prepare_complete(op_id, location, plan_generation,
                              Error::RANGESERVER_PHANTOM_RANGE_MAP_NOT_FOUND, msg);
      }
      catch (Exception &e) {
        HT_ERROR_OUT << e << HT_END;
      }
      return;
    }
    phantom_range_map = failover_map_it->second;
  }

  try {
    Locker<PhantomRangeMap> lock(*phantom_range_map);

    if (phantom_range_map->prepared()) {
      try {
        m_master_client->phantom_prepare_complete(op_id, location, plan_generation, Error::OK, "");
      }
      catch (Exception &e) {
        HT_ERROR_OUT << e << HT_END;
      }
      return;
    }

    HT_ASSERT(phantom_range_map->loaded());

    phantom_map = phantom_range_map->get_tableinfo_map();

    foreach_ht(const QualifiedRangeSpec &rr, specs) {
      phantom_table_info = 0;
      HT_ASSERT(phantom_map->lookup(rr.table.id, phantom_table_info));
      TableInfoPtr table_info;
      m_live_map->get(rr.table.id, table_info);

      if (rr.table.generation != table_info->get_schema()->get_generation())
        HT_WARNF("Table (id=%s) generation mismatch %d != %d", rr.table.id, 
                 rr.table.generation,
                 table_info->get_schema()->get_generation());

      //HT_DEBUG_OUT << "Creating Range object for range " << rr << HT_END;
      // create a real range and its transfer log
      phantom_range_map->get(rr, phantom_range);

      // If already staged, continue with next range
      if (!phantom_range || phantom_range->prepared())
        continue;

      if (!Global::metadata_table) {
        ScopedLock lock(Global::mutex);
        // TODO double-check locking (works fine on x86 and amd64 but may fail
        // on other archs without using a memory barrier
        if (!Global::metadata_table)
          Global::metadata_table = new Table(m_props, m_conn_manager,
                                             Global::hyperspace, m_namemap,
                                             TableIdentifier::METADATA_NAME);
      }

      // if we're about to move a root range: make sure that the location
      // of the metadata-table is updated
      if (rr.table.is_metadata()) {
        Global::metadata_table->get_range_locator()->invalidate(&rr.table,
                                                                Key::END_ROOT_ROW);
        Global::metadata_table->get_range_locator()->set_root_stale();
      }

      phantom_range->create_range(m_master_client, table_info,
                                  Global::log_dfs);
      HT_DEBUG_OUT << "Range object created for range " << rr << HT_END;
    }

    CommitLog *log;
    foreach_ht(const QualifiedRangeSpec &rr, specs) {
      bool is_empty = true;

      phantom_range_map->get(rr, phantom_range);

      // If already prepared, continue with next range
      if (!phantom_range || phantom_range->prepared())
        continue;

      phantom_range->populate_range_and_log(Global::log_dfs, &is_empty);

      HT_DEBUG_OUT << "populated range and log for range " << rr << HT_END;

      RangePtr range = phantom_range->get_range();
      {
        if (rr.is_root()) {
          if (!root_log_exists){
            if (!Global::root_log) {
              Global::log_dfs->mkdirs(Global::log_dir + "/root");
              ScopedLock lock(Global::mutex);
              Global::root_log = new CommitLog(Global::log_dfs,
                                               Global::log_dir + "/root", m_props);
              root_log_exists = true;
            }
          }
          log = Global::root_log;
        }
        else if (rr.table.is_metadata()) {
          if (!metadata_log_exists){
            if (!Global::metadata_log) {
              Global::log_dfs->mkdirs(Global::log_dir + "/metadata");
              ScopedLock lock(Global::mutex);
              Global::metadata_log = new CommitLog(Global::log_dfs,
                                                   Global::log_dir + "/metadata", m_props);
              metadata_log_exists = true;
            }
          }
          log = Global::metadata_log;
        }
        else if (rr.table.is_system()) {
          if (!system_log_exists){
            if (!Global::system_log) {
              Global::log_dfs->mkdirs(Global::log_dir + "/system");
              ScopedLock lock(Global::mutex);
              Global::system_log = new CommitLog(Global::log_dfs,
                                                 Global::log_dir + "/system", m_props);
              system_log_exists = true;
            }
          }
          log = Global::system_log;
        }
        else
          log = Global::user_log;
      }

      CommitLogReaderPtr phantom_log = phantom_range->get_phantom_log();
      HT_ASSERT(phantom_log && log);
      int error = Error::OK;
      if (!is_empty
          && (error = log->link_log(phantom_log.get())) != Error::OK) {

        String msg = format("Problem linking phantom log '%s' for range %s[%s..%s]",
                            phantom_range->get_phantom_logname().c_str(),
                            rr.table.id, rr.range.start_row, rr.range.end_row);

        m_master_client->phantom_prepare_complete(op_id, location, plan_generation, error, msg);
        return;
      }

      metalog_entities.push_back( range->metalog_entity() );

      HT_ASSERT(phantom_map->lookup(rr.table.id, phantom_table_info));

      HT_INFO("phantom adding range");

      phantom_table_info->add_range(range, true);
        
      phantom_range->set_prepared();
    }

    HT_MAYBE_FAIL_X("phantom-prepare-ranges-user-1", specs.back().table.is_user());

    HT_DEBUG_OUT << "write all range entries to rsml" << HT_END;
    // write metalog entities
    if (Global::rsml_writer)
      Global::rsml_writer->record_state(metalog_entities);
    else
      HT_THROW(Error::SERVER_SHUTTING_DOWN,
               Global::location_initializer->get());

    HT_MAYBE_FAIL_X("phantom-prepare-ranges-root-2", specs.back().is_root());
    HT_MAYBE_FAIL_X("phantom-prepare-ranges-user-2", specs.back().table.is_user());

    phantom_range_map->set_prepared();

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    try {
      m_master_client->phantom_prepare_complete(op_id, location, plan_generation, e.code(), e.what());
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
    }
    return;
  }

  try {
    m_master_client->phantom_prepare_complete(op_id, location, plan_generation, Error::OK, "");
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }

  HT_MAYBE_FAIL("phantom-prepare-ranges-user-3");

}

void RangeServer::phantom_commit_ranges(ResponseCallback *cb, int64_t op_id,
        const String &location, int plan_generation, 
        const vector<QualifiedRangeSpec> &specs) {
  FailoverPhantomRangeMap::iterator failover_map_it;
  PhantomRangeMapPtr phantom_range_map;
  TableInfoMapPtr phantom_map;
  TableMutatorPtr mutator;
  KeySpec key;
  String our_location = Global::location_initializer->get();
  vector<MetaLog::Entity *> entities;
  StringSet linked_logs;
  map<QualifiedRangeSpec, TableInfoPtr> phantom_table_info_map;
  map<QualifiedRangeSpec, int> error_map;
  vector<RangePtr> range_vec;

  HT_INFOF("phantom_commit_ranges op_id=%lld, location=%s, plan_generation=%d,"
           " num_ranges=%d", (Lld)op_id, location.c_str(), plan_generation,
           (int)specs.size());

  if (!m_system_replay_finished) {
    if (!wait_for_system_recovery_finish(cb->get_event()->expiration_time()))
      return;
  }

  cb->response_ok();

  if (live(specs)) {
    // Remove phantom map
    {
      ScopedLock lock(m_failover_mutex);
      m_failover_map.erase(location);
    }
    // Report success
    try {
      m_master_client->phantom_commit_complete(op_id, location, plan_generation, Error::OK, "");
    }
    catch (Exception &e) {
      String msg = format("Error during phantom_commit op_id=%lld, "
        "plan_generation=%d, location=%s, num ranges=%u", (Lld)op_id,
        plan_generation, location.c_str(), (unsigned)specs.size());
      HT_ERRORF("%s - %s", Error::get_text(e.code()), msg.c_str());
    }
    return;
  }

  {
    ScopedLock lock(m_failover_mutex);
    failover_map_it = m_failover_map.find(location);
    if (failover_map_it == m_failover_map.end()) {
      try {
        String msg = format("No phantom map found for %s plan_generation=%d",
                            location.c_str(), plan_generation);
        HT_INFOF("%s", msg.c_str());
        m_master_client->phantom_commit_complete(op_id, location, plan_generation,
                              Error::RANGESERVER_PHANTOM_RANGE_MAP_NOT_FOUND, msg);
      }
      catch (Exception &e) {
        HT_ERROR_OUT << e << HT_END;
      }
      return;
    }
    phantom_range_map = failover_map_it->second;
  }

  try {
    Locker<PhantomRangeMap> lock(*phantom_range_map);

    // Double-check to see if concurrent method call flipped them live
    if (live(specs))
      return;

    HT_ASSERT(phantom_range_map->prepared());
    HT_ASSERT(!phantom_range_map->committed());

    phantom_map = phantom_range_map->get_tableinfo_map();

    foreach_ht(const QualifiedRangeSpec &rr, specs) {

      RangePtr range;
      PhantomRangePtr phantom_range;

      String range_name = format("%s[%s..%s]", rr.table.id,
                                 rr.range.start_row, rr.range.end_row);

      // Fetch phantom_range object
      phantom_range_map->get(rr, phantom_range);

      if (!phantom_range || phantom_range->committed())
        continue;

      range = phantom_range->get_range();

      HT_ASSERT(range);

      bool is_root = range->is_root();
      MetaLogEntityRange *entity = range->metalog_entity();
      entity->set_needs_compaction(true);
      entity->set_load_acknowledged(false);
      entity->clear_state_bits(RangeState::PHANTOM);
      entities.push_back(entity);
      phantom_range->get_linked_logs(linked_logs);

      HT_MAYBE_FAIL_X("phantom-commit-user-1", rr.table.is_user());

      /**
       * Take ownership of the range by writing the 'Location' column in the
       * METADATA table, or /hypertable/root{location} attribute of Hyperspace
       * if it is the root range.
       */
      {
        String range_str = format("%s[%s..%s]", rr.table.id, rr.range.start_row, rr.range.end_row);
        HT_INFOF("Taking ownership of range %s", range_str.c_str());
      }
      if (!is_root) {
        String metadata_key_str = format("%s:%s", rr.table.id,rr.range.end_row);

        if (!mutator)
          mutator = Global::metadata_table->create_mutator();

        // Take ownership of the range
        key.row = metadata_key_str.c_str();
        key.row_len = strlen(metadata_key_str.c_str());
        key.column_family = "Location";
        key.column_qualifier = 0;
        key.column_qualifier_len = 0;

        // just set for now we'll do one big flush right at the end
        HT_DEBUG_OUT << "Update metadata location for " << key << " to "
                     << our_location << HT_END;
        mutator->set(key, our_location.c_str(), our_location.length());
      }
      else {  //root
        uint64_t handle=0;
        uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE;
        HT_INFO("Failing over root METADATA range");

        HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr,
                         Global::hyperspace, &handle);
        String root_filename = Global::toplevel_dir + "/root";
        handle = m_hyperspace->open(root_filename, oflags);
        Global::hyperspace->attr_set(handle, "Location", our_location.c_str(),
                                     our_location.length());
        HT_DEBUG_OUT << "Updated attr Location of " << root_filename << " to "
                     << our_location << HT_END;
      }

      phantom_range->set_committed();
    }

    // flush mutator
    if (mutator)
      mutator->flush();

    HT_MAYBE_FAIL_X("phantom-commit-user-2", specs.back().table.is_user());

    /*
     * This method atomically does the following:
     *   1. Adds linked_logs (transfer logs + children) to RemoveOkLogs entity
     *   2. Persists entities and RemoveOkLogs entity to RSML
     *   3. Merges phantom_map into the live map
     */
    m_live_map->merge(phantom_map.get(), entities, linked_logs);

    HT_MAYBE_FAIL_X("phantom-commit-user-3", specs.back().table.is_user());

    HT_INFOF("Merging phantom map into live map for recovery of %s (ID=%lld)",
             location.c_str(), (Lld)op_id);

    {
      ScopedLock lock(m_failover_mutex);
      m_failover_map.erase(location);
    }

    phantom_range_map->set_committed();

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    try {
      m_master_client->phantom_commit_complete(op_id, location, plan_generation, e.code(), e.what());
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
    }
    return;
  }

  try {
    
    m_master_client->phantom_commit_complete(op_id, location, plan_generation, Error::OK, "");

    HT_MAYBE_FAIL_X("phantom-commit-user-4", specs.back().table.is_user());

    HT_DEBUG_OUT << "phantom_commit_complete sent to master for num_ranges="
        << specs.size() << HT_END;

    // Wake up maintenance scheduler to handle any "in progress" operations
    // that were happening on the ranges just added
    {
      ScopedLock lock(m_mutex);
      if (m_timer_handler)
        m_timer_handler->schedule_immediate_maintenance();
    }

  }
  catch (Exception &e) {
    String msg = format("Error during phantom_commit op_id=%lld, "
        "plan_generation=%d, location=%s, num ranges=%u", (Lld)op_id,
        plan_generation, location.c_str(), (unsigned)specs.size());
    HT_ERRORF("%s - %s", Error::get_text(e.code()), msg.c_str());
    // do not re-throw because this would cause an error to get sent back
    // that is not expected
  }
}

bool RangeServer::live(const vector<QualifiedRangeSpec> &ranges) {
  TableInfoPtr table_info;
  size_t live_count = 0;
  foreach_ht (const QualifiedRangeSpec &qrs, ranges) {
    if (m_live_map->lookup(qrs.table.id, table_info)) {
      if (table_info->has_range(&qrs.range))
        live_count++;
    }
  }

  return live_count == ranges.size();
}

bool RangeServer::live(const QualifiedRangeSpec &spec) {
  TableInfoPtr table_info;
  if (m_live_map->lookup(spec.table.id, table_info)) {
    if (table_info->has_range(&spec.range))
      return true;
  }
  return false;
}


void RangeServer::wait_for_maintenance(ResponseCallback *cb) {
  boost::xtime expire_time = cb->get_event()->expiration_time();
  HT_INFO("wait_for_maintenance");
  if (!Global::maintenance_queue->wait_for_empty(expire_time))
    cb->error(Error::REQUEST_TIMEOUT, "");
  cb->response_ok();
}


void RangeServer::verify_schema(TableInfoPtr &table_info, uint32_t generation,
                                const TableSchemaMap *table_schemas) {
  DynamicBuffer valbuf;
  SchemaPtr schema = table_info->get_schema();

  if (schema.get() == 0 || schema->get_generation() < generation) {
    schema = 0;
    TableSchemaMap::const_iterator it;
    if (table_schemas &&
        (it = table_schemas->find(table_info->identifier().id))
            != table_schemas->end())
      schema = it->second;

    if (schema.get() == 0) {
      String tablefile = Global::toplevel_dir + "/tables/"
          + table_info->identifier().id;
      m_hyperspace->attr_get(tablefile, "schema", valbuf);
      schema = Schema::new_instance((char *)valbuf.base, valbuf.fill());
    }

    if (!schema->is_valid())
      HT_THROW(Error::RANGESERVER_SCHEMA_PARSE_ERROR,
              (String)"Schema Parse Error for table '"
              + table_info->identifier().id + "' : "
              + schema->get_error_string());

    if (schema->need_id_assignment())
      HT_THROW(Error::RANGESERVER_SCHEMA_PARSE_ERROR,
              (String)"Schema Parse Error for table '"
              + table_info->identifier().id + "' : needs ID assignment");

    table_info->update_schema(schema);

    // Generation check ...
    if (schema->get_generation() < generation)
      HT_THROWF(Error::RANGESERVER_GENERATION_MISMATCH,
                "Fetched Schema generation for table '%s' is %lld"
                " but supplied is %lld", table_info->identifier().id,
                (Lld)schema->get_generation(), (Lld)generation);
  }
}

void RangeServer::group_commit() {
  m_group_commit->trigger();
}

void RangeServer::do_maintenance() {

  HT_ASSERT(m_timer_handler);

  try {
    boost::xtime now;

    // Purge expired scanners
    Global::scanner_map.purge_expired(m_scanner_ttl);

    // Set Low Memory mode
    m_maintenance_scheduler->set_low_memory_mode(m_timer_handler->low_memory_mode());

    // Schedule maintenance
    m_maintenance_scheduler->schedule();

    // Check for control files
    boost::xtime_get(&now, TIME_UTC_);
    if (xtime_diff_millis(m_last_control_file_check, now) >= (int64_t)m_control_file_check_interval) {
      if (FileUtils::exists(System::install_dir + "/run/query-profile")) {
	if (!m_profile_query) {
          ScopedLock lock(m_profile_mutex);
	  String output_fname = System::install_dir + "/run/query-profile.output";
	  m_profile_query_out.open(output_fname.c_str(), ios_base::out|ios_base::app);
	  m_profile_query = true;
	}
      }
      else {
	if (m_profile_query) {
          ScopedLock lock(m_profile_mutex);
	  m_profile_query_out.close();
	  m_profile_query = false;
	}
      }
      m_last_control_file_check = now;      
    }

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }

  // Notify timer handler so that it can resume
  m_timer_handler->maintenance_scheduled_notify();

  HT_INFOF("Memory Usage: %llu bytes", (Llu)Global::memory_tracker->balance());
}

bool RangeServer::wait_for_recovery_finish(boost::xtime expire_time) {
  ScopedLock lock(m_mutex);
  while (!m_replay_finished) {
    HT_INFO("Waiting for recovery to complete...");
    if (!m_replay_finished_cond.timed_wait(lock, expire_time))
      return false;
  }
  return true;
}

bool RangeServer::wait_for_root_recovery_finish(boost::xtime expire_time) {
  ScopedLock lock(m_mutex);
  while (!m_root_replay_finished) {
    HT_INFO("Waiting for ROOT recovery to complete...");
    if (!m_root_replay_finished_cond.timed_wait(lock, expire_time))
      return false;
  }
  return true;
}

bool RangeServer::wait_for_metadata_recovery_finish(boost::xtime expire_time) {
  ScopedLock lock(m_mutex);
  while (!m_metadata_replay_finished) {
    HT_INFO("Waiting for METADATA recovery to complete...");
    if (!m_metadata_replay_finished_cond.timed_wait(lock, expire_time))
      return false;
  }
  return true;
}

bool RangeServer::wait_for_system_recovery_finish(boost::xtime expire_time) {
  ScopedLock lock(m_mutex);
  while (!m_system_replay_finished) {
    HT_INFO("Waiting for SYSTEM recovery to complete...");
    if (!m_system_replay_finished_cond.timed_wait(lock, expire_time))
      return false;
  }
  return true;
}

bool
RangeServer::wait_for_recovery_finish(const TableIdentifier *table,
                                      const RangeSpec *range_spec,
                                      boost::xtime expire_time) {
  ScopedLock lock(m_mutex);
  if (table->is_metadata()) {
    if (!strcmp(range_spec->end_row, Key::END_ROOT_ROW)) {
      while (!m_root_replay_finished) {
        HT_INFO("Waiting for ROOT recovery to complete...");
        if (!m_root_replay_finished_cond.timed_wait(lock, expire_time))
          return false;
      }
    }
    else {
      while (!m_metadata_replay_finished) {
        HT_INFO("Waiting for METADATA recovery to complete...");
        if (!m_metadata_replay_finished_cond.timed_wait(lock, expire_time))
          return false;
      }
    }
  }
  else if (table->is_system()) {
    while (!m_system_replay_finished) {
      HT_INFO("Waiting for SYSTEM recovery to complete...");
      if (!m_system_replay_finished_cond.timed_wait(lock, expire_time))
        return false;
    }
  }
  else {
    while (!m_replay_finished) {
      HT_INFO("Waiting for recovery to complete...");
      if (!m_replay_finished_cond.timed_wait(lock, expire_time))
        return false;
    }
  }

  return true;
}
