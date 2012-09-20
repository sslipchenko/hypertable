/** -*- c++ -*-
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

#include "Common/Compat.h"
#include "Common/FailureInducer.h"

#include "Context.h"
#include "LoadBalancer.h"
#include "Operation.h"
#include "OperationRecover.h"
#include "ReferenceManager.h"
#include "RecoveryReplayCounter.h"
#include "BalancePlanAuthority.h"

using namespace Hypertable;
using namespace std;

Context::~Context() {
  if (hyperspace && master_file_handle > 0) {
    hyperspace->close(master_file_handle);
    master_file_handle = 0;
  }
  delete balancer;
  delete reference_manager;
  delete m_balance_plan_authority;
}


void Context::notification_hook(int type, const String &message) {
  String type_str;
  switch (type) {
    case NotificationHookType::NOTICE:
      type_str = "NOTICE";
      break;
    case NotificationHookType::ERROR:
      type_str = "ERROR";
      break;
    default:
      type_str = "unknown type";
      break;
  };

  String cmd = format("%s/conf/notification-hook.sh '%s' '%s'", 
          System::install_dir.c_str(), type_str.c_str(), message.c_str());

  HT_DEBUG_OUT << "notification-hook (" << type_str << "): " << cmd << HT_END;

  int ret = ::system(cmd.c_str());
  HT_INFO_OUT << "notification-hook returned: " << ret << HT_END;
  if (ret != 0) {
    HT_WARNF("shell script conf/notification-hook.sh ('%s') returned "
            "error %d", cmd.c_str(), ret);
  }
}

void Context::set_balance_plan_authority(BalancePlanAuthority *bpa) {
  ScopedLock lock(mutex);
  m_balance_plan_authority = bpa;
}

BalancePlanAuthority *Context::get_balance_plan_authority() {
  ScopedLock lock(mutex);
  if (!m_balance_plan_authority)
    m_balance_plan_authority = new BalancePlanAuthority(this, mml_writer);
  return m_balance_plan_authority;
}

void Context::replay_complete(EventPtr &event) {
  int64_t id;
  uint32_t attempt, fragment;
  map<uint32_t, int> error_map;
  const uint8_t *decode_ptr = event->payload;
  size_t decode_remain = event->payload_len;
  int nn, error;
  bool success;

  id       = Serialization::decode_i64(&decode_ptr, &decode_remain);
  attempt  = Serialization::decode_i32(&decode_ptr, &decode_remain);
  nn       = Serialization::decode_i32(&decode_ptr, &decode_remain);

  HT_DEBUG_OUT << "Received replay_complete for op_id=" << id << " attempt="
      << attempt << " num_ranges=" << nn << " from " << event->proxy << HT_END;

  for (int ii = 0; ii < nn; ++ii) {
    fragment = Serialization::decode_i32(&decode_ptr, &decode_remain);
    error    = Serialization::decode_i32(&decode_ptr, &decode_remain);
    error_map[fragment] = error;
  }
  success  = Serialization::decode_bool(&decode_ptr, &decode_remain);

  {
    ScopedLock lock(m_recovery_state.m_mutex);
    RecoveryState::ReplayMap::iterator it = m_recovery_state.m_replay_map.find(id);
    RecoveryReplayCounterPtr replay_counter;
    if (it != m_recovery_state.m_replay_map.end()) {
      replay_counter = it->second;
      if (!replay_counter->complete(attempt, success, error_map)) {
        HT_WARN_OUT << "non-pending player complete message received for "
            "operation=" << id << " attempt=" << attempt << HT_END;
      }
    }
    else {
      HT_WARN_OUT << "No RecoveryReplayCounter found for operation="
          << id << " attempt=" << attempt << HT_END;
    }

    // if there were any corrupt fragments then notify the admin
    bool notify = replay_counter->has_errors();
    RecoveryReplayCounter::ErrorMap errors = replay_counter->get_errors();
    if (HT_FAILURE_SIGNALLED("bad-log-fragments-1")
        && !replay_counter->has_errors()) {
      errors[1] = Error::BAD_SCHEMA;
      errors[2] = Error::BLOCK_COMPRESSOR_DEFLATE_ERROR;
      notify = true;
    }
    if (notify) {
      String msg;
      msg = 
"Dear administrator,\\n"
"\\n"
"The recently started recovery operation reported failures when recovering\\n"
"log file fragments.  The following fragments are corrupt:\\n\\n";
      RecoveryReplayCounter::ErrorMap::iterator it;
      for (it = errors.begin(); it != errors.end(); ++it) {
        if (it->second != Error::OK) {
          msg += format("\t fragment id %u: error 0x%x (%s)\\n",
                  it->first, it->second, Error::get_text(it->second));
          HT_ERRORF("Corrupted logfile fragment %u: error 0x%x (%s)",
                  it->first, it->second, Error::get_text(it->second));
        }
      }
      msg += 
"\\n\\n"
"The log files on the Master and the RangeServer(s) will have more information\\n"
"about the error.\\n\\n";

      notification_hook(NotificationHookType::ERROR, msg);
    }
  }

  HT_DEBUG_OUT << "Exitting replay_complete for op_id=" << id << " attempt="
      << attempt << " num_ranges=" << nn << " from " << event->proxy << HT_END;
}

void Context::prepare_complete(EventPtr &event) {
  const uint8_t *decode_ptr = event->payload;
  size_t decode_remain = event->payload_len;
  int64_t id;
  uint32_t attempt;
  int nn;
  RecoveryCounter::Result rr;
  vector<RecoveryCounter::Result> results;

  id       = Serialization::decode_vi64(&decode_ptr, &decode_remain);
  attempt  = Serialization::decode_vi32(&decode_ptr, &decode_remain);
  nn       = Serialization::decode_vi32(&decode_ptr, &decode_remain);

  HT_DEBUG_OUT << "Received prepare_complete for op_id=" << id << " attempt="
      << attempt << " num_ranges=" << nn << " from " << event->proxy << HT_END;

  for (int ii=0; ii<nn; ++ii) {
    rr.range.decode(&decode_ptr, &decode_remain);
    rr.error    = Serialization::decode_vi32(&decode_ptr, &decode_remain);
    results.push_back(rr);
  }

  {
    ScopedLock lock(m_recovery_state.m_mutex);
    RecoveryState::CounterMap::iterator it = m_recovery_state.m_prepare_map.find(id);
    RecoveryCounterPtr prepare_counter;
    if (it != m_recovery_state.m_prepare_map.end()) {
      prepare_counter = it->second;
      prepare_counter->result_callback(attempt, results);
    }
    else
      HT_WARN_OUT << "No RecoveryCounter found for operation=" << id << HT_END;
  }
  HT_DEBUG_OUT << "Exitting prepare_complete for op_id=" << id << " attempt="
      << attempt << " num_ranges=" << nn << " from " << event->proxy << HT_END;

  return;
}

void Context::commit_complete(EventPtr &event) {
  const uint8_t *decode_ptr = event->payload;
  size_t decode_remain = event->payload_len;
  int64_t id;
  uint32_t attempt;
  int nn;
  RecoveryCounter::Result rr;
  vector<RecoveryCounter::Result> results;

  id       = Serialization::decode_vi64(&decode_ptr, &decode_remain);
  attempt  = Serialization::decode_vi32(&decode_ptr, &decode_remain);
  nn       = Serialization::decode_vi32(&decode_ptr, &decode_remain);
  for (int ii = 0; ii < nn; ++ii) {
    rr.range.decode(&decode_ptr, &decode_remain);
    rr.error    = Serialization::decode_vi32(&decode_ptr, &decode_remain);
    results.push_back(rr);
  }

  HT_DEBUG_OUT << "Received phantom_commit_complete for op_id=" << id
      << " attempt=" << attempt << " num_ranges=" << nn << " from "
      << event->proxy << HT_END;
  {
    ScopedLock lock(m_recovery_state.m_mutex);
    RecoveryState::CounterMap::iterator it = m_recovery_state.m_commit_map.find(id);
    RecoveryCounterPtr commit_counter;
    if (it != m_recovery_state.m_commit_map.end()) {
      commit_counter = it->second;
      commit_counter->result_callback(attempt, results);
    }
    else
      HT_WARN_OUT << "No RecoveryCounter found for operation=" << id
          << HT_END;
  }

  HT_DEBUG_OUT << "Exitting phantom_commit_complete for op_id=" << id
      << " attempt=" << attempt << " num_ranges=" << nn << " from "
      << event->proxy << HT_END;
  return;
}


bool Context::reassigned(TableIdentifier *table, RangeSpec &range, String &location) {
  // TBD
  return false;
}


bool Context::can_accept_ranges(const RangeServerStatistics &stats)
{
  static int threshold = 0;
  if (!threshold)
    threshold = props->get_i32("Hypertable.Master.DiskThreshold.Percentage");

  // system info was not yet initialized; assume that the disks are available
  if (!stats.stats) {
    HT_WARNF("RangeServer %s: no disk usage statistics available",
            stats.location.c_str());
    return true;
  }

  // accept new ranges if there's at least one disk below the threshold
  foreach_ht (const FsStat &fs, stats.stats->system.fs_stat) {
    if (fs.use_pct < threshold)
      return true;
  }
  HT_WARNF("RangeServer %s: all disks are above threshold of %d %% "
          "(Hypertable.Master.DiskThresholdPct); will not assign ranges",
          stats.location.c_str(), threshold);
  return false;
}

RecoveryReplayCounterPtr
Context::RecoveryState::create_replay_counter(int64_t id, uint32_t attempt)
{
  ScopedLock lock(m_mutex);

  RecoveryReplayCounterPtr counter = new RecoveryReplayCounter(attempt);

  if (m_replay_map.find(id) != m_replay_map.end())
    m_replay_map.erase(id);

  m_replay_map.insert(make_pair(id, counter));
  return counter;
}

void 
Context::RecoveryState::erase_replay_counter(int64_t id)
{
  ScopedLock lock(m_mutex);
  m_replay_map.erase(id);
}

RecoveryCounterPtr
Context::RecoveryState::create_prepare_counter(int64_t id, uint32_t attempt)
{
  ScopedLock lock(m_mutex);

  RecoveryCounterPtr counter = new RecoveryCounter(attempt);

  if (m_prepare_map.find(id) != m_prepare_map.end())
    m_prepare_map.erase(id);

  m_prepare_map.insert(make_pair(id, counter));
  return counter;
}

void
Context::RecoveryState::erase_prepare_counter(int64_t id)
{
  ScopedLock lock(m_mutex);
  m_prepare_map.erase(id);
}

RecoveryCounterPtr
Context::RecoveryState::create_commit_counter(int64_t id, uint32_t attempt)
{
  ScopedLock lock(m_mutex);

  RecoveryCounterPtr counter = new RecoveryCounter(attempt);

  if (m_commit_map.find(id) != m_commit_map.end())
    m_commit_map.erase(id);

  m_commit_map.insert(make_pair(id, counter));
  return counter;
}

void
Context::RecoveryState::erase_commit_counter(int64_t id)
{
  ScopedLock lock(m_mutex);
  m_commit_map.erase(id);
}
