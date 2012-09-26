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

#include "BalanceAlgorithmEvenRanges.h"
#include "BalanceAlgorithmOffload.h"
#include "LoadBalancer.h"
#include "Utility.h"

#include <cstring>

using namespace Hypertable;

LoadBalancer::LoadBalancer(ContextPtr context)
  : m_context(context), m_new_server_added(false) {

  m_crontab = new Crontab( m_context->props->get_str("Hypertable.LoadBalancer.Crontab") );

  m_new_server_balance_delay =
    m_context->props->get_i32("Hypertable.LoadBalancer.BalanceDelay.NewServer");

  // TODO: fix me!!
  //m_enabled = context->props->get_bool("Hypertable.LoadBalancer.Enable");
  m_enabled = false;

  m_loadavg_threshold = 
            m_context->props->get_f64("Hypertable.LoadBalancer.LoadavgThreshold");

  time_t t = time(0) +
    m_context->props->get_i32("Hypertable.LoadBalancer.BalanceDelay.Initial");

  m_next_balance_time_load = m_crontab->next_event(t);

  if (m_context->rsc_manager->exist_unbalanced_servers()) {
    m_new_server_added = true;
    m_next_balance_time_new_server = time(0) + m_new_server_balance_delay;
  }
  else
    m_next_balance_time_new_server = 0;
}

void LoadBalancer::signal_new_server() {
  ScopedLock lock(m_add_mutex);
  m_new_server_added = true;
  m_next_balance_time_new_server = time(0) + m_new_server_balance_delay;
}

bool LoadBalancer::balance_needed() {
  ScopedLock lock(m_add_mutex);
  if (m_paused)
    return false;
  if (m_new_server_added && time(0) >= m_next_balance_time_new_server)
    return true;
  if (m_enabled && time(0) >= m_next_balance_time_load)
    return true;
  return false;
}


void LoadBalancer::unpause() {
  ScopedLock lock(m_add_mutex);
  if (m_context->rsc_manager->exist_unbalanced_servers()) {
    m_new_server_added = true;
    m_next_balance_time_new_server = time(0) + m_new_server_balance_delay;
  }
  m_paused = false;
}


void LoadBalancer::transfer_monitoring_data(vector<RangeServerStatistics> &stats) {
  ScopedLock lock(m_add_mutex);
  m_statistics.swap(stats);
}


void LoadBalancer::create_plan(BalancePlanPtr &plan,
                               std::vector <RangeServerConnectionPtr> &balanced) {
  String name, arguments;
  time_t now = time(0);
  BalanceAlgorithmPtr algo;

  {
    ScopedLock lock(m_add_mutex);
    String algorithm_spec = plan->algorithm;

    if (m_statistics.size() <= 1)
      HT_THROWF(Error::MASTER_BALANCE_PREVENTED,
                "Too few servers (%u)", (unsigned)m_statistics.size());

    /**
     * Split algorithm spec into algorithm name + arguments
     */
    boost::trim(algorithm_spec);
    if (algorithm_spec == "") {
      if (m_new_server_added && now >= m_next_balance_time_new_server)
        name = "table_ranges";
      else if (now >= m_next_balance_time_load)
        name = "load";
      else
        HT_THROW(Error::MASTER_BALANCE_PREVENTED, "Balance not needed");
    }
    else {
      char *ptr = strchr((char *)algorithm_spec.c_str(), ' ');
      if (ptr) {
        name = String(algorithm_spec, 0, ptr-algorithm_spec.c_str());
        if (*ptr) {
          arguments = String(ptr);
          boost::trim(arguments);
        }
      }
      else
        name = algorithm_spec;
      boost::to_lower(name);
    }

    if (name == "offload")
      algo = new BalanceAlgorithmOffload(m_context, m_statistics, arguments);
    else if (name == "table_ranges")
      algo = new BalanceAlgorithmEvenRanges(m_context, m_statistics);
    else
      HT_THROWF(Error::MASTER_BALANCE_PREVENTED,
                "Unrecognized algorithm - %s", name.c_str());
  }

  algo->compute_plan(plan, balanced);

  {
    ScopedLock lock(m_add_mutex);
    m_paused = true;
  }
  return;

    /**

  if (algorithm == "load") {

    foreach_ht(const RangeServerStatistics &server_stats, statistics) {
      if (!server_stats.stats->live) {
        HT_INFOF("Aborting balance because '%s' not yet live",
                 server_stats.location.c_str());
        return false;
      }
    }

    size_t total_ranges = 0;
    get_unbalanced_servers(statistics);
      size_t num_unbalanced_servers=m_unbalanced_servers.size();
      foreach_ht(const RangeServerStatistics &server_stats, statistics) {
        total_ranges += server_stats.stats->range_count;
      }
      // 3 ranges shd always be in the system (2 metadata, 1 rs_metrics)
      if (num_unbalanced_servers > 0
          && total_ranges > 3 + statistics.size()) {
        HT_INFO_OUT << "Found " << num_unbalanced_servers
            << " new/unbalanced servers, " << "total ranges=" << total_ranges
            << ", total rangeservers=" << statistics.size() << HT_END;
        mode = BALANCE_MODE_DISTRIBUTE_TABLE_RANGES;
        m_wait_time_start = now;
        m_waiting_for_servers = true;
      }
    }
    else
      mode = BALANCE_MODE_DISTRIBUTE_TABLE_RANGES;
  }
  else {
    if (algorithm.compare("table_ranges") == 0) {
      mode = BALANCE_MODE_DISTRIBUTE_TABLE_RANGES;
      m_waiting_for_servers = false;
    }
    else if (algorithm.compare("load") == 0)
      mode = BALANCE_MODE_DISTRIBUTE_LOAD;
    else if (boost::starts_with(algorithm, "offload ")) {
      String list(algorithm, 8);
      boost::trim(list);
      boost::split(offload, list, boost::is_any_of(","));
      mode = BALANCE_MODE_OFFLOAD_SERVERS;
    }
    else
      HT_THROW(Error::NOT_IMPLEMENTED,
            (String)"Unknown LoadBalancer algorithm '" + algorithm
            + "' supported algorithms are 'TABLE_RANGES', 'LOAD'");
  }

  // TODO: write a factory class to create the sub balancer objects

  if (mode == BALANCE_MODE_DISTRIBUTE_LOAD && !m_waiting_for_servers) {
    distribute_load(now, statistics, balance_plan);
  }
  else if (mode == BALANCE_MODE_DISTRIBUTE_TABLE_RANGES) {
    time_duration td = now - m_wait_time_start;
    if (m_waiting_for_servers && td.total_milliseconds() > m_balance_wait)
      m_waiting_for_servers = false;
    if (!m_waiting_for_servers)
      distribute_table_ranges(statistics, balance_plan);
  }
  else if (mode == BALANCE_MODE_OFFLOAD_SERVERS) {
    offload_servers(statistics, offload, balance_plan);
  }

  if (balance_plan->moves.size()) {
    get_unbalanced_servers(statistics);
    if (mode == BALANCE_MODE_DISTRIBUTE_LOAD) {
      HT_INFO_OUT << "LoadBalancerBasic mode=BALANCE_MODE_DISTRIBUTE_LOAD"
              << HT_END;
    }
    else
      HT_INFO_OUT << "LoadBalancerBasic mode=BALANCE_MODE_DISTRIBUTE_TABLE_RANGES" << HT_END;
    HT_INFO_OUT << "BalancePlan created, move " << balance_plan->moves.size()
                << " ranges" << HT_END;
  }

    */

  // remember to return unbalanced_servers
  // set next_balance_time (if we didn't abort)
}

void Hypertable::reenable_balancer(LoadBalancer *balancer) {
  
}
