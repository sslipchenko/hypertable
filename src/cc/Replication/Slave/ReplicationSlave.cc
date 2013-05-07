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
#include "Common/ScopeGuard.h"
#include "Common/FileUtils.h"
#include "Common/SystemInfo.h"
#include "Common/md5.h"

#include "Hyperspace/Config.h"
#include "Hyperspace/Session.h"
#include "Hyperspace/LockSequencer.h"

#include "ReplicationSlave.h"
#include "RequestHandlerProcessFragment.h"

namespace Hypertable {

using namespace Hyperspace;

ReplicationSlave *ReplicationSlave::ms_instance = 0;

void
ReplicationSlave::update_state() {
  bool fail = false;
  StringSet fragments;
  StringSet::iterator fit;
  ReplicationTypes::ClusterToSlavesMap clusters;
  ReplicationTypes::TableIdToClusterMap table_ids;
  ReplicationTypes::TableIdToTableNameMap table_names;

  for (int retry = 0; retry < 3; retry++) {
    // ask the Replication.Master for an updated list of the current state
    ReplicationMasterClient client(m_master_address);

    fail = false;
    try {
      client.assign_fragments(m_location, m_port, fragments, clusters,
              table_ids, table_names);
    }
    catch (Exception &ex) {
      fail = true;
      HT_ERROR_OUT << ex << HT_END;
      // fetch an up-to-date master address from hyperspace
      update_master_address();
      // try again
      continue;
    }
    if (fail == false)
      break;
  }

  // failed too often? try again later
  if (fail) {
    HT_WARN("Failed too often; will try again later");
    return;
  }

  ReplicationTypes::ClusterToSlavesMap::iterator cit;
  for (cit = clusters.begin(); cit != clusters.end(); ++cit) {
    HT_INFOF("Adding cluster '%s': %d slaves", cit->first.c_str(),
             (int)cit->second.size());
    foreach_ht (const InetAddr &addr, cit->second)
      HT_INFOF(" .... slave is: %s", addr.format().c_str());
  }
  ReplicationTypes::TableIdToClusterMap::iterator tit;
  for (tit = table_ids.begin(); tit != table_ids.end(); ++tit) {
    HT_INFOF("Adding table %s: replicated to %d clusters",
             tit->first.c_str(), (int)tit->second.size());
  }
  ReplicationTypes::TableIdToTableNameMap::iterator tmit;
  for (tmit = table_names.begin(); tmit != table_names.end(); ++tmit) {
    HT_INFOF("Adding table name %s (%s)", tmit->first.c_str(),
             tmit->second.c_str());
  }

  ScopedLock lock(m_mutex);
  m_clusters.swap(clusters);
  m_table_ids.swap(table_ids);
  m_table_names.swap(table_names);

  for (fit = fragments.begin(); fit != fragments.end(); fit++) {
    // already scheduled?
    if (m_fragments.find(*fit) != m_fragments.end()) {
      HT_INFOF("Fragment %s already assigned, skipping", (*fit).c_str());
      continue;
    }
    HT_INFOF("Fragment %s newly assigned", (*fit).c_str());
    m_fragments.insert(*fit);
    // start to process this fragment
    EventPtr event(new Event(Event::MESSAGE));
    m_context->get_app_queue()->add(new RequestHandlerProcessFragment(event,
                m_context, *fit, m_table_ids, m_table_names, m_clusters));
  }
}

void
ReplicationSlave::finished_fragment(const String &fragment, int error,
        uint64_t offset, const std::vector<String> &linked_logs) {
  bool fail;
  for (int retry = 0; retry < 10; retry++) {
    // ask the Replication.Master for an updated list of the current state
    ReplicationMasterClient client(m_master_address);

    fail = false;
    try {
      client.finished_fragment(fragment, error, offset, linked_logs);
    }
    catch (Exception &ex) {
      fail = true;
      HT_ERROR_OUT << ex << HT_END;
      sleep(5 + 2 * retry);
      // fetch an up-to-date master address from hyperspace
      update_master_address();
      // try again
      continue;
    }
    if (fail == false)
      break;
  }

  ScopedLock lock(m_mutex);
  m_fragments.erase(fragment);
}

void
ReplicationSlave::update_master_address() {
  uint64_t handle = 0;
  DynamicBuffer value;

  String master_address;
 
  master_address = m_context->get_properties()->get_str("Hypertable.Replication.Slave.MasterAddress");
  if (master_address.empty()) {
    for (int i = 0; i < 3; i++) {
      try {
        HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr,
                m_context->get_hyperspace(), &handle);
  
        String path = m_context->get_toplevel_dir() + "/replication/master";
        handle = m_context->get_hyperspace()->open(path, OPEN_FLAG_READ);
        m_context->get_hyperspace()->attr_get(handle, "location", value);
        master_address = (const char *)value.base;
      }
      catch (Exception &e) {
        if (i == 2)
          HT_FATAL_OUT << e << HT_END;
        else {
          HT_ERROR_OUT << e << HT_END;
          sleep(10);
        }
      }
    }
  }

  m_master_address = InetAddr(master_address.c_str());
  HT_INFOF("Replication.Master address is %s",
           m_master_address.format().c_str());

  // TODO make timeout configurable
  m_context->get_connection_manager()->add(m_master_address, 10000,
          "Replication.Master");
}

void
ReplicationSlave::initialize_slave_id() {
  // command line parameter has highest priority
  m_location = m_context->get_properties()->get_str(
          "Hypertable.Replication.Slave.ProxyName");
  boost::trim(m_location);
  if (m_location.size() > 0) {
    HT_INFOF("This slave's location is %s", m_location.c_str());
    return;
  }

  // then check run/repslave-location
  String run_directory = m_context->get_properties()->get_str(
          "Hypertable.DataDirectory") + "/run";
  if (!FileUtils::exists(run_directory))
    FileUtils::mkdirs(run_directory);
  String location_file = run_directory + "/repslave-location";

  // if the file already exists: read it and return
  if (FileUtils::exists(location_file)) {
    if (FileUtils::read(location_file, m_location) <= 0)
      HT_FATAL("Unable to read run/repslave-location or file is empty");
    boost::trim(m_location);
    HT_INFOF("This slave's location is %s", m_location.c_str());
    return;
  }

  // does not exist? then create a new one based on the hostname/port
  char location_hash[33];
  md5_string(format("%s:%u",
              System::net_info().host_name.c_str(), m_port).c_str(),
          location_hash);
  m_location = format("rs-%s", String(location_hash).substr(0, 8).c_str());
  boost::trim(m_location);
  HT_INFOF("This slave's new location is %s", m_location.c_str());

  // store it in the local filesystem
  if (FileUtils::write(location_file, m_location) <= 0)
    HT_FATAL("Unable to write run/repslave-location");
}

void
ReplicationSlave::lock_slave_id() {
  String parent = m_context->get_toplevel_dir() + "/replication";
  String path = parent + "/" + m_location;
  Hyperspace::SessionPtr &hspace = m_context->get_hyperspace();

  // create the file and lock it exclusively
  uint32_t lock_status;
  Hyperspace::LockSequencer sequencer;
  uint32_t flags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE
      | OPEN_FLAG_CREATE | OPEN_FLAG_LOCK;

  try {
    hspace->mkdir(parent.c_str());
  }
  catch (Exception &ex) {
    if (ex.code() != Error::HYPERSPACE_FILE_EXISTS)
      throw;
  }

  uint64_t handle = hspace->open(path.c_str(), flags);
  while (true) {
    lock_status = 0;

    hspace->try_lock(handle, LOCK_MODE_EXCLUSIVE, &lock_status, &sequencer);
    if (lock_status == LOCK_STATUS_GRANTED)
      break;

    HT_INFOF("Waiting for exclusive lock on hyperspace:/%s ...", path.c_str());
    sleep(5);
  }
}

} // namespace Hypertable
