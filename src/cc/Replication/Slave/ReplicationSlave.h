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

#ifndef REPLICATION_SLAVE_H
#define REPLICATION_SLAVE_H

#include "Common/Runnable.h"

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"

#include "Hypertable/Lib/ReplicationTypes.h"
#include "Hypertable/Lib/ReplicationMasterClient.h"

#include "Context.h"

namespace Hypertable {

class ReplicationSlave : public ReferenceCount {
  public:
    typedef std::map<String, String> ReplicatedTables;

    ReplicationSlave(ContextPtr &context)
      : m_context(context) {

      HT_ASSERT(ms_instance == 0);
      ms_instance = this;

      m_port = context->get_properties()->get_i16("port");
      initialize_slave_id();
      lock_slave_id();
      update_master_address();
      update_state();
    }

    // this is a singleton class
    static ReplicationSlave *get_instance() {
      HT_ASSERT(ms_instance != 0);
      return ms_instance;
    }

    // ask the Replication.Master for an updated state
    void update_state();

    // inform the Replication.Master that a fragment was finished
    void finished_fragment(const String &fragment, int error, uint64_t offset,
            const std::vector<String> &linked_logs);

    // retrieve list of currently known clusters and their slaves
    void get_clusters(ReplicationTypes::ClusterToSlavesMap &clusters) {
      ScopedLock lock(m_mutex);
      clusters = m_clusters;
    }

  private:
    // read or create this slave's location name
    void initialize_slave_id();

    // lock this slave's location in Hyperspace
    void lock_slave_id();

    // fetch the address of this cluster's Replication.Master from hyperspace
    void update_master_address();

    Mutex m_mutex;
    ContextPtr m_context;

    // list of all fragments that this slave replicates
    StringSet m_fragments;

    // map of remote clusters and their slaves
    ReplicationTypes::ClusterToSlavesMap m_clusters;

    // map of tables and their destination
    ReplicationTypes::TableIdToClusterMap m_table_ids;

    // map of table IDs and their actual names
    ReplicationTypes::TableIdToTableNameMap m_table_names;

    // the Master's address
    InetAddr m_master_address;

    // local port of this service
    uint16_t m_port;

    // this slave's location id
    String m_location;

    // static singleton instance
    static ReplicationSlave *ms_instance;
};

typedef intrusive_ptr<ReplicationSlave> ReplicationSlavePtr;

} // namespace Hypertable

#endif // REPLICATION_SLAVE_H
