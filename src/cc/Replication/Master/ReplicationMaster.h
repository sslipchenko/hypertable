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

#ifndef REPLICATION_MASTER_H
#define REPLICATION_MASTER_H

#include <map>

#include "Common/Runnable.h"

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"

#include "Hypertable/Lib/ReplicationTypes.h"
#include "Hypertable/Lib/MetaLogWriter.h"

#include "Context.h"

namespace Hypertable {

class ReplicationMaster : public ReferenceCount {
  public:
    ReplicationMaster(ContextPtr &context)
      : m_context(context), m_master_handle(0) {
      HT_ASSERT(ms_instance == 0);
      ms_instance = this;
      m_server_dir = m_context->get_toplevel_dir() + "/servers";

      if (m_context->get_properties()->get_bool("Hypertable.Replication.TestMode"))
        return;
      obtain_master_lock();
      read_from_metalog();
    }

    ~ReplicationMaster() {
      if (m_master_handle) {
        m_context->get_hyperspace()->close(m_master_handle);
        m_master_handle = 0;
      }
    }

    // this is a singleton class
    static ReplicationMaster *get_instance() {
      HT_ASSERT(ms_instance != 0);
      return ms_instance;
    }

    // handles a schema update and forwards it to the remote clusters
    void handle_schema_update(int what, const char *table_id,
            const char *table_name, const char *schema);

    // scan the DFS for new commit logs and fragments
    void scan_dfs();

    // update list of remote recipients
    void update_recipients();

    // assign fragments to a slave
    void assign_fragments(const char *location,
            const InetAddr &remote_slave_addr, StringSet &fragments,
            ReplicationTypes::ClusterToSlavesMap &clusters);

    // when a schema update is received from a remote cluster: apply it
    void apply_schema_update(int what, const String &table_name,
            const String &schema);

    // update the receivers list for a specific cluster
    void update_receivers(const String &cluster,
            const std::vector<InetAddr> &list);

    // retrieve a list of receivers for this cluster
    void get_slaves(std::set<InetAddr> &slaves) {
      ScopedLock lock(m_mutex);
      slaves.clear();
      for (std::map<String, InetAddr>::iterator it = m_known_slaves.begin();
         it != m_known_slaves.end(); ++it)
        slaves.insert(it->second);
    }

    // get the list of all replicated tables
    const ReplicationTypes::TableIdToClusterMap &get_replicated_tables() {
      ScopedLock lock(m_mutex);
      return m_replicated_tables;
    }

    // get the mapping of table IDs to table names
    ReplicationTypes::TableIdToTableNameMap &get_table_names() {
      ScopedLock lock(m_mutex);
      return m_table_names;
    }

    // a fragment was replicated successfully
    void fragment_finished(String &fragment, int error, uint64_t offset,
            const std::vector<String> &linked_logs);

    // a remote Replication.Slave disconnected; reassign its fragments
    void handle_disconnect(const String &location);

  private:
    // helper function which scans a single directory
    void scan_directory(const String &directory, StringSet &fragments);

    // removes *.purged in the specified directory
    void remove_purged_files(const String &directory);

    // recursively removes all subdirectories with a "purged-directory"
    // marker
    bool remove_purged_directories(const String &directory,
            const StringSet &linked_logs,
            const ReplicationTypes::TableIdToClusterMap &replicated_tables,
            int level = 0, int basedir_length = 0);

    // this function places a lock on /hypertable/replication
    void obtain_master_lock();

    // reads the current state from the metalog
    void read_from_metalog();

    // writes the current state to the metalog
    void write_to_metalog();

    // registers a new incoming connection
    void register_new_slave(const char *location,
            const InetAddr &slave_address);

    Mutex m_mutex;
    ContextPtr m_context;
    // hyperspace master handle
    int64_t m_master_handle;
    // maps table id to cluster name
    ReplicationTypes::TableIdToClusterMap m_replicated_tables;
    // maps table id to table name
    ReplicationTypes::TableIdToTableNameMap m_table_names;
    // All known (remote) clusters
    StringSet m_known_clusters;
    // All known transfer log directories that we have to include in our scan
    StringSet m_linked_logs;
    // All known (local) slaves of this cluster
    std::map<String, InetAddr> m_known_slaves;
    // All unassigned fragments mapped to their cluster
    StringSet m_unassigned_fragments;
    // A list of DFS paths with fragments which were already assigned
    std::map<String, String> m_assigned_fragments;
    // All fragments that were finished; with extension .purged
    StringSet m_finished_fragments;
    // this map stores the last applied schema generation
    std::map<String, uint32_t> m_table_generations;
    // list of all known slaves of the remote clusters
    std::map<String, std::vector<InetAddr> > m_cluster_slaves;
    // this is where the state is persisted to
    MetaLog::WriterPtr m_mlwriter;
    // the DFS toplevel directory of /hypertable/servers
    String m_server_dir;

    // static singleton instance
    static ReplicationMaster *ms_instance;

    friend class ReplicationMasterEntity;
};

typedef intrusive_ptr<ReplicationMaster> ReplicationMasterPtr;

} // namespace Hypertable

#endif // REPLICATION_MASTER_H
