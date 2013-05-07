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

#ifndef REPLICATION_CONTEXT_H
#define REPLICATION_CONTEXT_H

#include <boost/algorithm/string.hpp>

#include "Common/Properties.h"
#include "Common/ReferenceCount.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/ApplicationQueue.h"

#include "DfsBroker/Lib/Client.h"

#include "Hyperspace/Session.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/ClusterConfiguration.h"
#include "Hypertable/Lib/ReplicationMasterClient.h"

namespace Hypertable {

class Context : public ReferenceCount {
  public:
    Context(PropertiesPtr &properties)
      : m_comm(Comm::instance()), m_properties(properties) {
      m_conn_manager = new ConnectionManager(m_comm);
      m_client = new Hypertable::Client();
      m_hyperspace = m_client->get_hyperspace_session();

      m_toplevel_dir = properties->get_str("Hypertable.Directory");
      boost::trim_if(m_toplevel_dir, boost::is_any_of("/"));
      m_toplevel_dir = String("/") + m_toplevel_dir;

      m_app_queue = new ApplicationQueue(20);
      m_dfs = new DfsBroker::Client(m_conn_manager, m_properties);
      m_cluster_config = new ClusterConfiguration(m_properties);
    }

    PropertiesPtr &get_properties() {
      return m_properties;
    }

    ConnectionManagerPtr &get_connection_manager() {
      return m_conn_manager;
    }

    Hyperspace::SessionPtr &get_hyperspace() {
      return m_hyperspace;
    }

    const String &get_toplevel_dir() {
      return m_toplevel_dir;
    }

    Comm *get_comm() {
      return m_comm;
    }

    ApplicationQueuePtr &get_app_queue() {
      return m_app_queue;
    }

    ClusterConfigurationPtr &get_cluster_config() {
      return m_cluster_config;
    }

    FilesystemPtr &get_dfs() {
      return m_dfs;
    }

    ClientPtr &get_client() {
      return m_client;
    }

    ReplicationMasterClient *get_master_client(const String &name) {
      ScopedLock lock(m_mutex);
      std::map<String, ReplicationMasterClientPtr>::iterator it;
      it = m_master_clients.find(name);
      if (it == m_master_clients.end()) {
        ReplicationMasterClientPtr client = new ReplicationMasterClient(name,
                m_cluster_config);
        std::vector<InetAddr> addresses;
        client->get_cluster_addresses(addresses);
        foreach_ht (const InetAddr &addr, addresses) {
          HT_INFOF("Connecting to remote Replication.Master %s",
                   addr.format().c_str());
          m_conn_manager->add(addr, 5000, "Replication.Master");
        }
        m_master_clients[name] = client;
        return client.get();
      }
      return it->second.get();
    }

  private:
    Mutex m_mutex;
    Comm *m_comm;
    PropertiesPtr m_properties;
    ConnectionManagerPtr m_conn_manager;
    Hyperspace::SessionPtr m_hyperspace;
    String m_toplevel_dir;
    ApplicationQueuePtr m_app_queue;
    FilesystemPtr m_dfs;
    ClusterConfigurationPtr m_cluster_config;
    ClientPtr m_client;
    std::map<String, ReplicationMasterClientPtr> m_master_clients;
};

typedef intrusive_ptr<Context> ContextPtr;

} // namespace Hypertable

#endif // REPLICATION_CONTEXT_H
