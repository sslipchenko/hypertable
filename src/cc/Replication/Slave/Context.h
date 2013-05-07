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
#include "Hypertable/Lib/ClusterIdManager.h"
#include "Hypertable/Lib/ClusterConfiguration.h"

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

      m_app_queue = new ApplicationQueue(10);
      m_cluster_config = new ClusterConfiguration(m_properties);
      m_cluster_id_mgr = new ClusterIdManager(m_hyperspace, m_properties);
      m_dfs = new DfsBroker::Client(m_conn_manager, m_properties);
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

    FilesystemPtr &get_dfs() {
      return m_dfs;
    }

    ClientPtr &get_client() {
      return m_client;
    }

    ClusterIdManagerPtr &get_cluster_id_mgr() {
      return m_cluster_id_mgr;
    }

    ClusterConfigurationPtr &get_cluster_cfg() {
      return m_cluster_config;
    }

  private:
    Comm *m_comm;
    PropertiesPtr m_properties;
    ConnectionManagerPtr m_conn_manager;
    Hyperspace::SessionPtr m_hyperspace;
    String m_toplevel_dir;
    ApplicationQueuePtr m_app_queue;
    FilesystemPtr m_dfs;
    ClientPtr m_client;
    ClusterConfigurationPtr m_cluster_config;
    ClusterIdManagerPtr m_cluster_id_mgr;
};

typedef intrusive_ptr<Context> ContextPtr;

} // namespace Hypertable

#endif // REPLICATION_CONTEXT_H
