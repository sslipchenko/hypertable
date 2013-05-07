/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License.
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
#include "Common/InetAddr.h"
#include "Common/ScopeGuard.h"
#include "Common/SystemInfo.h"
#include "Common/md5.h"

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "ClusterIdManager.h"

#include <vector>

using namespace boost;
using namespace Hyperspace;


namespace Hypertable {

ClusterIdManager::ClusterIdManager(Hyperspace::SessionPtr &hyperspace,
        PropertiesPtr &props)
  : m_hyperspace(hyperspace), m_properties(props), m_local_id(0) {
}

uint64_t
ClusterIdManager::assign_new_local_id() {
  ScopedLock lock(m_mutex);
  uint64_t handle = 0;
  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &handle);

  // open the "master" file in hyperspace
  String toplevel_dir = m_properties->get_str("Hypertable.Directory");
  handle = m_hyperspace->open(toplevel_dir + "/master",
          OPEN_FLAG_READ | OPEN_FLAG_WRITE);

  // Set cluster ID to Master listen address string + timestamp
  uint16_t port = m_properties->get_i16("Hypertable.Master.Port");
  InetAddr addr(System::net_info().primary_addr, port);
  String tmp = addr.format() + Hypertable::format("%u", (unsigned)time(0));
  m_local_id = (uint64_t)md5_hash(tmp.c_str());
  tmp = Hypertable::format("%llu", (long long unsigned int)m_local_id);

  m_hyperspace->attr_set(handle, "cluster_id", tmp.c_str(), tmp.length());
  return m_local_id;
}

bool
ClusterIdManager::load_from_hyperspace() {
  ScopedLock lock(m_mutex);
  String toplevel_dir = m_properties->get_str("Hypertable.Directory");

  try {
    uint64_t handle = 0;
    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &handle);

    // open the "master" file in hyperspace
    handle = m_hyperspace->open(toplevel_dir + "/master", OPEN_FLAG_READ);

    // get the "cluster_id" attribute
    DynamicBuffer buf;
    m_hyperspace->attr_get(handle, "cluster_id", buf);
    String cluster_id((const char *)buf.base, buf.fill());
    m_local_id = (uint64_t)strtoull(cluster_id.c_str(), 0, 0);
  }
  catch (Exception &ex) {
    // attribute not found? then the cluster ID was not yet assigned
    if (ex.code() != Error::HYPERSPACE_ATTR_NOT_FOUND)
      HT_FATAL_OUT << ex << HT_END;
  }

  if (m_local_id != 0)
    return true;

  return false;
}

} // namespace Hypertable
