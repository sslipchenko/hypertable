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

#include "RangeServerConnectionManager.h"

using namespace Hypertable;
using namespace std;

RangeServerConnectionManager::RangeServerConnectionManager()
  : conn_count(0) {
  comm = Comm::instance();
  m_server_list_iter = m_server_list.end();
}

void RangeServerConnectionManager::add_server(RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  pair<Sequence::iterator, bool> insert_result = m_server_list.push_back( RangeServerConnectionEntry(rsc) );

  if (!insert_result.second) {
    HT_INFOF("Tried to insert %s host=%s local=%s public=%s",
            rsc->location().c_str(), rsc->hostname().c_str(),
            rsc->local_addr().format().c_str(),
            rsc->public_addr().format().c_str());
    for (Sequence::iterator iter = m_server_list.begin();
            iter != m_server_list.end(); ++iter) {
      HT_INFOF("Contains %s host=%s local=%s public=%s",
              iter->location().c_str(), iter->hostname().c_str(),
              iter->local_addr().format().c_str(),
              iter->public_addr().format().c_str());
    }
    HT_ASSERT(insert_result.second);
  }
}

bool RangeServerConnectionManager::connect_server(RangeServerConnectionPtr &rsc,
        const String &hostname, InetAddr local_addr, InetAddr public_addr) {
  ScopedLock lock(mutex);
  LocationIndex &hash_index = m_server_list.get<1>();
  LocationIndex::iterator iter;

  bool retval = false;
  bool notify = false;

  comm->set_alias(local_addr, public_addr);
  comm->add_proxy(rsc->location(), hostname, public_addr);
  HT_INFOF("Registered proxy %s", rsc->location().c_str());

  if (rsc->connect(hostname, local_addr, public_addr)) {
    conn_count++;
    if (conn_count == 1)
      notify = true;
    retval = true;
  }

  if (m_server_list_iter != m_server_list.end() &&
      m_server_list_iter->location() == rsc->location())
    ++m_server_list_iter;

  // Remove this connection if already exists
  iter = hash_index.find(rsc->location());
  if (iter != hash_index.end())
    hash_index.erase(iter);

  // NOTE: If this assert is still here on 4/1/2013, remove it!
  {
    PublicAddrIndex &public_addr_index = m_server_list.get<3>();
    PublicAddrIndex::iterator public_addr_iter = 
      public_addr_index.find(rsc->public_addr());
    HT_ASSERT(public_addr_iter == public_addr_index.end());
  }

  // Add it (or re-add it)
  pair<Sequence::iterator, bool> insert_result = m_server_list.push_back(RangeServerConnectionEntry(rsc));
  HT_ASSERT(insert_result.second);
  if (m_server_list.size() == 1 || m_server_list_iter == m_server_list.end())
    m_server_list_iter = m_server_list.begin();

  if (notify)
    cond.notify_all();

  return retval;
}

bool RangeServerConnectionManager::disconnect_server(RangeServerConnectionPtr &rsc) {
  if (rsc->disconnect()) {
    HT_ASSERT(conn_count > 0);
    conn_count--;
    return true;
  }
  return false;
}

void RangeServerConnectionManager::wait_for_server() {
  ScopedLock lock(mutex);
  while (conn_count == 0)
    cond.wait(lock);
}

bool RangeServerConnectionManager::find_server_by_location(const String &location,
        RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  return find_server_by_location_unlocked(location, rsc);
}

bool RangeServerConnectionManager::find_server_by_location_unlocked(const String &location, RangeServerConnectionPtr &rsc) {
  LocationIndex &hash_index = m_server_list.get<1>();
  LocationIndex::iterator lookup_iter;

  if ((lookup_iter = hash_index.find(location)) == hash_index.end()) {
    //HT_DEBUG_OUT << "can't find server with location=" << location << HT_END;
    //for (Sequence::iterator iter = m_server_list.begin(); iter != m_server_list.end(); ++iter) {
    //  HT_DEBUGF("Contains %s host=%s local=%s public=%s", iter->location().c_str(),
    //           iter->hostname().c_str(), iter->local_addr().format().c_str(),
    //           iter->public_addr().format().c_str());
    //}
    rsc = 0;
    return false;
  }
  rsc = lookup_iter->rsc;
  return true;
}


bool RangeServerConnectionManager::find_server_by_hostname(const String &hostname,
        RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  HostnameIndex &hash_index = m_server_list.get<2>();

  pair<HostnameIndex::iterator, HostnameIndex::iterator> p
      = hash_index.equal_range(hostname);
  if (p.first != p.second) {
    rsc = p.first->rsc;
    if (++p.first == p.second)
      return true;
    rsc = 0;
  }
  return false;
}

bool RangeServerConnectionManager::find_server_by_public_addr(InetAddr addr,
        RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  PublicAddrIndex &hash_index = m_server_list.get<3>();
  PublicAddrIndex::iterator lookup_iter;

  if ((lookup_iter = hash_index.find(addr)) == hash_index.end()) {
    rsc = 0;
    return false;
  }
  rsc = lookup_iter->rsc;
  return true;
}

bool RangeServerConnectionManager::find_server_by_local_addr(InetAddr addr,
        RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  LocalAddrIndex &hash_index = m_server_list.get<4>();

  for (pair<LocalAddrIndex::iterator, LocalAddrIndex::iterator> p
          = hash_index.equal_range(addr);
       p.first != p.second; ++p.first) {
    if (p.first->connected()) {
      rsc = p.first->rsc;
      return true;
    }
  }
  return false;
}

void RangeServerConnectionManager::erase_server(RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  LocationIndex &hash_index = m_server_list.get<1>();
  LocationIndex::iterator iter;
  PublicAddrIndex &public_addr_index = m_server_list.get<3>();
  PublicAddrIndex::iterator public_addr_iter;

  // Remove this connection if already exists
  iter = hash_index.find(rsc->location());
  if (iter != hash_index.end())
    hash_index.erase(iter);
  public_addr_iter = public_addr_index.find(rsc->public_addr());
  if (public_addr_iter != public_addr_index.end())
    public_addr_index.erase(public_addr_iter);
  // reset server list iter
  m_server_list_iter = m_server_list.begin();

  rsc->set_removed();
}

bool RangeServerConnectionManager::next_available_server(RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);

  if (m_server_list.empty())
    return false;

  if (m_server_list_iter == m_server_list.end())
    m_server_list_iter = m_server_list.begin();

  ServerList::iterator saved_iter = m_server_list_iter;

  do {
    ++m_server_list_iter;
    if (m_server_list_iter == m_server_list.end())
      m_server_list_iter = m_server_list.begin();
    if (m_server_list_iter->rsc->connected()
            && !m_server_list_iter->rsc->is_recovering()) {
      rsc = m_server_list_iter->rsc;
      return true;
    }
  } while (m_server_list_iter != saved_iter);

  return false;
}

bool RangeServerConnectionManager::is_connected(const String &location) {
  RangeServerConnectionPtr rsc;
  if (find_server_by_location(location, rsc))
    rsc->connected();
  return false;
}

void RangeServerConnectionManager::get_servers(std::vector<RangeServerConnectionPtr> &servers) {
  ScopedLock lock(mutex);
  for (ServerList::iterator iter = m_server_list.begin(); iter != m_server_list.end(); ++iter) {
    if (!iter->removed())
      servers.push_back(iter->rsc);
  }
}

size_t RangeServerConnectionManager::connected_server_count() {
  ScopedLock lock(mutex);
  size_t count=0;
  for (ServerList::iterator iter = m_server_list.begin(); iter != m_server_list.end(); ++iter) {
    if (!iter->removed() && iter->connected())
      ++count;
  }
  return count;
}
void RangeServerConnectionManager::get_connected_servers(std::vector<RangeServerConnectionPtr> &servers) {
  ScopedLock lock(mutex);
  for (ServerList::iterator iter = m_server_list.begin(); iter != m_server_list.end(); ++iter) {
    if (!iter->removed() && iter->connected())
      servers.push_back(iter->rsc);
  }
}

void RangeServerConnectionManager::get_connected_servers(StringSet &locations) {
  ScopedLock lock(mutex);
  for (ServerList::iterator iter = m_server_list.begin(); iter != m_server_list.end(); ++iter) {
    if (!iter->removed() && iter->connected())
      locations.insert(iter->location());
  }
}

void RangeServerConnectionManager::get_unbalanced_servers(const std::vector<String> &locations,
    std::vector<RangeServerConnectionPtr> &unbalanced) {
  ScopedLock lock(mutex);
  LocationIndex &hash_index = m_server_list.get<1>();
  LocationIndex::iterator lookup_iter;
  RangeServerConnectionPtr rsc;

  foreach_ht(const String &location, locations) {
    if ((lookup_iter = hash_index.find(location)) == hash_index.end())
      continue;
    rsc = lookup_iter->rsc;
    if (!rsc->get_removed() && !rsc->get_balanced())
      unbalanced.push_back(rsc);
  }
}

void RangeServerConnectionManager::set_servers_balanced(const std::vector<RangeServerConnectionPtr> &unbalanced) {
  ScopedLock lock(mutex);
  foreach_ht (const RangeServerConnectionPtr rsc, unbalanced) {
    rsc->set_balanced();
  }
}
