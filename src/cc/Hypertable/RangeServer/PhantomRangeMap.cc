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

#include "Hypertable/Lib/Types.h"

#include "PhantomRangeMap.h"

using namespace std;
using namespace Hypertable;

namespace {

  enum {
    PREPARED  = 0x00000001,
    COMMITTED = 0x00000002
  };

}

PhantomRangeMap::PhantomRangeMap() : m_state(0), m_load_in_progress(false),
                 m_prepare_in_progress(false), m_commit_in_progress(false) {
  m_tableinfo_map = new TableInfoMap();
}

void PhantomRangeMap::insert(const QualifiedRangeStateSpec &range_spec, SchemaPtr &schema,
    const vector<uint32_t> &fragments) {
  ScopedLock lock(m_mutex);
  PhantomRangePtr phantom_range = new PhantomRange(range_spec, schema, fragments);
  const QualifiedRangeStateSpec &qrs = phantom_range->get_qualified_range_state_spec();
  MapInsRec ins_rec;
  ins_rec = m_map.insert(make_pair(qrs, phantom_range));
  HT_ASSERT(ins_rec.second);
}

void PhantomRangeMap::get(const QualifiedRangeStateSpec &range, SchemaPtr &schema,
    const vector<uint32_t> &fragments, PhantomRangePtr &phantom_range) {
  ScopedLock lock(m_mutex);
  Map::iterator it = m_map.find(range);
  if (it == m_map.end()) {
    phantom_range = new PhantomRange(range, schema, fragments);
    QualifiedRangeStateSpec qrs = phantom_range->get_qualified_range_state_spec();
    MapInsRec ins_rec;
    ins_rec = m_map.insert(make_pair(qrs, phantom_range));
    HT_ASSERT(ins_rec.second);
  }
  else
    phantom_range = it->second;
  return;
}

void PhantomRangeMap::get(const QualifiedRangeStateSpec &range, PhantomRangePtr &phantom_range) {
  ScopedLock lock(m_mutex);
  Map::iterator it = m_map.find(range);
  if (it == m_map.end())
    phantom_range = 0;
  else
    phantom_range = it->second;
}

void PhantomRangeMap::remove(const QualifiedRangeStateSpec &range) {
  ScopedLock lock(m_mutex);
  m_map.erase(range);
}

void PhantomRangeMap::remove(const QualifiedRangeSpec &range) {
  ScopedLock lock(m_mutex);
  QualifiedRangeStateSpec qrss(range);
  m_map.erase(qrss);
}

void PhantomRangeMap::get_all(vector<PhantomRangePtr> &range_vec) {
  ScopedLock lock(m_mutex);
  foreach_ht(Map::value_type &vv, m_map)
    range_vec.push_back(vv.second);
}

size_t PhantomRangeMap::size() {
  ScopedLock lock(m_mutex);
  return m_map.size();
}

void PhantomRangeMap::load_start() {
  ScopedLock lock(m_mutex);
  while (m_load_in_progress)
    m_load_cond.wait(lock);
  m_load_in_progress = true;
}

void PhantomRangeMap::load_finish() {
  ScopedLock lock(m_mutex);
  m_load_in_progress = false;
  m_load_cond.notify_all();
}

bool PhantomRangeMap::prepare_start() {
  ScopedLock lock(m_mutex);
  if ((m_state & PREPARED) == PREPARED || m_prepare_in_progress)
    return false;
  m_prepare_in_progress = true;
  return true;
}

void PhantomRangeMap::prepare_abort() {
  ScopedLock lock(m_mutex);
  m_prepare_in_progress = false;
}


void PhantomRangeMap::prepare_finish() {
  ScopedLock lock(m_mutex);
  m_prepare_in_progress = false;
  m_state |= PREPARED;
}

bool PhantomRangeMap::is_prepared() {
  ScopedLock lock(m_mutex);
  return (m_state & PREPARED) == PREPARED;
}


bool PhantomRangeMap::commit_start() {
  ScopedLock lock(m_mutex);
  if ((m_state & COMMITTED) == COMMITTED || m_commit_in_progress)
    return false;
  m_commit_in_progress = true;
  return true;
}

void PhantomRangeMap::commit_abort() {
  ScopedLock lock(m_mutex);
  m_commit_in_progress = false;
}


void PhantomRangeMap::commit_finish() {
  ScopedLock lock(m_mutex);
  m_commit_in_progress = false;
  m_state |= COMMITTED;
}

bool PhantomRangeMap::is_committed() {
  ScopedLock lock(m_mutex);
  return (m_state & COMMITTED) == COMMITTED;
}


void Hypertable::call_load_finish(PhantomRangeMapPtr phantom_range_map) {
  phantom_range_map->load_finish();
}
