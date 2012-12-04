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
#include "Common/md5.h"

#include "PhantomRange.h"

using namespace Hypertable;
using namespace std;

PhantomRange::PhantomRange(const QualifiedRangeSpec &spec,
                           const RangeState &state,
                           SchemaPtr &schema,
                           const vector<uint32_t> &fragments) 
  : m_range_spec(spec), m_range_state(state), m_schema(schema),
    m_outstanding(fragments.size()), m_state(LOADED) {
  foreach_ht(uint32_t fragment, fragments) {
    HT_ASSERT(m_fragments.count(fragment) == 0);
    FragmentDataPtr data = new FragmentData(fragment);
    m_fragments[fragment] = data;
  }
}

int PhantomRange::get_state() {
  ScopedLock lock(m_mutex);
  return m_state;
}

bool PhantomRange::add(uint32_t fragment, EventPtr &event) {
  ScopedLock lock(m_mutex);
  FragmentMap::iterator it = m_fragments.find(fragment);

  HT_ASSERT(it != m_fragments.end());

  if (it->second->complete()) {
    // fragment is already complete
    return false;
  }
  else {
    HT_ASSERT(m_outstanding);
    it->second->add(event);
  }
  return true;
}

void PhantomRange::purge_incomplete_fragments() {
  ScopedLock lock(m_mutex);
  FragmentMap::iterator it = m_fragments.begin();
  for(; it != m_fragments.end(); ++it)
    if (!it->second->complete())
      it->second->clear();
}

void PhantomRange::create_range(MasterClientPtr &master_client, 
        TableInfoPtr &table_info, FilesystemPtr &log_dfs, String &log_dir) { 
  ScopedLock lock(m_mutex);

  m_range = new Range(master_client, &m_range_spec.table, m_schema,
                      &m_range_spec.range, table_info.get(), &m_range_state, true);
  // replay existing transfer log
  m_range->recovery_finalize();
  m_range->metalog_entity()->state.state =
    m_range->metalog_entity()->state.state | RangeState::PHANTOM;
  m_range_state.state |= RangeState::PHANTOM;
}

void PhantomRange::populate_range_and_log(FilesystemPtr &log_dfs, 
        const String &log_dir, bool *is_empty) {
  ScopedLock lock(m_mutex);
  *is_empty = true;

  MetaLog::EntityRange *metalog_entity = m_range->metalog_entity();

  if ((metalog_entity->state.state & ~RangeState::PHANTOM) == RangeState::SPLIT_SHRUNK) {
    m_phantom_logname = metalog_entity->state.transfer_log;
    HT_INFO_OUT << "Using transfer log " << m_phantom_logname
                << " as phantom log for range " << m_range_spec
                << " because in SPLIT_SHURNK state" << HT_END;
  }
  else {
    char md5DigestStr[33];
    CommitLogPtr phantom_log;

    /**
     * Create phantom log
     */
    md5_trunc_modified_base64(metalog_entity->spec.end_row, md5DigestStr);
    md5DigestStr[16] = 0;
    time_t now = 0;

    do {
      if (now != 0)
        poll(0, 0, 1200);
      now = time(0);
      m_phantom_logname = log_dir + "/" + metalog_entity->table.id + "/" +
        md5DigestStr + "-" + (int)now;
    } while (log_dfs->exists(m_phantom_logname));

    log_dfs->mkdirs(m_phantom_logname);

    phantom_log = new CommitLog(log_dfs, m_phantom_logname, false);
    size_t table_id_len = m_range_spec.table.encoded_length();
    DynamicBuffer dbuf(table_id_len);
    int64_t latest_revision;

    Locker<Range> range_lock(*(m_range.get()));
    foreach_ht (FragmentMap::value_type &vv, m_fragments) {
      dbuf.clear();
      m_range_spec.table.encode(&dbuf.ptr);
      vv.second->merge(m_range, dbuf, &latest_revision);
      if (dbuf.fill() > table_id_len) {
        *is_empty = false;
        phantom_log->write(dbuf, latest_revision, false);
      }
    }
    phantom_log->close();

    metalog_entity->state.set_transfer_log(m_phantom_logname);

    HT_INFO_OUT << "Created phantom log " << m_phantom_logname
                << " for range " << m_range_spec << HT_END;
  }

  m_phantom_log = new CommitLogReader(log_dfs, m_phantom_logname);
  // Scan log blocks to set latest revision
  BlockCompressionHeaderCommitLog header;
  const uint8_t *base;
  size_t len;
  while (m_phantom_log->next(&base, &len, &header))
    ;
  *is_empty = m_phantom_log->get_latest_revision() == TIMESTAMP_MIN;

}

const String & PhantomRange::get_phantom_logname() {
  ScopedLock lock(m_mutex);
  return m_phantom_logname;
}

CommitLogReaderPtr PhantomRange::get_phantom_log() {
  ScopedLock lock(m_mutex);
  return m_phantom_log;
}

void PhantomRange::set_replayed() {
  ScopedLock lock(m_mutex);
  HT_ASSERT((m_state & REPLAYED) == 0);
  m_state |= REPLAYED;
}

bool PhantomRange::replayed() {
  ScopedLock lock(m_mutex);
  return (m_state & REPLAYED) == REPLAYED;
}

void PhantomRange::set_prepared() {
  ScopedLock lock(m_mutex);
  HT_ASSERT((m_state & PREPARED) == 0);
  m_state |= PREPARED;
}

bool PhantomRange::prepared() {
  ScopedLock lock(m_mutex);
  return (m_state & PREPARED) == PREPARED;
}

void PhantomRange::set_committed() {
  ScopedLock lock(m_mutex);
  HT_ASSERT((m_state & COMMITTED) == 0);
  m_state |= COMMITTED;
}

bool PhantomRange::committed() {
  ScopedLock lock(m_mutex);
  return (m_state & COMMITTED) == COMMITTED;
}
