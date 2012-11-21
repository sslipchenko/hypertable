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

#ifndef HYPERTABLE_PHANTOMRANGEMAP_H
#define HYPERTABLE_PHANTOMRANGEMAP_H

#include <map>
#include <string>

#include <boost/thread/condition.hpp>

#include "Common/PageArenaAllocator.h"
#include "Common/ReferenceCount.h"
#include "Common/Mutex.h"

#include "Hypertable/Lib/Types.h"

#include "TableInfoMap.h"
#include "PhantomRange.h"

namespace Hypertable {
  using namespace std;
  /**
   * Provides a mapping from table name to TableInfo object.
   */
  class PhantomRangeMap : public ReferenceCount {
  public:
    PhantomRangeMap();
    virtual ~PhantomRangeMap() { }

    TableInfoMapPtr get_tableinfo_map() { return m_tableinfo_map; }

    /**
     * Gets the phantom range if it is in map
     *
     * @param range range spec
     * @param phantom_range phantom range
     */
    void get(const QualifiedRangeSpec &range_spec, PhantomRangePtr &phantom_range);

    /**
     * Gets the phantom range, if not in map, insert first
     *
     * @param spec range spec
     * @param state range state
     * @param schema table schema
     * @param fragments fragments to be played
     * @param phantom_range phantom range
     */
    void get(const QualifiedRangeSpec &range, const RangeState &state, SchemaPtr &schema,
             const vector<uint32_t> &fragments, PhantomRangePtr &phantom_range);

    /**
     * Get vector of all phantom ranges in map
     * @param range_vec will contain all ranges in map
     */
    void get_all(std::vector<PhantomRangePtr> &range_vec);

    /**
     * Get number of phantom ranges in map
     *
     * @return number of phantom ranges in map
     */
    size_t size();

    void load_start();
    void load_finish();

    bool prepare_start();
    void prepare_abort();
    void prepare_finish();
    bool is_prepared();

    bool commit_start();
    void commit_abort();
    void commit_finish();
    bool is_committed();


  private:
    typedef std::map<QualifiedRangeSpec, PhantomRangePtr> Map;
    typedef std::pair<Map::iterator, bool> MapInsRec;

    Mutex            m_mutex;
    boost::condition m_load_cond;
    CharArena        m_arena;
    TableInfoMapPtr  m_tableinfo_map;
    Map              m_map;
    int              m_state;
    bool             m_load_in_progress;
    bool             m_prepare_in_progress;
    bool             m_commit_in_progress;
  };

  typedef boost::intrusive_ptr<PhantomRangeMap> PhantomRangeMapPtr;

  void call_load_finish(PhantomRangeMapPtr phantom_range_map);
}

#endif // HYPERTABLE_PHANTOMRANGEMAP_H
