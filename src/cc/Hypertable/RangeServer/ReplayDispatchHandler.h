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

#ifndef HYPERTABLE_REPLAYHANDLER_H
#define HYPERTABLE_REPLAYHANDLER_H

#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/xtime.hpp>

#include <map>

#include "Common/Mutex.h"

#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/Comm.h"

#include "Hypertable/Lib/Types.h"
#include "Hypertable/Lib/RangeServerClient.h"

namespace Hypertable {

  /**
   */
  class ReplayDispatchHandler : public DispatchHandler {

  public:
    ReplayDispatchHandler(Comm *comm, const String &location, 
                          int plan_generation, int32_t timeout_ms) :
        m_rsclient(comm, timeout_ms), m_recover_location(location),
        m_plan_generation(plan_generation), m_timeout_ms(timeout_ms),
        m_outstanding(0) { }

    virtual void handle(EventPtr &event_ptr);

    void add(const CommAddress &addr, const QualifiedRangeSpec &range,
             uint32_t fragment, bool more, StaticBuffer &buffer);

    bool has_errors() {
      ScopedLock lock(m_mutex);
      return (m_range_errors.size()>0 || m_location_errors.size()>0);
    }

    void get_error_locations(vector<String> &locations);
    void get_error_ranges(vector<QualifiedRangeSpec> &ranges);
    void get_completed_ranges(set<QualifiedRangeSpec> &ranges);
    bool wait_for_completion();

  private:
    Mutex         m_mutex;
    boost::condition m_cond;
    RangeServerClient m_rsclient;
    String m_recover_location;
    int m_plan_generation;
    int32_t m_timeout_ms;
    size_t        m_outstanding;
    map<QualifiedRangeSpecManaged, int32_t> m_range_errors;
    map<String, int32_t> m_location_errors;
    set<QualifiedRangeSpecManaged> m_completed_ranges;
  };
}

#endif // HYPERSPACE_REPLAYHANDLER_H

