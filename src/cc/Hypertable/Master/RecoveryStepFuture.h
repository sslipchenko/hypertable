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

#ifndef HYPERTABLE_RECOVERYSTEPFUTURE_H
#define HYPERTABLE_RECOVERYSTEPFUTURE_H

#include <boost/thread/condition.hpp>

#include <set>
#include <vector>

#include "Common/ReferenceCount.h"
#include "Common/Time.h"
#include "Common/Timer.h"

namespace Hypertable {

  /**
   * Tracks outstanding RangeServer recover requests.
   */
  class RecoveryStepFuture : public ReferenceCount {
  public:

    RecoveryStepFuture(const String &label) : m_label(label) { }

    void register_locations(StringSet &locations) {
      ScopedLock lock(m_mutex);
      foreach_ht (const String &location, m_success)
        locations.erase(location);
      m_outstanding.clear();
      m_outstanding.insert(locations.begin(), locations.end());
    }

    void success(const String &location) {
      ScopedLock lock(m_mutex);

      if (m_outstanding.empty()) {
        m_cond.notify_all();
        return;
      }

      if (m_outstanding.count(location) > 0) {
        m_outstanding.erase(location);
        m_success.insert(location);
      }
      else if (m_success.count(location) == 0) {
        HT_INFOF("Received response from %s for recovery step %s, but not registered",
                 location.c_str(), m_label.c_str());
      }

      if (m_outstanding.empty())
        m_cond.notify_all();
    }

    bool wait_for_completion(Timer &timer) {
      ScopedLock lock(m_mutex);
      boost::xtime expire_time;

      timer.start();

      while (!m_outstanding.empty()) {
        boost::xtime_get(&expire_time, boost::TIME_UTC_);
        xtime_add_millis(expire_time, timer.remaining());
        if (!m_cond.timed_wait(lock, expire_time)) {
          if (!m_outstanding.empty()) {
            HT_WARN_OUT << "Recovery step " << m_label << " timed out" << HT_END;
            return false;
          }
        }
      }
      return true;
    }

  protected:
    Mutex m_mutex;
    boost::condition m_cond;
    String m_label;
    StringSet m_outstanding;
    StringSet m_success;
  };
  typedef intrusive_ptr<RecoveryStepFuture> RecoveryStepFuturePtr;
}

#endif // HYPERTABLE_RECOVERYSTEPFUTURE_H
