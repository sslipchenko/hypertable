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

#ifndef HYPERTABLE_TIMERHANDLER_H
#define HYPERTABLE_TIMERHANDLER_H

#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>

#include "Common/Mutex.h"
#include "Common/Time.h"

#include "Context.h"

namespace Hypertable {

class TimerHandler : public DispatchHandler {
  public:
    TimerHandler(ContextPtr &context);

    virtual void handle(Hypertable::EventPtr &event_ptr);

  private:
    Mutex m_mutex;
    ContextPtr m_context;
    int32_t m_timer_interval;
  };

  typedef boost::intrusive_ptr<TimerHandler> TimerHandlerPtr;
}

#endif // HYPERTABLE_TIMERHANDLER_H

