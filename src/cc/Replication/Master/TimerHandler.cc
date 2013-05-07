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
#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/System.h"
#include "Common/Time.h"

#include <algorithm>
#include <sstream>

#include "Common/Time.h"

#include "Context.h"
#include "TimerHandler.h"
#include "ReplicationMaster.h"

using namespace Hypertable;
using namespace Hypertable::Config;

TimerHandler::TimerHandler(ContextPtr &context)
  : m_context(context) {
  m_timer_interval = get_i32("Hypertable.Replication.Timer.Interval");

  int error;
  if ((error = m_context->get_comm()->set_timer(0, this)) != Error::OK)
    HT_FATALF("Problem setting timer - %s", Error::get_text(error));
}

void
TimerHandler::handle(Hypertable::EventPtr &event_ptr) {
  ScopedLock lock(m_mutex);

  try {
    // scan all fragments in /hypertable/servers/rs*/log/user
    ReplicationMaster::get_instance()->scan_dfs();

    // retrieve list of recipients from all remote clusters
    ReplicationMaster::get_instance()->update_recipients();
  }
  catch (Exception &ex) {
    HT_ERROR_OUT << ex << HT_END;
  }

  // reset the timer
  int error;
  if ((error = m_context->get_comm()->set_timer(m_timer_interval, this))
          != Error::OK)
    HT_FATALF("Problem setting timer - %s", Error::get_text(error));
}
