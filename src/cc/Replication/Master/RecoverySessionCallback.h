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

#ifndef REPLICATION_RECOVERYSESSIONCALLBACK_H
#define REPLICATION_RECOVERYSESSIONCALLBACK_H

#include "Hyperspace/HandleCallback.h"

#include "ReplicationMaster.h"


namespace Hypertable {

class RecoverySessionCallback : public Hyperspace::HandleCallback {
  public:
    RecoverySessionCallback(ContextPtr &context, const String &location)
      : Hyperspace::HandleCallback(Hyperspace::EVENT_MASK_LOCK_RELEASED),
        m_context(context), m_location(location), m_handle(0) {
    }

    virtual void lock_released() {
      HT_INFOF("Replication.Slave %s lost its hyperspace lock",
            m_location.c_str());
      ReplicationMaster::get_instance()->handle_disconnect(m_location);
    }

    void set_handle(uint64_t handle) {
      m_handle = handle;
    }

  private:
    ContextPtr m_context;
    String m_location;
    uint64_t m_handle;
};

} // namespace Hypertable

#endif // REPLICATION_RECOVERYSESSIONCALLBACK_H
