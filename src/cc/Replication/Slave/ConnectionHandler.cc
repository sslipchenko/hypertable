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
#include "Common/Logger.h"
#include "Common/Serialization.h"
#include "Common/Time.h"

#include "AsyncComm/Event.h"
#include "AsyncComm/ResponseCallback.h"

#include "Hypertable/Lib/ReplicationSlaveProtocol.h"

#include "ConnectionHandler.h"
#include "RequestHandlerStatus.h"
#include "RequestHandlerUpdate.h"

using namespace Hypertable;
using namespace Serialization;
using namespace Error;

ConnectionHandler::ConnectionHandler(ContextPtr &context)
  : m_context(context) {
}

void ConnectionHandler::handle(EventPtr &event) {
  if (event->type == Event::MESSAGE) {
    ApplicationHandler *handler = 0;

    try {
      // sanity check command code
      if (event->header.command < 0
          || event->header.command >= ReplicationSlaveProtocol::COMMAND_MAX)
        HT_THROWF(PROTOCOL_ERROR, "Invalid command (%llu)",
                  (Llu)event->header.command);

      switch (event->header.command) {
        case ReplicationSlaveProtocol::COMMAND_STATUS:
          handler = new RequestHandlerStatus(m_context, event);
          break;
        case ReplicationSlaveProtocol::COMMAND_UPDATE:
          handler = new RequestHandlerUpdate(m_context, event);
          break;
        case ReplicationSlaveProtocol::COMMAND_SHUTDOWN:
          HT_INFO("Received ReplicationSlaveProtocol::COMMAND_SHUTDOWN");
          _exit(0);
          break;
        default:
          HT_THROWF(PROTOCOL_ERROR, "Unimplemented command (%llu)",
                  (Llu)event->header.command);
      }
      if (handler)
        m_context->get_app_queue()->add(handler);
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
    }
    return;
  }
  // ignore disconnects
  // TODO or not? we could mark this slave as "bad" and stop propagating him
  // to the remote clusters
  else if (event->type == Event::DISCONNECT) {
    return;
  }

  HT_WARNF("Unhandled event %s", event->to_str().c_str());
}

