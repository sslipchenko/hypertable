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

#ifndef REPLICATION_REQUESTHANDLERAPPLYSCHEMA_UPDATE_H
#define REPLICATION_REQUESTHANDLERAPPLYSCHEMA_UPDATE_H

#include "Common/Runnable.h"

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"

#include "Context.h"

namespace Hypertable {

class RequestHandlerApplySchemaUpdate : public ApplicationHandler {
  public:
    RequestHandlerApplySchemaUpdate(ContextPtr &context, EventPtr &event_ptr)
      : ApplicationHandler(event_ptr), m_context(context) {
    }

    virtual void run();

  private:
    ContextPtr m_context;
};

} // namespace Hypertable

#endif // REPLICATION_REQUESTHANDLERSCHEMA_UPDATE_H
