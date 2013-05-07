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

#ifndef REPLICATION_REQUESTHANDLER_NOTIFYSCHEMAUPDATE_H
#define REPLICATION_REQUESTHANDLER_NOTIFYSCHEMAUPDATE_H

#include "Common/Runnable.h"

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"

#include "ReplicationMaster.h"

namespace Hypertable {

class RequestHandlerNotifySchemaUpdate : public ApplicationHandler {
  public:
    RequestHandlerNotifySchemaUpdate(ContextPtr &context, EventPtr &event_ptr)
      : ApplicationHandler(event_ptr), m_context(context) {
    }

    virtual void run() {
      ResponseCallback cb(m_context->get_comm(), m_event);
      const uint8_t *decode_ptr = m_event->payload;
      size_t decode_remain = m_event->payload_len;
      int what = 0;
      const char *table_name = 0;
      const char *table_id = 0;
      const char *schema = 0;

      try {
        what = Serialization::decode_i32(&decode_ptr, &decode_remain);
        table_name = Serialization::decode_vstr(&decode_ptr, &decode_remain);
        table_id = Serialization::decode_vstr(&decode_ptr, &decode_remain);
        schema = Serialization::decode_vstr(&decode_ptr, &decode_remain);

        ReplicationMaster::get_instance()->handle_schema_update(
                what, table_id, table_name, schema);
      }
      catch (Exception &ex) {
        HT_ERROR_OUT << ex << HT_END;
        cb.error(ex.code(), ex.what());
        return;
      }
      cb.response_ok();
    }

  private:
    ContextPtr m_context;
};

} // namespace Hypertable

#endif // REPLICATION_REQUESTHANDLER_NOTIFYSCHEMAUPDATE_H
