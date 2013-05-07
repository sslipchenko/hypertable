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

#ifndef REPLICATION_REQUESTHANDLER_UPDATE_H
#define REPLICATION_REQUESTHANDLER_UPDATE_H

#include "Common/Runnable.h"
#include "Common/FailureInducer.h"

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/Namespace.h"
#include "Hypertable/Lib/Table.h"
#include "Hypertable/Lib/TableMutator.h"

#include "Context.h"

namespace Hypertable {

class RequestHandlerUpdate : public ApplicationHandler {
  public:
    RequestHandlerUpdate(ContextPtr &context, EventPtr &event_ptr)
      : ApplicationHandler(event_ptr), m_context(context) {
    }

    virtual void run() {
      String ns_path, tbl_name;
      const char *cfg = "Hypertable.Replication.BaseNamespace";
      if (m_context->get_properties()->has(cfg))
        ns_path = m_context->get_properties()->get_str(cfg);

      ResponseCallback cb(m_context->get_comm(), m_event);
      try {
        const uint8_t *decode_ptr = m_event->payload;
        size_t decode_remain = m_event->payload_len;
        const char *full_table_name = Serialization::decode_vstr(&decode_ptr,
              &decode_remain);

        HT_MAYBE_FAIL("Replication.Slave-Update-2");

        // open namespace and table, then create a mutator
        ns_path += Filesystem::dirname(full_table_name);
        tbl_name = Filesystem::basename(full_table_name);
        NamespacePtr ns = m_context->get_client()->open_namespace(ns_path);
        TablePtr table = ns->open_table(tbl_name);
        SchemaPtr schema = table->schema();
        TableMutator *mutator = table->create_mutator();

        // now add all the keys
        uint32_t count = Serialization::decode_i32(&decode_ptr, &decode_remain);

        HT_DEBUGF("Table %s: updating %u keys", full_table_name, count);

        for (uint32_t i = 0; i < count; i++) {
          SerializedKey serkey;
          ByteString value;

          // extract the key
          serkey.ptr = decode_ptr;
          decode_ptr += serkey.length();
  
          // extract the value
          value.ptr = decode_ptr;
          decode_ptr += value.length();

          // add key/value pair to buffer
          Key key;
          key.load(serkey);
          Schema::ColumnFamily *cf
              = schema->get_column_family(key.column_family_code);
          KeySpec spec(key.row, cf ? cf->name.c_str() : "",
                  key.column_qualifier, key.timestamp, key.flag);

          //if (i == 0)
            //HT_INFO_OUT << "XXX " << key.row << HT_END;

          if (key.flag < KEYSPEC_DELETE_MAX)
            mutator->set_delete(spec);
          else {
            const uint8_t *p = value.ptr;
            size_t len = value.decode_length(&p);
            mutator->set(spec, value.str(), len);
          }
        }

        // close (and flush) mutator
        delete mutator;
      }
      catch (Exception &ex) {
        HT_ERROR_OUT << "Failed inserting keys (ns_path=" << ns_path
            << ", tbl_name=" << tbl_name << "): " << ex << HT_END;
        cb.error(ex.code(), ex.what());
        return;
      }

      cb.response_ok();
      HT_MAYBE_FAIL("Replication.Slave-Update-1");
    }

  private:
    ContextPtr m_context;
};

} // namespace Hypertable

#endif // REPLICATION_REQUESTHANDLER_UPDATE_H
