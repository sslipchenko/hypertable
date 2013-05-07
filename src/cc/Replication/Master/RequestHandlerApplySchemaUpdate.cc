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
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Filesystem.h"
#include "Common/Serialization.h"

#include "AsyncComm/ResponseCallback.h"

#include "RequestHandlerApplySchemaUpdate.h"
#include "ReplicationMaster.h"

using namespace Hypertable;
using namespace Serialization;

void
RequestHandlerApplySchemaUpdate::run() {
  ResponseCallback cb(m_context->get_comm(), m_event);
  const uint8_t *decode_ptr = m_event->payload;
  size_t decode_remain = m_event->payload_len;
  int what = 0;
  const char *table_name = 0;
  const char *schema = 0;

  try {
    what = decode_i32(&decode_ptr, &decode_remain);
    table_name = decode_vstr(&decode_ptr, &decode_remain);
    schema = decode_vstr(&decode_ptr, &decode_remain);

    HT_INFOF("Schema update: what=%d, table=%s, schema=%s",
            what, table_name, schema);

    ReplicationMaster::get_instance()->apply_schema_update(what, table_name,
            schema);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), e.what());
    return;
  }
  cb.response_ok();
}
