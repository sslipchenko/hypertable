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

#include "ReplicationMaster.h"
#include "RequestHandlerFinishedFragment.h"

using namespace Hypertable;
using namespace Serialization;

void
RequestHandlerFinishedFragment::run() {
  ResponseCallback cb(m_context->get_comm(), m_event);
  const uint8_t *decode_ptr = m_event->payload; 
  size_t decode_remain = m_event->payload_len;

  try {
    int error = decode_i32(&decode_ptr, &decode_remain);
    String fragment = decode_vstr(&decode_ptr, &decode_remain);
    uint64_t offset = decode_i64(&decode_ptr, &decode_remain);
    int links = decode_i32(&decode_ptr, &decode_remain);

    HT_INFOF("Finished replication of fragment %s (error %d, offset %lld, "
            "%d links)", fragment.c_str(), error, (Lld)offset, links);

    std::vector<String> linked_logs;
    for (int i = 0; i < links; i++)
      linked_logs.push_back(decode_vstr(&decode_ptr, &decode_remain));

    ReplicationMaster::get_instance()->fragment_finished(fragment, error,
            offset, linked_logs);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), e.what());
  }
  cb.response_ok();
}