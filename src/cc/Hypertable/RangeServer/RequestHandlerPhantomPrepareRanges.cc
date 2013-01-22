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

#include "AsyncComm/ResponseCallback.h"
#include "Common/Serialization.h"

#include "Hypertable/Lib/Types.h"
#include "RangeServer.h"
#include "RequestHandlerPhantomPrepareRanges.h"

using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
void RequestHandlerPhantomPrepareRanges::run() {
  ResponseCallback cb(m_comm, m_event);
  int64_t op_id;
  String location;
  vector<QualifiedRangeSpec> ranges;
  QualifiedRangeSpec range;
  int plan_generation;
  uint32_t nn;

  const uint8_t *decode_ptr = m_event->payload;
  size_t decode_remain = m_event->payload_len;

  try {
    op_id     = Serialization::decode_i64(&decode_ptr, &decode_remain);
    location  = Serialization::decode_vstr(&decode_ptr, &decode_remain);
    plan_generation = Serialization::decode_i32(&decode_ptr, &decode_remain);
    nn        = Serialization::decode_i32(&decode_ptr, &decode_remain);
    for(uint32_t ii = 0; ii < nn; ++ii) {
      range.decode(&decode_ptr, &decode_remain);
      ranges.push_back(range);
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), e.what());
    return;
  }

  try {
    m_range_server->phantom_prepare_ranges(&cb, op_id, location, plan_generation, ranges);
  }
  catch (Exception &e) {
    HT_FATAL_OUT << e << HT_END;
  }
}
