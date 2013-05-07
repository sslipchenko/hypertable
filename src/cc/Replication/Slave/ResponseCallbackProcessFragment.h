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

#ifndef HYPERTABLE_RESPONSECALLBACKPROCESSFRAGMENT_H
#define HYPERTABLE_RESPONSECALLBACKPROCESSFRAGMENT_H

#include <boost/shared_array.hpp>

#include "Common/Error.h"

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/ResponseCallback.h"

namespace Hypertable {

  class ResponseCallbackProcessFragment : public ResponseCallback {
  public:
    ResponseCallbackProcessFragment(Comm *comm, EventPtr &event_ptr)
      : ResponseCallback(comm, event_ptr) { }

    int response_ok(const String &fragment) {
      CommHeader header;
      header.initialize_from_request_header(m_event->header);
      CommBufPtr cbp(new CommBuf(header,
                  4 + Serialization::encoded_length_vstr(fragment)));
      cbp->append_i32(Error::OK);
      cbp->append_vstr(fragment);
      return m_comm->send_response(m_event->addr, cbp);
    }
  };

}


#endif // HYPERTABLE_RESPONSECALLBACKPROCESSFRAGMENT_H
