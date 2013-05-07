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

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/ResponseCallback.h"

#include "RequestHandlerGetReceivers.h"
#include "ReplicationMaster.h"

namespace Hypertable {

void
RequestHandlerGetReceivers::run() {
  ResponseCallback cb(m_context->get_comm(), m_event);
  try {
    // retrieve the list of this cluster's slaves
    std::set<InetAddr> slaves;
    ReplicationMaster::get_instance()->get_slaves(slaves);
    HT_INFOF("Handling GetReceivers-request (%lu slaves)", slaves.size());

    // and send it back
    size_t length = 4;
    foreach_ht (const InetAddr &addr, slaves)
      length += addr.encoded_length();

    CommHeader header;
    header.initialize_from_request_header(m_event->header);
    CommBufPtr cbp(new CommBuf(header, length + 4));
    cbp->append_i32(0); // success
    cbp->append_i32(slaves.size());
    foreach_ht (const InetAddr &addr, slaves)
      addr.encode(cbp->get_data_ptr_address());

    int error = m_context->get_comm()->send_response(m_event->addr, cbp);
    if (error != Error::OK)
      HT_THROW(error, Error::get_text(error));
  }
  catch (Exception &ex) {
    HT_ERROR_OUT << ex << HT_END;
    cb.error(ex.code(), ex.what());
    return;
  }
  cb.response_ok();
}

} // namespace Hypertable
