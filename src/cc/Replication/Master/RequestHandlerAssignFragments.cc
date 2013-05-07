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

#include "RequestHandlerAssignFragments.h"
#include "ReplicationMaster.h"

using namespace Hypertable;
using namespace Serialization;

void RequestHandlerAssignFragments::run() {
  ResponseCallback cb(m_comm, m_event);
  const uint8_t *decode_ptr = m_event->payload;
  size_t decode_remain = m_event->payload_len;

  try {
    StringSet fragments;
    StringSet::iterator fit;
    ReplicationTypes::TableIdToClusterMap tables
        = ReplicationMaster::get_instance()->get_replicated_tables();
    ReplicationTypes::TableIdToClusterMap::iterator tit;
    ReplicationTypes::ClusterToSlavesMap clusters;
    ReplicationTypes::ClusterToSlavesMap::iterator cit;
    ReplicationTypes::TableIdToTableNameMap &table_names
        = ReplicationMaster::get_instance()->get_table_names();

    // get the location id of the slave
    const char *location = decode_vstr(&decode_ptr, &decode_remain);
    // get the public address of the remote slave
    uint16_t slave_port = decode_i16(&decode_ptr, &decode_remain);
    InetAddr slave_addr(ntohl(m_event->addr.sin_addr.s_addr), slave_port);

    ReplicationMaster::get_instance()->assign_fragments(location,
            slave_addr, fragments, clusters);

    CommHeader header;
    header.initialize_from_request_header(m_event->header);

    // first calculate the length for the whole packet
    size_t length = 4;
    for (tit = tables.begin(); tit != tables.end(); tit++) {
      length += 4 + Serialization::encoded_length_vstr(tit->first)
          + Serialization::encoded_length_vstr(table_names[tit->first]);
      for (std::vector<String>::iterator vit = tit->second.begin();
          vit != tit->second.end(); ++vit)
        length += Serialization::encoded_length_vstr(*vit);
    }

    length += 4;
    for (fit = fragments.begin(); fit != fragments.end(); fit++) {
      HT_INFOF("assigning fragment to %s: %s", slave_addr.format().c_str(),
               fit->c_str());
      length += Serialization::encoded_length_vstr(*fit);
    }

    length += 4;
    for (cit = clusters.begin(); cit != clusters.end(); cit++) {
      length += 4 + Serialization::encoded_length_vstr(cit->first);
      std::vector<InetAddr> &vec = cit->second;
      for (std::vector<InetAddr>::iterator it = vec.begin(); it != vec.end();
              ++it)
        length += it->encoded_length();
    }

    // now create the packet
    CommBufPtr cbp(new CommBuf(header, length + 4));
    cbp->append_i32(0); // success

    cbp->append_i32(tables.size());
    for (tit = tables.begin(); tit != tables.end(); tit++) {
      cbp->append_vstr(tit->first);
      cbp->append_vstr(table_names[tit->first]);
      cbp->append_i32(tit->second.size());
      HT_INFOF("Appending %lu clusters for table id %s ('%s')",
              tit->second.size(), tit->first.c_str(),
              table_names[tit->first].c_str());
      for (std::vector<String>::iterator vit = tit->second.begin();
          vit != tit->second.end(); ++vit)
        cbp->append_vstr(*vit);
    }

    cbp->append_i32(fragments.size());
    for (fit = fragments.begin(); fit != fragments.end(); fit++)
      cbp->append_vstr(*fit);

    cbp->append_i32(clusters.size());
    for (cit = clusters.begin(); cit != clusters.end(); cit++) {
      cbp->append_vstr(cit->first);
      std::vector<InetAddr> &vec = cit->second;
      HT_INFOF("Appending %lu slaves for cluster %s", vec.size(),
              cit->first.c_str());
      cbp->append_i32(vec.size());
      for (std::vector<InetAddr>::iterator it = vec.begin(); it != vec.end();
              ++it)
        it->encode(cbp->get_data_ptr_address());
    }

    int error = m_comm->send_response(m_event->addr, cbp);
    if (error != Error::OK)
      HT_THROW(error, Error::get_text(error));
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), e.what());
    return;
  }
}
