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

#ifndef REPLICATION_REQUESTHANDLER_PROCESS_FRAGMENT_H
#define REPLICATION_REQUESTHANDLER_PROCESS_FRAGMENT_H

#include "Common/Runnable.h"

#include <queue>

#include "AsyncComm/ApplicationHandler.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ResponseCallback.h"

#include "Hypertable/Lib/BlockCompressionCodec.h"
#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/CommitLogBlockStream.h"
#include "Hypertable/Lib/CommitLogReader.h"
#include "Hypertable/Lib/ReplicationSlaveClient.h"

#include "Context.h"
#include "ReplicationSlave.h"

namespace Hypertable {

class RequestHandlerProcessFragment : public ApplicationHandler {
  public:
    RequestHandlerProcessFragment(EventPtr &event,
            ContextPtr context, const String &fragment,
            const ReplicationTypes::TableIdToClusterMap &table_ids,
            const ReplicationTypes::TableIdToTableNameMap &table_names,
            const ReplicationTypes::ClusterToSlavesMap &clusters)
      : ApplicationHandler(event), m_context(context),
        m_table_ids(table_ids), m_table_names(table_names),
        m_clusters(clusters), m_eof(false),
        m_compressor_type(BlockCompressionCodec::COMPRESSION_TYPE_LIMIT) {
      m_local_cluster_id = m_context->get_cluster_id_mgr()->get_local_id();
      m_fragment_queue.push(fragment);
    }

    virtual void run();

  private:
    int replicate_fragment(const String &fragment);

    void load_compressor(uint16_t ztype);

    void process_fragment(CommitLogBlockStream *stream,
            BlockCompressionHeaderCommitLog *header,
            CommitLogBlockInfo *info);

    void process_block(uint8_t *base, size_t len);

    void send_to_slave(const String &table, const DynamicBuffer &dbuf,
            const String &dest_cluster);

    ContextPtr m_context;
    String m_cur_fragment;
    ReplicationTypes::TableIdToClusterMap m_table_ids;
    ReplicationTypes::TableIdToTableNameMap m_table_names;
    ReplicationTypes::ClusterToSlavesMap m_clusters;
    BlockCompressionCodecPtr m_compressor;
    bool m_eof;
    uint16_t m_compressor_type;
    std::map<String, ReplicationSlaveClientPtr> m_clients;
    uint64_t m_local_cluster_id;
    std::queue<String> m_fragment_queue;
    std::vector<String> m_linked_logs;
};

} // namespace Hypertable

#endif // REPLICATION_REQUESTHANDLER_PROCESS_FRAGMENT_H
