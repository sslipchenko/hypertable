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
#include "Common/Filesystem.h"
#include "Common/Error.h"
#include "Common/FailureInducer.h"

#include "Hypertable/Lib/CompressorFactory.h"

#include "RequestHandlerProcessFragment.h"

namespace Hypertable {

void
RequestHandlerProcessFragment::run() {
  int error = 0;
  bool first = true;

  if (m_fragment_queue.empty())
    return;

  String original_fragment = m_fragment_queue.front();

  do {
    String fragment = m_fragment_queue.front();
    m_fragment_queue.pop();

    m_eof = false;
    HT_INFOF("Processing fragment %s", fragment.c_str());
    error = replicate_fragment(fragment);

    // only send the finished_fragment() message to the Master after ALL
    // linked logs were transferred successfully. Only then the Master can
    // reassign this fragment (including all its linked fragments) in case
    // the replication fails (see test replication-basic-5).
    if (!first)
      ReplicationSlave::get_instance()->finished_fragment(fragment, 0, 0,
              m_linked_logs);
    else
      first = false;

    HT_MAYBE_FAIL("Replication.Slave-ProgressFragment-1");
  } while (!m_fragment_queue.empty());

  ReplicationSlave::get_instance()->finished_fragment(original_fragment,
            error, 0, m_linked_logs); // TODO offset
}

int
RequestHandlerProcessFragment::replicate_fragment(const String &fragment) {
  int error = 0;
  m_cur_fragment = fragment;

  // don't crash if there's an error because we frequently read wrong files
  // or try to seek behind eof
  CommitLogBlockStream::ms_assert_on_error = false;

  CommitLogBlockInfo info;
  BlockCompressionHeaderCommitLog header;

  try {
    CommitLogBlockStream stream(m_context->get_dfs(),
          Filesystem::dirname(m_cur_fragment),
          Filesystem::basename(m_cur_fragment), 
          false);
    while (!m_eof) {
      HT_INFOF("%s: offset=%u", m_cur_fragment.c_str(),
              (unsigned)stream.get_offset());
      process_fragment(&stream, &header, &info);

      if (m_eof) {
        HT_INFOF("%s: reached EOF", m_cur_fragment.c_str());
        break;
      }

      // TODO wait for minimum time window but not longer than allowed
      HT_INFOF("%s: Waiting 5000 milliseconds for new fragment data",
                m_cur_fragment.c_str());
      poll(0, 0, 5000);
    }

    HT_INFOF("Finished %s: offset=%u", m_cur_fragment.c_str(),
            (unsigned)stream.get_offset());
    // done!
    error = 0;
  }
  catch (Exception &ex) {
    HT_ERROR_OUT << m_cur_fragment << ": " << ex << HT_END;
    error = ex.code();
    if (error == Error::DFSBROKER_BAD_FILENAME
        || error == Error::DFSBROKER_FILE_NOT_FOUND) {
      if (boost::algorithm::ends_with(m_cur_fragment, ".purged")) {
        HT_ERRORF("File %s not found", m_cur_fragment.c_str());
      }
      else {
        HT_INFOF("File %s not found; Enqueueing purged file %s.purged",
                m_cur_fragment.c_str(), m_cur_fragment.c_str());
        m_fragment_queue.push(m_cur_fragment + ".purged");
      }
      return error;
    }
  }

  HT_INFOF("Finished processing fragment %s (error %d)", m_cur_fragment.c_str(),
          error);
  return error;
}

void
RequestHandlerProcessFragment::process_fragment(CommitLogBlockStream *stream,
            BlockCompressionHeaderCommitLog *header,
            CommitLogBlockInfo *info) {
  while (stream->next(info, header, true)) {
    HT_INFOF("%s: new block starting at "
             "position %lld, length %u", m_cur_fragment.c_str(),
             (Lld)info->start_offset, (unsigned)info->block_len);

    // reached end of file?
    if (header->check_magic(CommitLog::MAGIC_EOF)) {
      HT_INFOF("Fragment %s reached EOF", m_cur_fragment.c_str());
      m_eof = true;
      return;
    }

    // add linked fragments to the queue
    if (header->check_magic(CommitLog::MAGIC_LINK2)) {
      String log_dir = (const char *)(info->block_ptr + header->length());
      boost::trim_right_if(log_dir, boost::is_any_of("/"));
      HT_INFOF("Enqueueing linked fragments in %s", log_dir.c_str());
      m_linked_logs.push_back(log_dir);
      continue;
    }
    
    // skip all other unknown fragments
    if (!header->check_magic(CommitLog::MAGIC_DATA2)) {
      HT_INFO("invalid fragment block magic, skipping");
      continue;
    }

    // skip blocks that do not originate from the local cluster
    if (header->get_cluster_id() == 0
        || header->get_cluster_id() != m_local_cluster_id) {
      HT_INFOF("%s: cluster id %llu differs, skipping", m_cur_fragment.c_str(),
               (Llu)header->get_cluster_id());
      continue;
    }

    if (info->error != Error::OK) {
      HT_INFOF("Corruption detected in CommitLog fragment %s starting at "
               "position %lld - %s", m_cur_fragment.c_str(),
               (Lld)info->start_offset, Error::get_text(info->error));
      // try again later; maybe the file is incomplete
      return;
    }

    DynamicBuffer buffer;
    DynamicBuffer zblock(0, false);
    zblock.base = info->block_ptr;
    zblock.ptr = info->block_ptr + info->block_len;

    load_compressor(header->get_compression_type());
    m_compressor->inflate(zblock, buffer, *header);
    process_block(buffer.base, buffer.fill());
  }
}

void
RequestHandlerProcessFragment::process_block(uint8_t *base, size_t len) {
  const uint8_t *ptr = base;
  const uint8_t *end = base + len;
  TableIdentifier table_id;
  DynamicBuffer dbuf(len);
  SerializedKey key;
  ByteString value;

  // if table is NOT replicated: skip this block
  table_id.decode(&ptr, &len);
  HT_INFOF("Table id of fragment block: %s", table_id.id);
  if (m_table_ids.find(table_id.id) == m_table_ids.end()) {
    HT_INFOF("%s: Table %s is not replicated", m_cur_fragment.c_str(),
            table_id.id);
    return;
  }
  HT_INFOF("%s: Table %s is replicated", m_cur_fragment.c_str(),
          table_id.id);

  dbuf.ptr += 4;  // need room for number of items
  base = dbuf.ptr;

  size_t count = 0;
  while (ptr < end) {
    // extract the key
    key.ptr = ptr;
    ptr += key.length();
    if (ptr > end)
      HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding key");

    // extract the value
    value.ptr = ptr;
    ptr += value.length();
    if (ptr > end)
      HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding value");

    if (count == 0)
      HT_INFOF("%s XXX %s", m_cur_fragment.c_str(), key.row());

    // add key/value pair to buffer
    dbuf.add(key.ptr, ptr - key.ptr);

    count++;
  }

  base = dbuf.base;
  Serialization::encode_i32(&base, count);

  // now send the buffer to the remote slave(s)
  std::vector<String> &destinations = m_table_ids[table_id.id];
  foreach_ht (const String &dest, destinations) {
    HT_INFOF("%s: Sending replication block with %u keys (%u bytes) to "
            "destination %s (table '%s')", m_cur_fragment.c_str(),
            (unsigned)count, (unsigned)(dbuf.ptr - dbuf.base), dest.c_str(),
            m_table_names[table_id.id].c_str());
    send_to_slave(m_table_names[table_id.id], dbuf, dest);
  }
}

void
RequestHandlerProcessFragment::send_to_slave(const String &table,
        const DynamicBuffer &dbuf, const String &dest_cluster) {
  for (int errors = 0; errors < 3; errors++) {
    try {
      ReplicationSlaveClientPtr client = m_clients[dest_cluster];
      if (!client) {
        const std::vector<InetAddr> &addresses = m_clusters[dest_cluster];
        // if no addresses are available: update cluster addresses, then
        // try again
        if (addresses.empty()) {
          ReplicationSlave::get_instance()->get_clusters(m_clusters);
          HT_THROW(Error::REPLICATION_CLUSTER_NOT_FOUND, "No slaves available");
        }
        // TODO make timeout configurable
        foreach_ht(const InetAddr &a, addresses) {
          HT_INFOF("Initializing client address %s", a.format().c_str());
          m_context->get_connection_manager()->add(a, 10000, "Replication.Slave");
        }
        client = new ReplicationSlaveClient(addresses);
        m_clients[dest_cluster] = client;
      }
      HT_INFOF("Sending update for table %s to client %s", table.c_str(),
               client->get_current_address().format().c_str());
      client->update(table, dbuf);
      return;
    }
    catch (Exception &ex) {
      HT_ERROR_OUT << "Failed sending " << dbuf.fill() << " bytes of table "
          << table << " to cluster " << dest_cluster << ": " << ex << HT_END;
      if (ex.code() == Error::INDUCED_FAILURE || errors == 2)
        HT_THROW(ex.code(), "Failure limit exceeded");
    }

    poll(0, 0, 5000 + errors * 10000);
  }

  HT_ERROR_OUT << "Giving up sending data, too many errors" << HT_END;
}

void
RequestHandlerProcessFragment::load_compressor(uint16_t ztype) {
  BlockCompressionCodecPtr compressor_ptr;

  if (m_compressor != 0 && ztype == m_compressor_type)
    return;

  if (ztype >= BlockCompressionCodec::COMPRESSION_TYPE_LIMIT)
    HT_THROWF(Error::BLOCK_COMPRESSOR_UNSUPPORTED_TYPE,
              "Invalid compression type '%d'", (int)ztype);

  compressor_ptr = CompressorFactory::create_block_codec(
        (BlockCompressionCodec::Type)ztype);

  m_compressor_type = ztype;
  m_compressor = compressor_ptr.get();
}

} // namespace Hypertable

