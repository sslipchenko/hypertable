/*
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

/** @file
 * Type declarations for BlockCompressionHeaderCommitLog class.
 * This file contains the type declarations for BlockCompressionHeaderCommitLog,
 * a class representing a commit log block header.
 */

#ifndef HYPERTABLE_BLOCKCOMPRESSIONHEADERCOMMITLOG_H
#define HYPERTABLE_BLOCKCOMPRESSIONHEADERCOMMITLOG_H

#include "BlockCompressionHeader.h"

namespace Hypertable {

  /** @addtogroup libHypertable
   * @{
   */

  /**
   * Encodes and decodes a commit log block header.
   */
  class BlockCompressionHeaderCommitLog : public BlockCompressionHeader {

  public:

    /** Constructor. */
    BlockCompressionHeaderCommitLog();

    /** Constructor.
     * @param magic Pointer to 10-character magic string
     * @param revision Latest revision of cells in block
     * @param cluster_id Cluster ID of originating cluster
     */
    BlockCompressionHeaderCommitLog(const char *magic, int64_t revision,
            uint64_t cluster_id);

    /** Gets revision number of newest cell in block.
     * @param revision Revision number of newest cell in block
     */
    void set_revision(int64_t revision) { m_revision = revision; }

    /** Gets revision number of newest cell in block.
     * @return Revision number of newest cell in block
     */
    int64_t get_revision() { return m_revision; }

    /** Sets Cluster ID
     * @param cluster_id Cluster ID of originating cluster
     */
    void set_cluster_id(uint64_t cluster_id) { m_cluster_id = cluster_id; }
    
    /** Gets Cluster ID
     * @return Cluster ID of originating cluster
     */
    uint64_t get_cluster_id() { return m_cluster_id; }

    /** Returns length of serizlied block header.
     * If legacy block, then the length is BlockCompressionHeader::LENGTH plus
     * 8 bytes for the revision number.  If non-legacy block, then 8 bytes is
     * also added for the cluster ID.
     * @return Length of serizlied block header.
     */
    virtual size_t length();

    /** Writes (encodes) serialized header to memory location.
     * This method encodes the header in a serialized format to the location
     * pointed to by <code>*bufp</code>.  The base header fields are first
     * encoded with a call to BlockCompressionHeader::encode, and then the
     * commit log specific fields are encoded as follows:
     * <table style="font-family: 'Courier New', monospace;">
     *   <tr><td>latest revision</td> <td> 8 byte integer </td></tr>
     *   <tr><td>cluster ID</td>      <td> 8 byte integer </td></tr>
     *   <tr><td>checksum</td>        <td> 2 byte integer </td></tr>
     * </table>
     * This method only computes and writes the final checksum field if after
     * @param bufp Address of pointer to destination location (advanced by call)
     */
    virtual void encode(uint8_t **bufp);

    /** Reads (decodes) serailized header from memory location.
     * See encode() for details on the serialization format.
     * @param bufp Address of pointer to serialized header (advanced by call)
     * @param remainp Address of remaining byte count (decremented by call)
     */
    virtual void decode(const uint8_t **bufp, size_t *remainp);

  private:

    /** Determines if block is a <i>legacy</i> block.
     * A legacy block does not include the cluster ID and has a magic
     * string that does not end with the character '2'.
     * @return <i>true</i> if block is legacy, <i>false</i> otherwise
     */
    bool is_legacy() { return (get_magic()[9] != '2'); }

    /// Revision number of newest cell in block
    int64_t m_revision;

    /// Cluster ID of originating cluster
    uint64_t m_cluster_id;
  };

  /* @} */
}

#endif // HYPERTABLE_BLOCKCOMPRESSIONHEADERCOMMITLOG_H
