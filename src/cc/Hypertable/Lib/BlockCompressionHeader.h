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
 * Type declarations for BlockCompressionHeader class.
 * This file contains the type declarations for BlockCompressionHeader, a
 * base class representing a compressed block header.
 */

#ifndef HYPERTABLE_BLOCKCOMPRESSIONHEADER_H
#define HYPERTABLE_BLOCKCOMPRESSIONHEADER_H

namespace Hypertable {

  /** @addtogroup libHypertable
   * @{
   */

  /**
   * Base class for compressed block header.
   */
  class BlockCompressionHeader {
  public:

    /// Default header length
    static const size_t LENGTH = 26;

    /** Constructor. */
    BlockCompressionHeader()
      : m_data_length(0), m_data_zlength(0), m_data_checksum(0),
        m_compression_type((uint16_t)-1) {
      memset(m_magic, 0, sizeof(m_magic));
    }

    /** Constructor.
     * @param magic Pointer to 10-character magic string
     */
    BlockCompressionHeader(const char *magic)
      : m_data_length(0), m_data_zlength(0), m_data_checksum(0),
        m_compression_type((uint16_t)-1) {
      memcpy(m_magic, magic, 10);
    }

    /** Destructor. */
    virtual ~BlockCompressionHeader() { ; }

    /** Sets the 10-character "magic" string
     * @param magic Pointer to 10-character magic string
     */
    void set_magic(const char *magic) { memcpy(m_magic, magic, 10); }
    
    /** Sets the 10-character magic string
     * @return Pointer to 10-character magic string
     */
    const char *get_magic() { return (const char *)m_magic; }

    /** Checks if header's magic string matches <code>magic</code>
     * @param magic Pointer to 10-character magic string for comparison
     * @return <i>true</i> if magic string matches <code>magic</code>,
     * <i>false</i> otherwise.
     */
    bool check_magic(const char *magic) { return !memcmp(magic, m_magic, 10); }

    /** Sets the data length of the block.
     * @param length Uncompressed length of data in block
     */
    void set_data_length(uint32_t length) { m_data_length = length; }

    /** Gets the data length of the block.
     * @return Uncompressed length of data in block
     */
    uint32_t get_data_length() { return m_data_length; }

    /** Sets the compressed data length of the block.
    * @param length Compressed length of data in block
    */
    void set_data_zlength(uint32_t zlength) { m_data_zlength = zlength; }

    /** Gets the compressed data length of the block.
     * @return Compressed length of data in block
     */
    uint32_t get_data_zlength() { return m_data_zlength; }

    /** Sets the block data checksum.
     * @param checksum Block data checksum
     */
    void set_data_checksum(uint32_t checksum) { m_data_checksum = checksum; }

    /** Gets the block data checksum.
     * @return Block data checksum
     */
    uint32_t get_data_checksum() { return m_data_checksum; }

    /** Sets the block compression type.
     * @param type Block compression type (see BlockCompressionCodec::Type)
     */
    void set_compression_type(uint16_t type) { m_compression_type = type; }

    /** Gets the block compression type.
     * @return Block compression type (see BlockCompressionCodec::Type)
     */
    uint16_t get_compression_type() { return m_compression_type; }

    /** Length of serialized header.
     * @return Length of serialized header.
     */
    virtual size_t length() { return LENGTH; }

    /** Writes (encodes) serialized header to memory location.
     * This method encodes the header in a serialized format to the location
     * pointed to by <code>*bufp</code>.  The serialized format is as
     * follows:
     * <table style="font-family: 'Courier New', monospace;">
     *   <tr><td>magic string</td>        <td> 10 bytes </td></tr>
     *   <tr><td>header length</td>       <td> 1 byte </td></tr>
     *   <tr><td>compression type</td>    <td> 1 byte </td></tr>
     *   <tr><td>data checksum</td>       <td> 4 byte integer </td></tr>
     *   <tr><td>data length</td>         <td> 4 byte integer </td></tr>
     *   <tr><td>checksum (optional)</td> <td> 2 byte integer </td></tr>
     * </table>
     * This method only computes and writes the final checksum field if after
     * the data length has been written, there are only two more bytes remaining
     * to be serizlized.  This allows derived classes to append additional
     * header fields and compute the checksum over all of the fields.
     * @param bufp Address of pointer to destination location (advanced by call)
     */
    virtual void encode(uint8_t **bufp);

    /** Computes and writes header checksum.
     * This method computes a checksum over the bytes from <code>base</code> to
     * <code>*bufp</code> and writes the checksum as a two-byte integer starting
     * at *bufp.
     * @param base Beginning of serialized header memory
     * @param bufp Address of pointer to end of serialized header where
     * the checksum is to be written.
     */
    virtual void write_header_checksum(uint8_t *base, uint8_t **bufp);

    /** Reads (decodes) serailized header from memory location.
     * See encode() for details on the serialization format.
     * @param bufp Address of pointer to serialized header (advanced by call)
     * @param remainp Address of remaining byte count (decremented by call)
     */
    virtual void decode(const uint8_t **bufp, size_t *remainp);

  protected:

    /// Magic string used for locating block header
    char m_magic[10];

    /// Uncompressed length of data in block
    uint32_t m_data_length;

    /// Compressed length of data in block
    uint32_t m_data_zlength;

    /// Block data checksum
    uint32_t m_data_checksum;

    /// Block compression type (see BlockCompressionCodec::Type)
    uint16_t m_compression_type;
  };

  /* @} */

}

#endif // HYPERTABLE_BLOCKCOMPRESSIONHEADER_H

