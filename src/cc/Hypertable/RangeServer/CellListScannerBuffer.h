/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
 * Declarations for CellListScannerBuffer.
 * This file contains the type declarations for CellListScannerBuffer, a
 * class used to scan over a list of cells with no backing store (CellCache,
 * CellStore)
 */

#ifndef HYPERTABLE_CELLLISTSCANNERBUFFER_H
#define HYPERTABLE_CELLLISTSCANNERBUFFER_H

#include "Common/PageArena.h"
#include "Common/StlAllocator.h"

#include "CellListScanner.h"

namespace Hypertable {

  /** @addtogroup RangeServer
   * @{
   */

  /** Cell list scanner over a buffer of cells.  This concrete class if for
   * situations where a list of cells with no backing store (CellCache,
   * CellStore, etc.), needs to be returned via the CellListScanner
   * interface.
   */
  class CellListScannerBuffer : public CellListScanner {
  public:

    /** Constructor.
     * @param scan_ctx Reference to scan context
     */
    CellListScannerBuffer(ScanContextPtr &scan_ctx);

    /** Destructor. */
    virtual ~CellListScannerBuffer() { return; }

    /** Adds a key/value pair to the buffer.
     * @param key Serialized key
     * @param value ByteString pointer to value
     */
    void add(const SerializedKey key, const ByteString value);

    virtual void forward();
    virtual bool get(Key &key, ByteString &value);

  private:

    /** Sorts cells 
     */
    void initialize_for_scan();

    typedef std::pair<SerializedKey, ByteString> KeyValueT;
    typedef StlAllocator<KeyValueT> KeyValueAllocT;
    typedef std::vector<KeyValueT, KeyValueAllocT> KeyValueVectorT;

    /** STL Strict Weak Ordering for KeyValueT. */
    struct LtKeyValueT {
      bool operator()(const KeyValueT &kv1, const KeyValueT &kv2) const {
        return kv1.first < kv2.first;
      }
    };

    KeyValueVectorT m_cells;
    KeyValueVectorT::iterator m_iter;
    ByteArena m_arena;
    bool m_initialized_for_scan;
  };

  /// Smart pointer to CellListScannerBuffer
  typedef boost::intrusive_ptr<CellListScannerBuffer> CellListScannerBufferPtr;
  /** @}*/
}

#endif // HYPERTABLE_CELLLISTSCANNERBUFFER_H

