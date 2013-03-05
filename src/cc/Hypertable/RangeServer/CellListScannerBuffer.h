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
 * class used to hold a buffer of cells t
 */

#ifndef HYPERTABLE_CELLLISTSCANNERBUFFER_H
#define HYPERTABLE_CELLLISTSCANNERBUFFER_H

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
    CellListScannerBuffer(ScanContextPtr &scan_ctx) : CellListScanner(scan_ctx);
    virtual ~CellListScannerBuffer() { return; }

    virtual void forward();
    virtual bool get(Key &key, ByteString &value);
  private:
    
  };

  /// Smart pointer to CellListScannerBuffer
  typedef boost::intrusive_ptr<CellListScannerBuffer> CellListScannerBufferPtr;
  /** @}*/
}

#endif // HYPERTABLE_CELLLISTSCANNERBUFFER_H

