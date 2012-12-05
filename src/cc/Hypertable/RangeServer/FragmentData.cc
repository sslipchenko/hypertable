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

#include "FragmentData.h"

using namespace std;
using namespace Hypertable;

void FragmentData::add(EventPtr &event) {
  m_data.push_back(event);
  return;
}

void FragmentData::merge(RangePtr &range, const char *split_point,
                         DynamicBuffer &dbuf_lower, int64_t *latest_revision_lower, bool add_lower,
                         DynamicBuffer &dbuf_upper, int64_t *latest_revision_upper, bool add_upper) {
  String location;
  QualifiedRangeSpec range_spec;
  DeserializedFragments fragments;
  Key key;
  SerializedKey serkey;
  ByteString value;
  int plan_generation;
  DynamicBuffer *dbufp;
  size_t total_size = 0;

  *latest_revision_lower = TIMESTAMP_MIN;
  *latest_revision_upper = TIMESTAMP_MIN;

  // de-serialize all objects
  foreach_ht(EventPtr &event, m_data) {
    const uint8_t *decode_ptr = event->payload;
    size_t decode_remain = event->payload_len;
    location = Serialization::decode_vstr(&decode_ptr, &decode_remain);
    plan_generation = Serialization::decode_i32(&decode_ptr, &decode_remain);
    range_spec.decode(&decode_ptr, &decode_remain);
    // skip "fragment"
    (void)Serialization::decode_i32(&decode_ptr, &decode_remain);

    total_size += decode_remain;
    uint8_t *base = (uint8_t *)decode_ptr;
    size_t size = decode_remain;
    fragments.push_back(Fragment(base, size));
  }

  // resize the buffers
  dbuf_lower.ensure(total_size);
  dbuf_upper.ensure(total_size);

  foreach_ht(Fragment &fragment, fragments) {
    const uint8_t *mod, *mod_end;

    mod_end = fragment.first + fragment.second;
    mod = fragment.first;

    while (mod < mod_end) {
      serkey.ptr = mod;
      value.ptr = mod + serkey.length();
      HT_ASSERT(serkey.ptr <= mod_end && value.ptr <= mod_end);
      HT_ASSERT(key.load(serkey));
      if (strcmp(key.row, split_point) <= 0) {
        if (key.revision > *latest_revision_lower)
          *latest_revision_lower = key.revision;
        if (add_lower)
          range->add(key, value);
        dbufp = &dbuf_lower;
      }
      else {
        if (key.revision > *latest_revision_upper)
          *latest_revision_upper = key.revision;
        if (add_upper)
          range->add(key, value);
        dbufp = &dbuf_upper;
      }
      // skip to next kv pair
      value.next();
      dbufp->add_unchecked((const void *)mod, value.ptr-mod);
      mod = value.ptr;
    }
  }
}
