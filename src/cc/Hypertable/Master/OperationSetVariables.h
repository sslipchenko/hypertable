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
 * Declarations for OperationSetVariables.
 * This file contains declarations for OperationSetVariables, an Operation class
 * for setting system state variables.
 */

#ifndef HYPERTABLE_OPERATIONSETVARIABLES_H
#define HYPERTABLE_OPERATIONSETVARIABLES_H

#include "Common/StringExt.h"

#include "Hypertable/Lib/SystemVariable.h"

#include "Operation.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Carries out a set variables operation */
  class OperationSetVariables : public Operation {
  public:
    OperationSetVariables(ContextPtr &context,
                          const std::vector<SystemVariable::Spec> &specs);
    OperationSetVariables(ContextPtr &context, const MetaLog::EntityHeader &header_);
    OperationSetVariables(ContextPtr &context, EventPtr &event);
    virtual ~OperationSetVariables() { }

    virtual void execute();
    virtual const String name();
    virtual const String label();
    virtual void display_state(std::ostream &os);
    virtual size_t encoded_state_length() const;
    virtual void encode_state(uint8_t **bufp) const;
    virtual void decode_state(const uint8_t **bufp, size_t *remainp);
    virtual void decode_request(const uint8_t **bufp, size_t *remainp);

  private:
    void initialize_dependencies();

    std::vector<SystemVariable::Spec> m_specs;
    StringSet m_completed;
  };

  /// Smart pointer to OperationSetVariables
  typedef intrusive_ptr<OperationSetVariables> OperationSetVariablesPtr;

  /* @}*/

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONSETVARIABLES_H
