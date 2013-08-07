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
 * Declarations for SystemState.
 * This file contains declarations for SystemState, a MetaLog entity
 * for persisting global system state.
 */

#ifndef HYPERTABLE_SYSTEMSTATE_H
#define HYPERTABLE_SYSTEMSTATE_H

#include <vector>

#include "Hypertable/Lib/MetaLogEntity.h"
#include "Hypertable/Lib/SystemVariable.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Holds persistent global system state. */
  class SystemState : public MetaLog::Entity {
  public:
    SystemState();
    SystemState(const MetaLog::EntityHeader &header_);
    virtual ~SystemState() { }

    /** Set a vector of variables by administrator.
     * @param specs Vector of variable specifications
     * @return <i>true</i> if variable state changed and generation
     * number incremented, <i>false</i> otherwise.
     */
    bool admin_set(std::vector<SystemVariable::Spec> &spces);

    /** Set a variable by administrator.
     * @param code Variable code to set
     * @param value Value of variable to set
     * @return <i>true</i> if variable state changed and generation
     * number incremented, <i>false</i> otherwise.
     */
    bool admin_set(int code, bool value);

    /** Set a vector of variables by automated condition.
     * @param specs Vector of variable specifications
     * @return <i>true</i> if variable state changed and generation
     * number incremented, <i>false</i> otherwise.
     */
    bool auto_set(std::vector<SystemVariable::Spec> &spces);

    /** Set a variable by administrator.
     * @param code Variable code to set
     * @param value Value of variable to set
     * @return <i>true</i> if variable state changed and generation
     * number incremented, <i>false</i> otherwise.
     */
    bool auto_set(int code, bool value);

    void get(std::vector<SystemVariable::Spec> &spces, uint64_t *generation);

    /** Return name of entity.
     * @return Name of entity.
     */
    virtual const String name() { return "SystemState"; }

    /** Return textual representation of entity state.
     * @param os Output stream on which to write state string
     */
    virtual void display(std::ostream &os);
    virtual size_t encoded_length() const;
    virtual void encode(uint8_t **bufp) const;
    virtual void decode(const uint8_t **bufp, size_t *remainp);

  private:

    /// Generation number incremented with each state change
    uint64_t m_generation;

    /// Administrator set state variables
    std::vector<SystemVariable::Spec> m_admin_specified;

    /// Automatically set state variables
    std::vector<SystemVariable::Spec> m_auto_specified;
  };

  /// Smart pointer to SystemState
  typedef intrusive_ptr<SystemState> SystemStatePtr;

  /* @}*/

} // namespace Hypertable

#endif // HYPERTABLE_SYSTEMSTATE_H
