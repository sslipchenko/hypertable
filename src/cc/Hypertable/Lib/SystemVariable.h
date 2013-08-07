/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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
 * Declarations for SystemVariable.
 * This file contains the namespace SystemVariable which includes an enum and
 * conversion functions for representing system variables.
 */

#ifndef HYPERTABLE_SYSTEMVARIABLE_H
#define HYPERTABLE_SYSTEMVARIABLE_H

#include "Common/String.h"
#include <ostream>
#include <stdexcept>

namespace Hypertable {

  /** @addtogroup libHypertable
   *  @{
   */

  namespace SystemVariable {

    /** Enumeration for variable codes.
     */
    enum Code {
      READONLY = 0,   /**< Read-only */
      COUNT    = 1    /**< Valid code count */
    };

    /** Holds a variable code and boolean value.
     */
    struct Spec {
      int code;   /**< Variable code */
      bool value; /**< Variable value */
    };

    /** Converts variable code to variable string
     * @param var_code The variable code
     * @return Variable string corresponding to variable code
     */
    const char *code_to_string(int var_code);

    /** Converts variable string to variable code.
     * @param var_string String representation of variable
     * @return Variable string corresponding to variable code or 0 if
     * <code>var_string</code> is invalid
     */
    int string_to_code(const String &var_string);

    /** Returns default value for given variable
     * @param var_code The variable code
     * @return Default value for <code>var_code</code>
     */
    bool default_value(int var_code);

  } // namespace SystemVariable

  /** @} */

} // namespace Hypertable

#endif // HYPERTABLE_SYSTEMVARIABLE_H
