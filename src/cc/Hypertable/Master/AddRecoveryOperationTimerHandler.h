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

#ifndef HYPERTABLE_ADDRECOVERYOPERATIONTIMERHANDLER_H
#define HYPERTABLE_ADDRECOVERYOPERATIONTIMERHANDLER_H

#include "AsyncComm/DispatchHandler.h"

#include "Context.h"
#include "OperationRecover.h"

namespace Hypertable {

  /**
   */
  class AddRecoveryOperationTimerHandler : public DispatchHandler {

  public:
    
    AddRecoveryOperationTimerHandler(ContextPtr &context,
                                     RangeServerConnectionPtr &rsc) 
      : m_context(context), m_rsc(rsc) { }

    virtual void handle(EventPtr &event_ptr) {
      OperationPtr operation = new OperationRecover(m_context, m_rsc);
      m_context->op->add_operation(operation);
    }

  private:
    ContextPtr m_context;
    RangeServerConnectionPtr m_rsc;
  };

}

#endif // HYPERTABLE_ADDRECOVERYOPERATIONTIMERHANDLER_H
