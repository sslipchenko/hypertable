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
#include "Common/Init.h"
#include "Common/System.h"
#include "Common/FailureInducer.h"

#include "AsyncComm/Config.h"

#include "DfsBroker/Lib/Config.h"

#include "Hyperspace/Config.h"

#include "Context.h"
#include "ConnectionHandler.h"
#include "TimerHandler.h"
#include "ReplicationSlave.h"

using namespace Hypertable;
using namespace Config;

namespace {

  struct AppPolicy : Config::Policy {
    static void init_options() {
      alias("port", "Hypertable.Replication.Slave.Port");
    }
  };

  typedef Meta::list<GenericServerPolicy, DfsClientPolicy,
          HyperspaceClientPolicy, DefaultCommPolicy, AppPolicy> Policies;

  class HandlerFactory : public ConnectionHandlerFactory {
  public:
    HandlerFactory(ContextPtr &context) : m_context(context) {
      m_handler = new ConnectionHandler(m_context);
    }

    virtual void get_instance(DispatchHandlerPtr &dhp) {
      dhp = m_handler;
    }

  private:
    ContextPtr m_context;
    DispatchHandlerPtr m_handler;
  };

} // local namespace


int main(int argc, char **argv) {
  try {
    init_with_policies<Policies>(argc, argv);
    uint16_t port = get_i16("port");

    if (!properties->get_bool("Hypertable.RangeServer.CommitLog.FragmentRemoval.Disable")) {
      HT_ERROR("Option 'Hypertable.RangeServer.CommitLog.FragmentRemoval.Disable' "
              "MUST be set to true in order to use replication!");
      _exit(-1);
    }

    if (has("induce-failure")) {
      if (FailureInducer::instance == 0)
        FailureInducer::instance = new FailureInducer();
      FailureInducer::instance->parse_option(get_str("induce-failure"));
    }

    ContextPtr context = new Context(properties);

    // initialize the ReplicationSlave class
    ReplicationSlavePtr repslave = new ReplicationSlave(context);

    ConnectionHandlerFactoryPtr hf(new HandlerFactory(context));
    InetAddr listen_addr(INADDR_ANY, port);

    Comm::instance()->listen(listen_addr, hf);

    HT_INFO("Successfully Initialized.");

    TimerHandlerPtr timer = new TimerHandler(context);

    context->get_app_queue()->join();
    Comm::instance()->close_socket(listen_addr);
    context = 0;
  }
  catch (Exception &e) {
    HT_FATAL_OUT << e << HT_END;
  }

  return 0;
}
