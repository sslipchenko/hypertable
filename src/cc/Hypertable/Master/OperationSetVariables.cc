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
 * Definitions for OperationSetVariables.
 * This file contains definitions for OperationSetVariables, an Operation class
 * for setting system state variables.
 */

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/FailureInducer.h"
#include "Common/ScopeGuard.h"
#include "Common/Serialization.h"

#include "Hyperspace/Session.h"

#include "Hypertable/Lib/Key.h"

//#include "DispatchHandlerOperationSetVariables.h"
#include "OperationSetVariables.h"
#include "Utility.h"

#include <boost/algorithm/string.hpp>

using namespace Hypertable;
using namespace Hyperspace;

OperationSetVariables::OperationSetVariables(ContextPtr &context,
                                             const std::vector<SystemVariable::Spec> &specs)
  : Operation(context, MetaLog::EntityType::OPERATION_SET), m_specs(specs) {
  initialize_dependencies();
}

OperationSetVariables::OperationSetVariables(ContextPtr &context,
                                             const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}

OperationSetVariables::OperationSetVariables(ContextPtr &context, EventPtr &event)
  : Operation(context, event, MetaLog::EntityType::OPERATION_SET) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  initialize_dependencies();
}

void OperationSetVariables::initialize_dependencies() {
  m_exclusivities.insert("SET");
  m_dependencies.insert(Dependency::INIT);
  m_dependencies.insert(Dependency::METADATA);
  m_dependencies.insert(Dependency::SYSTEM);
  m_dependencies.insert(Dependency::RECOVER_SERVER);
}

void OperationSetVariables::execute() {

#if 0
  String filename;
  String index_id;
  String qualifier_index_id;
  String index_name = Filesystem::dirname(m_name);
  if (index_name == "/")
    index_name += String("^") + Filesystem::basename(m_name);
  else
    index_name += String("/^") + Filesystem::basename(m_name);
  String qualifier_index_name = Filesystem::dirname(m_name);
  if (qualifier_index_name == "/")
    qualifier_index_name += String("^^") + Filesystem::basename(m_name);
  else
    qualifier_index_name += String("/^^") + Filesystem::basename(m_name);
  bool is_namespace;
  StringSet servers;
  DispatchHandlerOperationPtr op_handler;
  TableIdentifier table;
  DependencySet dependencies;
#endif

  int32_t state = get_state();
  String label;

  bool first=true;
  foreach_ht (const SystemVariable::Spec &spec, m_specs) {
    if (!first)
      label += ",";
    label += SystemVariable::code_to_string(spec.code);
    label += "=";
    label += spec.value ? "true" : "false";
    first = false;
  }

  HT_INFOF("Entering SetVariables-%lld(%s) state=%s",
           (Lld)header.id, label.c_str(), OperationState::get_text(state));

#if 0
  switch (state) {

  case OperationState::INITIAL:
    // Check to see if namespace exists
    if(m_context->namemap->name_to_id(m_name, m_id, &is_namespace)) {
      if (is_namespace && !m_if_exists) {
        complete_error(Error::TABLE_NOT_FOUND, format("%s is a namespace", m_name.c_str()));
        return;
      }
    }
    else {
      if (m_if_exists)
        complete_ok();
      else
        complete_error(Error::TABLE_NOT_FOUND, m_name);
      return;
    }

    // issue another request for an index table
    if (m_context->namemap->name_to_id(index_name, index_id)) {
      HT_INFOF("  Dropping index table %s (id %s)", 
           index_name.c_str(), index_id.c_str());
      Operation *op = new OperationSetVariables(m_context, index_name, false);
      op->add_obstruction(index_name + "-drop-index");

      ScopedLock lock(m_mutex);
      add_dependency(index_name + "-drop-index");
      m_sub_ops.push_back(op);
    }

    // ... and for the qualifier index
    if (m_context->namemap->name_to_id(qualifier_index_name, 
                qualifier_index_id)) {
      HT_INFOF("  Dropping qualifier index table %s (id %s)", 
           qualifier_index_name.c_str(), qualifier_index_id.c_str());
      Operation *op = new OperationSetVariables(m_context, qualifier_index_name, 
              false);
      op->add_obstruction(qualifier_index_name + "-drop-qualifier-index");

      ScopedLock lock(m_mutex);
      add_dependency(qualifier_index_name + "-drop-qualifier-index");
      m_sub_ops.push_back(op);
    }

    set_state(OperationState::SCAN_METADATA);
    m_context->mml_writer->record_state(this);

    HT_MAYBE_FAIL("drop-table-INITIAL");
    break;

    // fall through

  case OperationState::SCAN_METADATA:
    servers.clear();
    Utility::get_table_server_set(m_context, m_id, "", servers);
    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_dependencies.insert(Dependency::INIT);
      m_dependencies.insert(Dependency::METADATA);
      m_dependencies.insert(Dependency::SYSTEM);
      for (StringSet::iterator iter=servers.begin(); iter!=servers.end(); ++iter) {
        if (m_completed.count(*iter) == 0)
          m_dependencies.insert(*iter);
      }
      m_state = OperationState::ISSUE_REQUESTS;
    }
    m_context->mml_writer->record_state(this);
    return;

  case OperationState::ISSUE_REQUESTS: {
    table.id = m_id.c_str();
    table.generation = 0;
    {
      ScopedLock lock(m_mutex);
      dependencies = m_dependencies;
    }
    dependencies.erase(Dependency::INIT);
    dependencies.erase(Dependency::METADATA);
    dependencies.erase(Dependency::SYSTEM);
    op_handler = new DispatchHandlerOperationSetVariables(m_context, table);
    op_handler->start(dependencies);
    if (!op_handler->wait_for_completion()) {
      std::set<DispatchHandlerOperation::Result> results;
      op_handler->get_results(results);
      foreach_ht (const DispatchHandlerOperation::Result &result, results) {
        if (result.error == Error::OK ||
            result.error == Error::TABLE_NOT_FOUND ||
            result.error == Error::RANGESERVER_TABLE_DROPPED) {
          ScopedLock lock(m_mutex);
          m_completed.insert(result.location);
          m_dependencies.erase(result.location);
        }
        else
          HT_WARNF("Drop table error at %s - %s (%s)", result.location.c_str(),
                   Error::get_text(result.error), result.msg.c_str());
      }
      set_state(OperationState::SCAN_METADATA);
      m_context->mml_writer->record_state(this);
      return;
    }

    // now drop the "primary" table
    try {
      m_context->namemap->drop_mapping(m_name);
      filename = m_context->toplevel_dir + "/tables/" + m_id;
      m_context->hyperspace->unlink(filename.c_str());
    }
    catch (Exception &e) {
      if (e.code() != Error::HYPERSPACE_FILE_NOT_FOUND &&
          e.code() != Error::HYPERSPACE_BAD_PATHNAME)
        HT_THROW2F(e.code(), e, "Error executing DropTable %s", 
                m_name.c_str());
    }
    m_context->monitoring->invalidate_id_mapping(m_id);
    complete_ok();
    break;
  }

  default:
    HT_FATALF("Unrecognized state %d", state);
  }
#endif

  complete_ok_no_log();

  HT_INFOF("Leaving SetVariables-%lld", (Lld)header.id);
}


void OperationSetVariables::display_state(std::ostream &os) {
  bool first = true;
  foreach_ht (const SystemVariable::Spec &spec, m_specs) {
    if (!first)
      os << ",";
    os << SystemVariable::code_to_string(spec.code) << "=" << (spec.value ? "true" : "false");
    first = false;
  }
  os << " ";
}

size_t OperationSetVariables::encoded_state_length() const {
  return 0;
}

void OperationSetVariables::encode_state(uint8_t **bufp) const {
}

void OperationSetVariables::decode_state(const uint8_t **bufp, size_t *remainp) {
}

void OperationSetVariables::decode_request(const uint8_t **bufp, size_t *remainp) {
  int32_t count = Serialization::decode_i32(bufp, remainp);
  SystemVariable::Spec spec;
  for (int32_t i=0; i<count; i++) {
    spec.code = Serialization::decode_i32(bufp, remainp);
    spec.value = Serialization::decode_bool(bufp, remainp);
    m_specs.push_back(spec);
  }
}

const String OperationSetVariables::name() {
  return "OperationSetVariables";
}

const String OperationSetVariables::label() {
  String label = String("SetVariables ");
  bool first=true;
  foreach_ht (const SystemVariable::Spec &spec, m_specs) {
    if (!first)
      label += ",";
    label += SystemVariable::code_to_string(spec.code);
    label += "=";
    label += spec.value ? "true" : "false";
    first = false;
  }
  return label;
}

