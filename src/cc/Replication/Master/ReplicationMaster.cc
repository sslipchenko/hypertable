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

#include "Common/Compat.h"

#include <vector>

#include "Common/ScopeGuard.h"
#include "Common/SystemInfo.h"
#include "Common/FailureInducer.h"
#include "Common/md5.h"

#include "AsyncComm/ResponseCallback.h"

#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Namespace.h"

#include "Hyperspace/Config.h"
#include "Hyperspace/Session.h"

#include "Hypertable/Lib/SchemaUpdateEntities.h"
#include "Hypertable/Lib/MetaLogReader.h"

#include "Context.h"
#include "ReplicationMaster.h"
#include "RequestHandlerUpdateReceivers.h"
#include "ReplicationMasterEntity.h"
#include "RecoverySessionCallback.h"

namespace Hypertable {

using namespace Hyperspace;
using namespace Config;

ReplicationMaster *ReplicationMaster::ms_instance = 0;

void
ReplicationMaster::handle_schema_update(int what, const char *table_id,
            const char *table_name, const char *schema) {
  std::vector<String> destinations;
  {
    ScopedLock lock(m_mutex);

    SchemaPtr schema_ptr = Schema::new_instance(schema, strlen(schema));

    if (schema_ptr->get_replication_cluster().empty()) {
      // no replication
      HT_INFOF("Disabled replication for %s", table_id);
      ReplicationTypes::TableIdToClusterMap::iterator rt
          = m_replicated_tables.find(table_id);
      if (rt != m_replicated_tables.end())
        m_replicated_tables.erase(rt);
    }
    else {
      // replication is active
      HT_INFOF("Handling schema update for %s (%s) because replication is "
              "enabled; new schema is %s", table_id, table_name, schema);

      for (Schema::ReplicationClusterMap::const_iterator rit
            = schema_ptr->get_replication_cluster().begin();
          rit != schema_ptr->get_replication_cluster().end(); ++rit) {
        HT_INFOF("Table %s is replicated to '%s'", table_name,
                rit->first.c_str());
        destinations.push_back(rit->first);
      }
      m_replicated_tables[table_id] = destinations;
      m_table_names[table_id] = table_name;
      for (Schema::ReplicationClusterMap::const_iterator rit
              = schema_ptr->get_replication_cluster().begin(); 
          rit != schema_ptr->get_replication_cluster().end(); ++rit)
        m_known_clusters.insert(rit->first);
    }
  }

  // now send the schema updates the remote clusters
  foreach_ht (const String &dest, destinations) {
    ReplicationMasterClient *client = m_context->get_master_client(dest);
    HT_INFOF("Sending update (%s) to %s", table_name, dest.c_str());
    if (!client)
      HT_THROW(Error::REPLICATION_CLUSTER_NOT_FOUND,
              "Unable to connect to Replication.Master");
    client->apply_schema_update(what, table_name, schema);
    HT_INFOF("Schema update was applied successfully: what=%d, table=%s", 
            what, table_name);
  }
}
  
void
ReplicationMaster::update_recipients() {
  if (m_context->get_properties()->get_bool("Hypertable.Replication.TestMode"))
    return;

  ScopedLock lock(m_mutex);

  // get a list of all remote clusters
  ClusterConfiguration::RemoteMasterMap all_clusters;
  all_clusters = m_context->get_cluster_config()->get_all_clusters();

  // for each cluster: ask the Replication.Master for an up-to-date list of
  // its Replication.Slaves
  foreach_ht (const String &cluster, m_known_clusters) {
    EventPtr event(new Event(Event::MESSAGE));
    HT_INFOF("Requesting new receiver list for cluster %s", cluster.c_str());
    m_context->get_app_queue()->add(new RequestHandlerUpdateReceivers(
                event, m_context, cluster));
  }
}

void
ReplicationMaster::scan_dfs() {
  if (m_context->get_properties()->get_bool("Hypertable.Replication.TestMode"))
    return;

  String server_dir;
  StringSet fragment_list;
  StringSet linked_logs;
  ReplicationTypes::TableIdToClusterMap replicated_tables;
  {
    ScopedLock lock(m_mutex);
    linked_logs = m_linked_logs;
    server_dir = m_server_dir;
    replicated_tables = m_replicated_tables;
  }

  // descend through the DFS and get a list of all user commit log
  // fragments that require replication
  std::vector<Filesystem::DirectoryEntry> subdirs;
  try {
    m_context->get_dfs()->posix_readdir(server_dir, subdirs);
  }
  catch (...) {
  }

  foreach_ht (const Filesystem::DirectoryEntry &dirent, subdirs) {
    // skip files
    if (!(dirent.flags & Filesystem::DIRENT_DIRECTORY))
      continue;
    if (dirent.name.size() > 2 && dirent.name[0] == 'r'
            && dirent.name[1] == 's') {
      // log/user: replicate; only delete replicated/purged files
      String usr = Hypertable::format("%s/%s/log/user",
              server_dir.c_str(), dirent.name.c_str());
      scan_directory(usr, fragment_list);
      // log/\d+: delete all subdirectories with "purged-directory"
      remove_purged_directories(m_server_dir + "/" + dirent.name + "/log",
              linked_logs, replicated_tables);
      // log/metadata, log/root, log/system: delete all files that are .purged
      remove_purged_files(m_server_dir + "/" + dirent.name + "/log/root");
      remove_purged_files(m_server_dir + "/" + dirent.name + "/log/metadata");
      remove_purged_files(m_server_dir + "/" + dirent.name + "/log/system");
    }
  }

  // now scan all known transfer log directories for new files, or delete
  // the directory if possible
  foreach_ht (const String &s, linked_logs) {
    HT_INFOF("Scanning linked log %s", s.c_str());
    scan_directory(s, fragment_list);
    remove_purged_directories(s, linked_logs, replicated_tables);
  }

  // from here on we modify class members
  ScopedLock lock(m_mutex);

  m_unassigned_fragments.clear();

  // add all new fragments to the list of unreplicated fragments, and
  // delete all purged and replicated fragments from the DFS
  foreach_ht(StringSet::value_type fragment, fragment_list) {
    String purged_name, original_name;
    if (boost::ends_with(fragment, ".purged")) {
      purged_name = fragment;
      original_name = String(fragment.c_str(), fragment.size() - 7);
    }
    else {
      purged_name = fragment + ".purged";
      original_name = fragment;
    }

    // if the purged fragment was already fully replicated: delete the
    // fragment and remove it from the lists
    if (m_finished_fragments.find(purged_name) != m_finished_fragments.end()) {
      HT_INFOF("Fragment %s was replicationed and purged; will now be deleted",
               fragment.c_str());
      // m_context->get_dfs()->remove(fragment);
      m_context->get_dfs()->rename(fragment, fragment + ".deleted");
      m_finished_fragments.erase(purged_name);
      m_finished_fragments.erase(original_name);
    }
    // if the original fragment was already replicated: skip it
    else if (m_finished_fragments.find(original_name)
        != m_finished_fragments.end()) {
      HT_INFOF("Fragment %s already finished, skipping", fragment.c_str());
    }
    // if the fragment was already assigned then again skip it
    else if ((m_assigned_fragments.find(purged_name)
            != m_assigned_fragments.end())
        || (m_assigned_fragments.find(original_name)
            != m_assigned_fragments.end())) {
      HT_INFOF("Fragment %s already assigned, skipping", fragment.c_str());
    }
    // otherwise this fragment requires replication
    else {
      HT_INFOF("Adding fragment %s to the list of unassigned fragments",
               fragment.c_str());
      m_unassigned_fragments.insert(fragment);
    }
  }
}

void
ReplicationMaster::assign_fragments(const char *location,
            const InetAddr &slave_address, StringSet &fragments,
            ReplicationTypes::ClusterToSlavesMap &clusters) {
  ScopedLock lock(m_mutex);

  // store this slave's ID in our list
  if (m_known_slaves.find(location) == m_known_slaves.end())
    register_new_slave(location, slave_address);

  fragments.clear();

  // always return the current list of clusters and slaves, even if there are
  // no fragments to be replicated. It's possible that another slave (which
  // already processes a fragment) requires an updated list.
  clusters = m_cluster_slaves;

  // nothing to do?
  if (m_unassigned_fragments.empty()
      || m_replicated_tables.empty()
      || m_cluster_slaves.empty()) {
    HT_DEBUGF("Not enough data; not assigning any fragments. unassigned=%d, "
            "replicated=%d, slaves=%d", (int)m_unassigned_fragments.size(),
            (int)m_replicated_tables.size(), (int)m_cluster_slaves.size());
    return;
  }

  // and assign some of the unassigned fragments to this slave
  size_t fragment_count = m_unassigned_fragments.size() / m_known_slaves.size();
  if (!fragment_count)
    fragment_count = 1;

  for (size_t i = 0; i < fragment_count; i++) {
    StringSet::iterator it = m_unassigned_fragments.begin();
    HT_INFOF("Assigning fragment %s to %s", it->c_str(), location);
    m_assigned_fragments[*it] = location;
    fragments.insert(*it);
    m_unassigned_fragments.erase(it);
  }

  write_to_metalog();
}

void
ReplicationMaster::scan_directory(const String &directory,
        StringSet &fragments) {
  std::vector<Filesystem::DirectoryEntry> dirents;
  try {
    m_context->get_dfs()->posix_readdir(directory, dirents);
  }
  catch (...) {
  }

  if (dirents.empty()) {
    HT_DEBUG_OUT << "current directory: " << directory << "(empty)" << HT_END;
    return;
  }

  HT_DEBUG_OUT << "current directory: " << directory << HT_END;

  foreach_ht (const Filesystem::DirectoryEntry &dirent, dirents) {
    // skip subdirectories
    if (dirent.flags & Filesystem::DIRENT_DIRECTORY)
      continue;

    String path = directory + "/" + dirent.name;

    HT_DEBUG_OUT << " ... checking " << path << HT_END;
    // make sure this is a valid fragment ID
    if (dirent.name == "purged-directory")
      continue;
    if (boost::ends_with(dirent.name, ".mark"))
      continue;
    if (boost::ends_with(dirent.name, ".tmp"))
      continue;
    HT_DEBUG_OUT << path << " length is " << dirent.length << HT_END;
    if (dirent.length == 0) {
      uint64_t l = 0;
      try {
        l = m_context->get_dfs()->length(path);
        HT_DEBUG_OUT << path << " new length is " << l << HT_END;
      }
      catch (Exception &e) {
        if (e.code() != Error::DFSBROKER_FILE_NOT_FOUND) {
          HT_ERROR_OUT << e << HT_END;
          continue;
        }
        if (boost::ends_with(dirent.name, ".purged")) {
          HT_ERROR_OUT << e << HT_END;
          continue;
        }
        // retry with .purged
        try {
          l = m_context->get_dfs()->length(path + ".purged");
          HT_DEBUG_OUT << path << " new length is " << l << HT_END;
        }
        catch (Exception &ex) {
          HT_ERROR_OUT << e << HT_END;
          continue;
        }
      }
      if (l == 0) {
        HT_DEBUG_OUT << path << " is empty, skipping" << HT_END;
        continue;
      }
    }

    HT_INFOF(" ... adding %s", path.c_str());
    // add this fragment for replication
    fragments.insert(path);
  }
}

bool
ReplicationMaster::remove_purged_directories(const String &directory,
            const StringSet &linked_logs,
            const ReplicationTypes::TableIdToClusterMap &replicated_tables,
            int level, int basedir_length) {
  std::vector<Filesystem::DirectoryEntry> dirents;

  if (!basedir_length)
    basedir_length = directory.size();

  try {
    m_context->get_dfs()->posix_readdir(directory, dirents);
  }
  catch (...) {
    return false;
  }

  if (dirents.empty())
    return false;

  bool found_marker = false;
  bool all_purged = true;
  bool not_replicated = false;

  // check if there's a "purged-directory" and every other file in the
  // directory was already replicated; if yes then this whole directory
  // can be removed
  foreach_ht (const Filesystem::DirectoryEntry &dirent, dirents) {
    if (level == 0
        && (dirent.name == "user" || dirent.name == "metadata"
            || dirent.name == "root" || dirent.name == "system"))
      continue;
    String path = directory + "/" + dirent.name;

    if (dirent.name == "purged-directory") {
      HT_DEBUG_OUT << path << " is purge-marker" << HT_END;
      found_marker = true;
      // if this is a transfer log then it's definitely replicated, because
      // it was reported by a Replication.Slave
      if (linked_logs.find(directory) != linked_logs.end())
        break;
      // check if this directory requires replication; if not then it
      // can be deleted right away
      String table_id(directory.c_str() + basedir_length);
      if (table_id.empty())
        break;
      boost::trim_if(table_id, boost::is_any_of("/"));
      while (table_id[table_id.size() - 1] != '/')
        table_id.resize(table_id.size() - 1);
      boost::trim_if(table_id, boost::is_any_of("/"));
      if (replicated_tables.find(table_id) != replicated_tables.end()) {
        HT_INFOF("Table %s is not replicated, can be deleted",
               table_id.c_str());
        not_replicated = true;
        break;
      }
      HT_INFOF("Table %s is replicated", table_id.c_str());
      continue;
    }

    if (m_finished_fragments.find(path) != m_finished_fragments.end()) {
      // file was already replicated, continue
      HT_DEBUG_OUT << path << " was already replicated" << HT_END;
      continue;
    }

    if (boost::ends_with(dirent.name, ".purged")) {
      String original(path.c_str(), path.size() - 7);
      if (m_finished_fragments.find(original) != m_finished_fragments.end()) {
        // file was already replicated, continue
        HT_DEBUG_OUT << path << " was already replicated" << HT_END;
        continue;
      }
    }

    // file was not yet replicated; abort
    all_purged = false;
    break;
  }

  if ((all_purged || not_replicated) && found_marker) {
    HT_INFOF("directory %s is marked for removal", directory.c_str());
    // remove files from m_finished_fragments
    ScopedLock lock(m_mutex);
    foreach_ht (const Filesystem::DirectoryEntry &dirent, dirents) {
      m_finished_fragments.erase(dirent.name);
      if (boost::ends_with(dirent.name, ".purged")) {
        String original(dirent.name.c_str(), dirent.name.size() - 7);
        m_finished_fragments.erase(original);
      }
    }
    return true;
  }

  // otherwise continue to check all directories recursively
  foreach_ht (const Filesystem::DirectoryEntry &dirent, dirents) {
    if (!(dirent.flags & Filesystem::DIRENT_DIRECTORY))
      continue;
    String path = directory + "/" + dirent.name;
    if (remove_purged_directories(path, linked_logs, replicated_tables,
                level + 1, basedir_length)) {
      HT_INFOF("Removing purged directory %s", path.c_str());
      // m_context->get_dfs()->rmdir(path);
      // also remove this directory from the list of the linked log directories
      ScopedLock lock(m_mutex);
      m_linked_logs.erase(path);
    }
  }

  return false;
}

void
ReplicationMaster::remove_purged_files(const String &directory) {
  std::vector<Filesystem::DirectoryEntry> dirents;

  try {
    m_context->get_dfs()->posix_readdir(directory, dirents);
  }
  catch (...) {
    return;
  }

  foreach_ht (const Filesystem::DirectoryEntry &dirent, dirents) {
    String path = directory + "/" + dirent.name;
    if (boost::ends_with(path, ".purged")) {
      HT_INFOF("Removing purged file %s", path.c_str());
      //m_context->get_dfs()->remove(path);
      m_context->get_dfs()->rename(path, path + ".deleted");

      ScopedLock lock(m_mutex);
      m_finished_fragments.erase(dirent.name);
    }
  }
}

void
ReplicationMaster::update_receivers(const String &cluster,
        const std::vector<InetAddr> &receivers) {
  foreach_ht(const InetAddr &receiver, receivers) {
    HT_INFOF("New receiver for cluster %s: %s", cluster.c_str(),
             receiver.format().c_str());
  }

  ScopedLock lock(m_mutex);
  if (receivers.size())
    m_cluster_slaves[cluster] = receivers;
}

void
ReplicationMaster::apply_schema_update(int what, const String &table_name,
            const String &schema_str) {
  ScopedLock lock(m_mutex);

  // make sure that we're not applying an older version
  SchemaPtr schema = Schema::new_instance(schema_str, schema_str.size());
  std::map<String, uint32_t>::iterator it = m_table_generations.find(table_name);
  if (it != m_table_generations.end()) {
    if (m_table_generations[table_name] >= schema->get_generation()) {
      HT_WARN_OUT << "Skipping update because the generation is too old"
          << HT_END;
      return;
    }
  }

  // disable replication, otherwise we'd replicate recursively to ourselves
  schema->clear_replication_clusters();
  String s;
  schema->render(s);

  try {
    String ns_path = Filesystem::dirname(table_name);
    HT_INFOF("Schema update for (%s) %s", ns_path.c_str(), table_name.c_str());
    ClientPtr client = m_context->get_client();

    String basepath = m_context->get_properties()->get_str("Hypertable.Replication.BaseNamespace");
    NamespacePtr basens = client->open_namespace(basepath, 0);
    client->create_namespace(ns_path, basens.get(), true, true);
    NamespacePtr ns = client->open_namespace(ns_path, basens.get());
    switch (what) {
      case ReplicationMasterClient::SCHEMA_UPDATE_CREATE_TABLE:
        HT_INFOF("Creating table '%s'", Filesystem::basename(table_name).c_str());
        HT_MAYBE_FAIL("Replication.Master-Apply-Schema-1");
        ns->create_table(Filesystem::basename(table_name), s);
        break;
      case ReplicationMasterClient::SCHEMA_UPDATE_ALTER_TABLE:
        HT_INFOF("Altering table '%s'", table_name.c_str());
        HT_MAYBE_FAIL("Replication.Master-Apply-Schema-2");
        ns->alter_table(Filesystem::basename(table_name), s, false);
        break;
      default:
        HT_ERROR_OUT << "Unknown 'what' parameter " << what << HT_END;
        HT_THROW(Error::MALFORMED_REQUEST, "Unknown 'what' parameter");
        break;
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << "Schema update failed: " << e << HT_END;
    throw (e);
  }
  
  m_table_generations[table_name] = schema->get_generation();
}

void
ReplicationMaster::fragment_finished(String &fragment, int error,
        uint64_t offset, const std::vector<String> &linked_logs) {
  ScopedLock lock(m_mutex);

  // in case of error: copy fragment and inform user
  if (error) {
    char hash[33];
    md5_string(format("%s:%u", fragment.c_str(), (unsigned)time(0)).c_str(),
            hash);
    String new_path = format("%s/%s/%s", m_server_dir.c_str(),
            "replicator/errors", hash);
    HT_ERROR_OUT << "Replication of fragment " << fragment << " failed with "
        "error " << error << "; moved to " << new_path << HT_END;
    m_context->get_dfs()->mkdirs(new_path);
    m_context->get_dfs()->rename(fragment, new_path);
  }

  // add the linked log(s)
  foreach_ht (const String &log_dir, linked_logs) {
    HT_INFOF("Adding linked log dir %s", log_dir.c_str());
    m_linked_logs.insert(log_dir);
  }

  m_assigned_fragments.erase(fragment);
  m_finished_fragments.insert(fragment);

  write_to_metalog();

  HT_MAYBE_FAIL("Replication.Master-FragmentFinished-1");
}

void
ReplicationMaster::handle_disconnect(const String &location) {
  ScopedLock lock(m_mutex);

  HT_INFOF("Handling disconnect from %s", location.c_str());

  // check if this remote connection is from a Replication.Slave
  if (m_known_slaves.find(location) == m_known_slaves.end())
    return;

  HT_INFOF("Local Replication.Slave %s disconnected", location.c_str());
  m_known_slaves.erase(location);

  // check each assigned fragment; if it was assigned to this slave then
  // move it to the "unassigned" list
  std::map<String, String>::iterator it = m_assigned_fragments.begin();
  while (it != m_assigned_fragments.end()) {
    std::map<String, String>::iterator it2 = it;
    HT_INFOF("Checking fragment %s assigned to %s", it2->first.c_str(),
             it2->second.c_str());
    ++it;
    if (it2->second == location) {
      HT_INFOF("Reassigning fragment %s", it2->first.c_str());
      m_unassigned_fragments.insert(it2->first);
      m_assigned_fragments.erase(it2);
    }
  }
}

void
ReplicationMaster::obtain_master_lock() {
  try {
    uint64_t handle = 0;
    uint32_t lock_status = LOCK_STATUS_BUSY;
    uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;
    LockSequencer sequencer;
    bool reported = false;
    uint32_t retry_interval = m_context->get_properties()->get_i32("Hypertable.Connection.Retry.Interval");

    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_context->get_hyperspace(),
            &handle);

    String path = m_context->get_toplevel_dir() + "/replication";

    // Create /hypertable/replication if it does not exist
    if (!m_context->get_hyperspace()->exists(path))
      m_context->get_hyperspace()->mkdir(path);

    // Create /hypertable/replication/master if it does not exist
    path += "/master";
    if (!m_context->get_hyperspace()->exists(path)) {
      handle = m_context->get_hyperspace()->open(path,
              OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE);
      m_context->get_hyperspace()->close(handle);
      handle = 0;
    }

    // now try to lock the file
    m_master_handle = m_context->get_hyperspace()->open(path, oflags);

    while (lock_status != LOCK_STATUS_GRANTED) {
      m_context->get_hyperspace()->try_lock(m_master_handle,
              LOCK_MODE_EXCLUSIVE, &lock_status, &sequencer);

      if (lock_status != LOCK_STATUS_GRANTED) {
        if (!reported) {
          HT_INFOF("Couldn't obtain lock on '%s' due to conflict, entering "
                  "retry loop ...", path.c_str());
          reported = true;
        }
        poll(0, 0, retry_interval);
      }
    }

    // write the location info
    NetInfo ni = System::net_info();
    String value = Hypertable::format("%s:%d", ni.host_name.c_str(),
           (int)get_i16("port"));
    m_context->get_hyperspace()->attr_set(m_master_handle, "location",
            value.c_str(), value.size() + 1);

    HT_INFOF("Obtained lock on '%s'", path.c_str());
  }
  catch (Exception &e) {
    HT_FATAL_OUT << e << HT_END;
  }
}

void
ReplicationMaster::read_from_metalog() {
  // no need to lock this function; it's only called in the constructor

  std::vector<MetaLog::EntityPtr> entities;
  MetaLog::ReaderPtr mlr;
  MetaLog::DefinitionPtr definition
      = new ReplicationMasterDefinition("repmaster");
  mlr = new MetaLog::Reader(m_context->get_dfs(), definition,
          m_server_dir + "/replicator/master");
  mlr->get_entities(entities);
  if (entities.empty())
    return;

  // no need to do anything; the decode() function automatically stores
  // everything in the ReplicationMaster instance
}

void 
ReplicationMaster::write_to_metalog() {
  // don't lock; mutex was already locked by callers

  if (!m_mlwriter) {
    std::vector<MetaLog::EntityPtr> entities;
    MetaLog::DefinitionPtr definition
        = new ReplicationMasterDefinition("repmaster");
    m_mlwriter = new MetaLog::Writer(m_context->get_dfs(), definition,
          m_server_dir + "/replicator/master", entities);
  }

  MetaLog::EntityPtr entity = new ReplicationMasterEntity(this);
  m_mlwriter->record_state(entity.get());
}

void
ReplicationMaster::register_new_slave(const char *location,
        const InetAddr &slave_address) {
  // mutex is already locked by the caller
  HT_INFOF("Incoming connection from %s (%s)", location,
           slave_address.format().c_str());
  m_known_slaves[location] = slave_address;

  // register a hyperspace callback to figure out when the slave disconnects
  String path = m_context->get_toplevel_dir() + "/replication/" + location;

  RecoverySessionCallback *cb = new RecoverySessionCallback(m_context,
                    location);
  Hyperspace::HandleCallbackPtr cb_ptr = cb;

  uint64_t handle = m_context->get_hyperspace()->open(path,
          Hyperspace::OPEN_FLAG_READ, cb_ptr);
  HT_ASSERT(handle);
  cb->set_handle(handle);
}

} // namespace Hypertable
