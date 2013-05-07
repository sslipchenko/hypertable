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

#ifndef REPLICATION_MASTER_ENTITY_H
#define REPLICATION_MASTER_ENTITY_H

#include "Hypertable/Lib/MetaLogDefinition.h"
#include "Hypertable/Lib/MetaLogEntity.h"

namespace Hypertable {

class ReplicationMasterEntity : public MetaLog::Entity {
  public:
    const static int ENTITY_ID = 0x40001;

    ReplicationMasterEntity(ReplicationMaster *repmaster)
      : Entity(ENTITY_ID), m_repmaster(repmaster) {
    }

    ReplicationMasterEntity(const MetaLog::EntityHeader &header_)
      : Entity(header_) {
      m_repmaster = ReplicationMaster::get_instance();
    }

    virtual const String name() { return "ReplicationMasterEntity"; }

    virtual size_t encoded_length() const {
      size_t len = 0;

      // the last applied schema generations
      len += 4;
      std::map<String, uint32_t>::iterator genit;
      for (genit = m_repmaster->m_table_generations.begin();
            genit != m_repmaster->m_table_generations.end(); ++genit)
        len += 4 + Serialization::encoded_length_vstr(genit->first);

      // the list of replicated tables
      len += 4;
      ReplicationTypes::TableIdToClusterMap::iterator rtit;
      for (rtit = m_repmaster->m_replicated_tables.begin();
            rtit != m_repmaster->m_replicated_tables.end(); ++rtit) {
        len += 4 + Serialization::encoded_length_vstr(rtit->first);
        std::vector<String>::iterator vit;
        for (vit = rtit->second.begin(); vit != rtit->second.end(); ++vit)
          len += Serialization::encoded_length_vstr(*vit);
      }

      // the table names
      len += 4;
      ReplicationTypes::TableIdToTableNameMap::iterator tnit;
      for (tnit = m_repmaster->m_table_names.begin();
            tnit != m_repmaster->m_table_names.end(); ++tnit) {
        len += Serialization::encoded_length_vstr(tnit->first);
        len += Serialization::encoded_length_vstr(tnit->second);
      }

      // the list of unassigned fragments
      len += 4;
      StringSet::iterator fit;
      for (fit = m_repmaster->m_unassigned_fragments.begin();
            fit != m_repmaster->m_unassigned_fragments.end(); ++fit)
        len += Serialization::encoded_length_vstr(*fit);

      // the list of finished fragments
      len += 4;
      for (fit = m_repmaster->m_finished_fragments.begin();
            fit != m_repmaster->m_finished_fragments.end(); ++fit)
        len += Serialization::encoded_length_vstr(*fit);

      // list of remote clusters
      len += 4;
      for (fit = m_repmaster->m_known_clusters.begin();
            fit != m_repmaster->m_known_clusters.end(); ++fit)
        len += Serialization::encoded_length_vstr(*fit);

      // all the linked log directories
      len += 4;
      for (fit = m_repmaster->m_linked_logs.begin();
            fit != m_repmaster->m_linked_logs.end(); ++fit)
        len += Serialization::encoded_length_vstr(*fit);

      return len;
    }

    virtual void encode(uint8_t **bufp) const {
      // the last applied schema generations
      Serialization::encode_i32(bufp,
              m_repmaster->m_table_generations.size());
      std::map<String, uint32_t>::iterator genit;
      for (genit = m_repmaster->m_table_generations.begin();
            genit != m_repmaster->m_table_generations.end(); ++genit) {
        Serialization::encode_vstr(bufp, genit->first);
        Serialization::encode_i32(bufp, genit->second);
      }

      // the list of replicated tables
      Serialization::encode_i32(bufp,
              m_repmaster->m_replicated_tables.size());
      ReplicationTypes::TableIdToClusterMap::iterator rtit;
      for (rtit = m_repmaster->m_replicated_tables.begin();
            rtit != m_repmaster->m_replicated_tables.end(); ++rtit) {
        Serialization::encode_i32(bufp, rtit->second.size());
        Serialization::encode_vstr(bufp, rtit->first);
        std::vector<String>::iterator vit;
        for (vit = rtit->second.begin(); vit != rtit->second.end(); ++vit)
          Serialization::encode_vstr(bufp, *vit);
      }

      // the table names
      Serialization::encode_i32(bufp,
              m_repmaster->m_table_names.size());
      ReplicationTypes::TableIdToTableNameMap::iterator tnit;
      for (tnit = m_repmaster->m_table_names.begin();
            tnit != m_repmaster->m_table_names.end(); ++tnit) {
        Serialization::encode_vstr(bufp, tnit->first);
        Serialization::encode_vstr(bufp, tnit->second);
      }

      // the list of unassigned fragments
      Serialization::encode_i32(bufp,
              m_repmaster->m_unassigned_fragments.size());
      StringSet::iterator fit;
      for (fit = m_repmaster->m_unassigned_fragments.begin();
            fit != m_repmaster->m_unassigned_fragments.end(); ++fit)
        Serialization::encode_vstr(bufp, *fit);

      // the list of finished fragments
      Serialization::encode_i32(bufp,
              m_repmaster->m_finished_fragments.size());
      for (fit = m_repmaster->m_finished_fragments.begin();
            fit != m_repmaster->m_finished_fragments.end(); ++fit)
        Serialization::encode_vstr(bufp, *fit);

      // the list of remote clusters
      Serialization::encode_i32(bufp,
              m_repmaster->m_known_clusters.size());
      for (fit = m_repmaster->m_known_clusters.begin();
            fit != m_repmaster->m_known_clusters.end(); ++fit)
        Serialization::encode_vstr(bufp, *fit);

      // all the linked log directories
      Serialization::encode_i32(bufp,
              m_repmaster->m_linked_logs.size());
      for (fit = m_repmaster->m_linked_logs.begin();
            fit != m_repmaster->m_linked_logs.end(); ++fit)
        Serialization::encode_vstr(bufp, *fit);
    }

    virtual void decode(const uint8_t **bufp, size_t *remainp) {
      HT_INFO("Decoding Master's state from MetaLog");
      m_repmaster->m_table_generations.clear();

      // the last applied schema generations
      size_t len = Serialization::decode_i32(bufp, remainp);
      for (size_t i = 0; i < len; i++) {
        String table = Serialization::decode_vstr(bufp, remainp);
        m_repmaster->m_table_generations[table]
            = Serialization::decode_i32(bufp, remainp);
        HT_INFOF("Adding table generation %s = %lld", table.c_str(),
                 (Lld)m_repmaster->m_table_generations[table]);
      }

      m_repmaster->m_replicated_tables.clear();

      // the list of replicated tables
      len = Serialization::decode_i32(bufp, remainp);
      for (size_t i = 0; i < len; i++) {
        size_t vlen = Serialization::decode_i32(bufp, remainp);
        String table = Serialization::decode_vstr(bufp, remainp);
        std::vector<String> vec;
        for (size_t j = 0; j < vlen; j++)
          vec.push_back(Serialization::decode_vstr(bufp, remainp));
        m_repmaster->m_replicated_tables[table] = vec;
        HT_INFOF("Adding replicated table %s", table.c_str());
      }

      // adding the table names

      m_repmaster->m_table_names.clear();

      len = Serialization::decode_i32(bufp, remainp);
      for (size_t i = 0; i < len; i++) {
        String s1 = Serialization::decode_vstr(bufp, remainp);
        String s2 = Serialization::decode_vstr(bufp, remainp);
        m_repmaster->m_table_names[s1] = s2;
        HT_INFOF("Adding table name %s = %s", s1.c_str(), s2.c_str());
      }

      m_repmaster->m_unassigned_fragments.clear();

      // the list of unassigned fragments
      len = Serialization::decode_i32(bufp, remainp);
      for (size_t i = 0; i < len; i++) {
        String s = Serialization::decode_vstr(bufp, remainp);
        m_repmaster->m_unassigned_fragments.insert(s);
        HT_INFOF("Adding unassigned fragment %s", s.c_str());
      }

      // the list of assigned fragments is not stored; they are re-assigned
      // when restarting
      m_repmaster->m_assigned_fragments.clear();

      m_repmaster->m_finished_fragments.clear();

      // the list of finished fragments
      len = Serialization::decode_i32(bufp, remainp);
      for (size_t i = 0; i < len; i++) {
        String s = Serialization::decode_vstr(bufp, remainp);
        m_repmaster->m_finished_fragments.insert(s);
        HT_INFOF("Adding finished fragment %s", s.c_str());
      }

      m_repmaster->m_known_clusters.clear();

      // the list of remote clusters
      len = Serialization::decode_i32(bufp, remainp);
      for (size_t i = 0; i < len; i++) {
        String s = Serialization::decode_vstr(bufp, remainp);
        m_repmaster->m_known_clusters.insert(s);
        HT_INFOF("Adding remote cluster %s", s.c_str());
      }

      m_repmaster->m_linked_logs.clear();

      // all the linked log directories
      len = Serialization::decode_i32(bufp, remainp);
      for (size_t i = 0; i < len; i++) {
        String s = Serialization::decode_vstr(bufp, remainp);
        m_repmaster->m_linked_logs.insert(s);
        HT_INFOF("Adding linked log directory %s", s.c_str());
      }
    }

    virtual void display(std::ostream &os) {
      os << "ReplicationMasterEntity";
    }

  private:
    ReplicationMaster *m_repmaster;
};

class ReplicationMasterDefinition : public MetaLog::Definition {
  public:
    ReplicationMasterDefinition(const char *backup_name)
    : MetaLog::Definition(backup_name) {
    }

    virtual uint16_t version() { return 1; }

    virtual bool supported_version(uint16_t ver) { return (ver == 1); }

    virtual const char *name() { return "repmaster"; }

    virtual MetaLog::Entity *create(uint16_t log_version,
            const MetaLog::EntityHeader &header) {
      if (header.type == ReplicationMasterEntity::ENTITY_ID)
        return new ReplicationMasterEntity(header);

      HT_ASSERT(!"shouldn't be here");
      return 0;
    }
};

} // namespace Hypertable

#endif // REPLICATION_MASTER_ENTITY_H
