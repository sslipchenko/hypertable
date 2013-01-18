/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#ifndef HYPERTABLE_APPLICATIONQUEUE_H
#define HYPERTABLE_APPLICATIONQUEUE_H

#include <cassert>
#include <list>
#include <map>
#include <vector>

#include <boost/thread/condition.hpp>
#include <boost/thread/xtime.hpp>

#include "Common/Thread.h"
#include "Common/Mutex.h"
#include "Common/HashMap.h"
#include "Common/ReferenceCount.h"
#include "Common/StringExt.h"
#include "Common/Logger.h"

#include "ApplicationQueueInterface.h"
#include "ApplicationHandler.h"

namespace Hypertable {

  /** \addtogroup AsyncComm
   *  @{
   */

  /**
   * Application request queue.  
   * Generic queue service that can be used to buffer and execute incoming
   * application requests.
   */
  class ApplicationQueue : public ApplicationQueueInterface {

    /** Tracks thread group execution state.
     * A GroupState object is created for each unique thread group to track the
     * queue execution state of requests in the thread group.
     */
    class GroupState {
    public:
      GroupState() : thread_group(0), running(false), outstanding(1) { return; }
      uint64_t thread_group; //!< Thread group ID
      bool     running;      //!< Sentinal indicating if a request from this
                             //!< group is being executed
      int      outstanding;  //!< Number of outstanding (uncompleted) requests
                             //!< in queue for this thread group
    };

    /** Hash map of thread group ID to GroupState
     */ 
    typedef hash_map<uint64_t, GroupState *> GroupStateMap;

    /** Defines queue object with pointer to applcation handler and group state
     */
    class WorkRec {
    public:
      WorkRec(ApplicationHandler *ah) : handler(ah), group_state(0) { return; }
      ~WorkRec() { delete handler; }
      ApplicationHandler *handler;
      GroupState         *group_state;
    };

    /** Individual work queue
     */
    typedef std::list<WorkRec *> WorkQueue;

    /** Application queue state object shared among worker threads.
     */
    class ApplicationQueueState {
    public:
      ApplicationQueueState() : threads_available(0), shutdown(false),
                                paused(false) { }
      WorkQueue           queue;
      WorkQueue           urgent_queue;
      GroupStateMap       group_state_map;
      Mutex               mutex;
      boost::condition    cond;
      boost::condition    quiesce_cond;
      size_t              threads_available;
      size_t              threads_total;
      bool                shutdown;
      bool                paused;
    };

    /** Application queue worker thread function (functor)
     */
    class Worker {

    public:
      Worker(ApplicationQueueState &qstate, bool one_shot=false) 
      : m_state(qstate), m_one_shot(one_shot) { return; }

      void operator()() {
        WorkRec *rec = 0;
        WorkQueue::iterator iter;

        while (true) {
          {
            ScopedLock lock(m_state.mutex);

            m_state.threads_available++;
            while ((m_state.paused || m_state.queue.empty()) &&
                   m_state.urgent_queue.empty()) {
              if (m_state.shutdown) {
                m_state.threads_available--;
                return;
              }
              if (m_state.threads_available == m_state.threads_total)
                m_state.quiesce_cond.notify_all();
              m_state.cond.wait(lock);
            }

            if (m_state.shutdown) {
              m_state.threads_available--;
              return;
            }

            rec = 0;

            iter = m_state.urgent_queue.begin();
            while (iter != m_state.urgent_queue.end()) {
              rec = (*iter);
              if (rec->group_state == 0 || !rec->group_state->running) {
                if (rec->group_state)
                  rec->group_state->running = true;
                m_state.urgent_queue.erase(iter);
                break;
              }
              if (!rec->handler || rec->handler->expired()) {
                iter = m_state.urgent_queue.erase(iter);
                remove_expired(rec);
              }
              rec = 0;
              iter++;
            }

            if (rec == 0 && !m_state.paused) {
              iter = m_state.queue.begin();
              while (iter != m_state.queue.end()) {
                rec = (*iter);
                if (rec->group_state == 0 || !rec->group_state->running) {
                  if (rec->group_state)
                    rec->group_state->running = true;
                  m_state.queue.erase(iter);
                  break;
                }
                if (!rec->handler || rec->handler->expired()) {
                  iter = m_state.queue.erase(iter);
                  remove_expired(rec);
                }
                rec = 0;
                iter++;
              }
            }

            if (rec == 0 && !m_one_shot) {
              if (m_state.shutdown) {
                m_state.threads_available--;
                return;
              }
              m_state.cond.wait(lock);
              if (m_state.shutdown) {
                m_state.threads_available--;
                return;
              }
            }

            m_state.threads_available--;
          }

          if (rec) {
            if (rec->handler)
              rec->handler->run();
            remove(rec);
            if (m_one_shot)
              return;
          }
          else if (m_one_shot)
            return;
        }

        HT_INFO("thread exit");
      }

    private:

      void remove(WorkRec *rec) {
        if (rec->group_state) {
          ScopedLock ulock(m_state.mutex);
          rec->group_state->running = false;
          rec->group_state->outstanding--;
          if (rec->group_state->outstanding == 0) {
            m_state.group_state_map.erase(rec->group_state->thread_group);
            delete rec->group_state;
          }
        }
        delete rec;
      }

      void remove_expired(WorkRec *rec) {
        if (rec->group_state) {
          rec->group_state->outstanding--;
          if (rec->group_state->outstanding == 0) {
            m_state.group_state_map.erase(rec->group_state->thread_group);
            delete rec->group_state;
          }
        }
        delete rec;
      }

      ApplicationQueueState &m_state;
      bool m_one_shot;
    };

    ApplicationQueueState  m_state;
    ThreadGroup            m_threads;
    std::vector<Thread::id> m_thread_ids;
    bool joined;
    bool m_dynamic_threads;

  public:

    /**
     * Default ctor used by derived classes only
     */
    ApplicationQueue() : joined(true) {}

    /**
     * Constructor to set up the application queue.  It creates a number
     * of worker threads specified by the worker_count argument.
     *
     * @param worker_count number of worker threads to create
     */
    ApplicationQueue(int worker_count, bool dynamic_threads=true) 
      : joined(false), m_dynamic_threads(dynamic_threads) {
      m_state.threads_total = worker_count;
      Worker Worker(m_state);
      assert (worker_count > 0);
      for (int i=0; i<worker_count; ++i) {
        m_thread_ids.push_back(m_threads.create_thread(Worker)->get_id());
      }
      //threads
    }

    virtual ~ApplicationQueue() {
      if (!joined) {
        shutdown();
        join();
      }
    }

    /**
     * Return all the thread ids for this threadgroup
     *
     */
    std::vector<Thread::id> get_thread_ids() const {
      return m_thread_ids;
    }

    /**
     * Shuts down the application queue.  All outstanding requests are carried
     * out and then all threads exit.  #join can be called to wait for
     * completion of the shutdown.
     */
    void shutdown() {
      m_state.shutdown = true;
      m_state.cond.notify_all();
    }

    void wait_for_empty(int reserve_threads=0) {
      ScopedLock lock(m_state.mutex);
      while (m_state.threads_available < (m_state.threads_total-reserve_threads))
        m_state.quiesce_cond.wait(lock);
    }

    void wait_for_empty(boost::xtime &expire_time, int reserve_threads=0) {
      ScopedLock lock(m_state.mutex);
      while (m_state.threads_available < (m_state.threads_total-reserve_threads)) {
        if (!m_state.quiesce_cond.timed_wait(lock, expire_time))
          return;
      }
    }

    /**
     * Waits for a shutdown to complete.  This method returns when all
     * application queue threads exit.
     */

    void join() {
      if (!joined) {
        m_threads.join_all();
        joined = true;
      }
    }

    /** Starts application queue.
     */
    void start() {
      ScopedLock lock(m_state.mutex);
      m_state.paused = false;
      m_state.cond.notify_all();
    }

    /** Stops application queue allowing currently executing requests to
     * complete.
     */
    void stop() {
      ScopedLock lock(m_state.mutex);
      m_state.paused = true;
    }

    /**
     * Adds a request (application handler) to the application queue.  The request
     * queue is designed to support the serialization of related requests.
     * Requests are related by the thread group ID value in the
     * ApplicationHandler.  This thread group ID is constructed in the Event
     * object
     */
    virtual void add(ApplicationHandler *app_handler) {
      GroupStateMap::iterator uiter;
      uint64_t thread_group = app_handler->get_thread_group();
      WorkRec *rec = new WorkRec(app_handler);
      rec->group_state = 0;

      HT_ASSERT(app_handler);

      if (thread_group != 0) {
        ScopedLock ulock(m_state.mutex);
        if ((uiter = m_state.group_state_map.find(thread_group))
            != m_state.group_state_map.end()) {
          rec->group_state = (*uiter).second;
          rec->group_state->outstanding++;
        }
        else {
          rec->group_state = new GroupState();
          rec->group_state->thread_group = thread_group;
          m_state.group_state_map[thread_group] = rec->group_state;
        }
      }

      {
        ScopedLock lock(m_state.mutex);
        if (app_handler->is_urgent()) {
          m_state.urgent_queue.push_back(rec);
          if (m_dynamic_threads && m_state.threads_available == 0) {
            Worker worker(m_state, true);
            Thread t(worker);
          }
        }
        else
          m_state.queue.push_back(rec);
        m_state.cond.notify_one();
      }
    }

    virtual void add_unlocked(ApplicationHandler *app_handler) {
      add(app_handler);
    }
  };

  /// Smart pointer to ApplicationQueue object
  typedef boost::intrusive_ptr<ApplicationQueue> ApplicationQueuePtr;
  /** @}*/
} // namespace Hypertable

#endif // HYPERTABLE_APPLICATIONQUEUE_H
