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


#ifndef HYPERTABLE_IOHANDLER_H
#define HYPERTABLE_IOHANDLER_H

extern "C" {
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <poll.h>
#if defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/event.h>
#elif defined(__linux__)
#include <sys/epoll.h>
#if !defined(POLLRDHUP)
#define POLLRDHUP 0x2000
#endif
#elif defined(__sun__)
#include <port.h>
#include <sys/port_impl.h>
#include <unistd.h>
#endif
}

#include "Common/Logger.h"
#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/Time.h"

#include "DispatchHandler.h"
#include "ReactorFactory.h"
#include "ExpireTimer.h"

namespace Hypertable {

  /**
   *
   */
  class IOHandler : public ReferenceCount {

  public:

    IOHandler(int sd, const InetAddr &addr, DispatchHandlerPtr &dhp)
      : m_reference_count(0), m_free_flag(0), m_error(Error::OK), m_addr(addr),
        m_sd(sd), m_dispatch_handler(dhp), m_decomissioned(false) {
      ReactorFactory::get_reactor(m_reactor);
      m_poll_interest = 0;
      socklen_t namelen = sizeof(m_local_addr);
      getsockname(m_sd, (sockaddr *)&m_local_addr, &namelen);
      memset(&m_alias, 0, sizeof(m_alias));
    }

    IOHandler(int sd, DispatchHandlerPtr &dhp)
      : m_reference_count(0), m_free_flag(0), m_error(Error::OK),
        m_sd(sd), m_dispatch_handler(dhp), m_decomissioned(false) {
      ReactorFactory::get_reactor(m_reactor);
      m_poll_interest = 0;
      socklen_t namelen = sizeof(m_local_addr);
      getsockname(m_sd, (sockaddr *)&m_local_addr, &namelen);
      memset(&m_alias, 0, sizeof(m_alias));
    }

    // define default poll() interface for everyone since it is chosen at runtime
    virtual bool handle_event(struct pollfd *event, time_t arival_time=0) = 0;

#if defined(__APPLE__) || defined(__FreeBSD__)
    virtual bool handle_event(struct kevent *event, time_t arival_time=0) = 0;
#elif defined(__linux__)
    virtual bool handle_event(struct epoll_event *event, time_t arival_time=0) = 0;
#elif defined(__sun__)
    virtual bool handle_event(port_event_t *event, time_t arival_time=0) = 0;
#else
    ImplementMe;
#endif

    virtual ~IOHandler() {
      HT_EXPECT(m_free_flag != 0xdeadbeef, Error::FAILED_EXPECTATION);
      m_free_flag = 0xdeadbeef;
      ::close(m_sd);
      return;
    }

    void deliver_event(Event *event) {
      memcpy(&event->local_addr, &m_local_addr, sizeof(m_local_addr));
      if (!m_dispatch_handler) {
        HT_INFOF("%s", event->to_str().c_str());
        delete event;
      }
      else {
        EventPtr event_ptr(event);
        m_dispatch_handler->handle(event_ptr);
      }
    }

    void deliver_event(Event *event, DispatchHandler *dh) {
      memcpy(&event->local_addr, &m_local_addr, sizeof(m_local_addr));
      if (!dh) {
        if (!m_dispatch_handler) {
          HT_INFOF("%s", event->to_str().c_str());
          delete event;
        }
        else {
          EventPtr event_ptr(event);
          m_dispatch_handler->handle(event_ptr);
        }
      }
      else {
        EventPtr event_ptr(event);
        dh->handle(event_ptr);
      }
    }

    int start_polling(int mode=Reactor::READ_READY) {
      if (ReactorFactory::use_poll) {
	m_poll_interest = mode;
	return m_reactor->add_poll_interest(m_sd, poll_events(mode), this);
      }
#if defined(__APPLE__) || defined(__sun__) || defined(__FreeBSD__)
      return add_poll_interest(mode);
#elif defined(__linux__)
      struct epoll_event event;
      memset(&event, 0, sizeof(struct epoll_event));
      event.data.ptr = this;
      if (mode & Reactor::READ_READY)
        event.events |= EPOLLIN;
      if (mode & Reactor::WRITE_READY)
        event.events |= EPOLLOUT;
      if (ReactorFactory::ms_epollet)
        event.events |= POLLRDHUP | EPOLLET;
      m_poll_interest = mode;
      if (epoll_ctl(m_reactor->poll_fd, EPOLL_CTL_ADD, m_sd, &event) < 0) {
        HT_ERRORF("epoll_ctl(%d, EPOLL_CTL_ADD, %d, %x) failed : %s",
                  m_reactor->poll_fd, m_sd, event.events, strerror(errno));
        return Error::COMM_POLL_ERROR;
      }
#endif
      return Error::OK;
    }

    int add_poll_interest(int mode);

    int remove_poll_interest(int mode);

    int reset_poll_interest() {
      return add_poll_interest(m_poll_interest);
    }

    InetAddr &get_address() { return m_addr; }

    InetAddr &get_local_address() { return m_local_addr; }

    void get_local_address(InetAddr *addrp) {
      *addrp = m_local_addr;
    }

    void set_alias(const InetAddr &alias) {
      m_alias = alias;
    }

    void get_alias(InetAddr *aliasp) {
      *aliasp = m_alias;
    }

    void set_proxy(const String &proxy) {
      ScopedLock lock(m_mutex);
      m_proxy = proxy;
    }

    String get_proxy() {
      ScopedLock lock(m_mutex);
      return m_proxy;
    }

    void set_dispatch_handler(DispatchHandler *dh) {
      ScopedLock lock(m_mutex);
      m_dispatch_handler = dh;
    }

    int get_sd() { return m_sd; }

    void get_reactor(ReactorPtr &reactor) { reactor = m_reactor; }

    void display_event(struct pollfd *event);

#if defined(__APPLE__) || defined(__FreeBSD__)
    void display_event(struct kevent *event);
#elif defined(__linux__)
    void display_event(struct epoll_event *event);
#elif defined(__sun__)
    void display_event(port_event_t *event);
#endif

    void lock() {
      m_mutex.lock();
    }

    void unlock() {
      m_mutex.unlock();
    }

    friend class HandlerMap;

  protected:

    void increment_reference_count() {
      m_reference_count++;
    }

    void decrement_reference_count() {
      HT_ASSERT(m_reference_count > 0);
      m_reference_count--;
      if (m_reference_count == 0 && m_decomissioned) {
        ExpireTimer timer;
        m_reactor->schedule_removal(this);
        boost::xtime_get(&timer.expire_time, boost::TIME_UTC_);
        timer.expire_time.nsec += 200000000LL;
        timer.handler = 0;
        m_reactor->add_timer(timer);
      }
    }

    size_t reference_count() const {
      return m_reference_count;
    }

    void decomission() {
      if (!m_decomissioned) {
        m_decomissioned = true;
        if (m_reference_count == 0) {
          ExpireTimer timer;
          m_reactor->schedule_removal(this);
          boost::xtime_get(&timer.expire_time, boost::TIME_UTC_);
          timer.expire_time.nsec += 200000000LL;
          timer.handler = 0;
          m_reactor->add_timer(timer);
        }
      }
    }

    bool is_decomissioned() {
      return m_decomissioned;
    }

    virtual void disconnect() { }

    short poll_events(int mode) {
      short events = 0;
      if (mode & Reactor::READ_READY)
	events |= POLLIN;
      if (mode & Reactor::WRITE_READY)
	events |= POLLOUT;
      return events;
    }

    void stop_polling() {
      if (ReactorFactory::use_poll) {
	m_poll_interest = 0;
	m_reactor->modify_poll_interest(m_sd, 0);
	return;
      }
#if defined(__APPLE__) || defined(__sun__) || defined(__FreeBSD__)
      remove_poll_interest(Reactor::READ_READY|Reactor::WRITE_READY);
#elif defined(__linux__)
      struct epoll_event event;  // this is necessary for < Linux 2.6.9
      if (epoll_ctl(m_reactor->poll_fd, EPOLL_CTL_DEL, m_sd, &event) < 0) {
        HT_ERRORF("epoll_ctl(%d, EPOLL_CTL_DEL, %d) failed : %s",
                     m_reactor->poll_fd, m_sd, strerror(errno));
        exit(1);
      }
      m_poll_interest = 0;
#endif
    }

    Mutex               m_mutex;
    size_t              m_reference_count;
    uint32_t            m_free_flag;
    int32_t             m_error;
    String              m_proxy;
    InetAddr            m_addr;
    InetAddr            m_local_addr;
    InetAddr            m_alias;
    int                 m_sd;
    DispatchHandlerPtr  m_dispatch_handler;
    ReactorPtr          m_reactor;
    int                 m_poll_interest;
    bool                m_decomissioned;
  };
  typedef boost::intrusive_ptr<IOHandler> IOHandlerPtr;

  struct ltiohp {
    bool operator()(const IOHandlerPtr &p1, const IOHandlerPtr &p2) const {
      return p1.get() < p2.get();
    }
  };

}


#endif // HYPERTABLE_IOHANDLER_H
