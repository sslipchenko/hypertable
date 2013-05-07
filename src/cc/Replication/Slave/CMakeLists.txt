#
# Copyright (C) 2007-2012 Hypertable, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#

set(RSlave_SRCS
    ConnectionHandler.cc
    main.cc
    TimerHandler.cc
    ReplicationSlave.cc
    RequestHandlerProcessFragment.cc
)

# Replication.Slave
add_executable(Replication.Slave ${RSlave_SRCS})
target_link_libraries(Replication.Slave Hypertable HyperCommon HyperComm
    ${BOOST_THREAD_LIB} ${BOOST_SYSTEM_LIB} ${BOOST_PROGRAM_OPTIONS_LIB}
    ${BOOST_FILESYSTEM_LIB} ${BOOST_IOSTREAMS_LIB} ${RE2_LIBRARY}
    ${SIGAR_LIBRARY} ${SNAPPY_LIBRARY} ${Log4cpp_LIBRARY} ${EDITLINE_LIBRARIES}
    ${MALLOC_LIBRARY} ncurses crypto z bz2 expat)

install(PROGRAMS
    ${HYPERTABLE_BINARY_DIR}/src/cc/Replication/Slave/Replication.Slave
    DESTINATION bin)
