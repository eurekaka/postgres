/*-------------------------------------------------------------------------
 *
 * raftserver.h
 *	  Exports from postmaster/raftserver.c.
 *
 * src/include/postmaster/raftserver.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _RAFTSERVER_H
#define _RAFTSERVER_H

extern int raft_server_id;

extern void RaftServerMain(void) pg_attribute_noreturn();

#endif							/* _RAFTSERVER_H */
