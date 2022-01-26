/*-------------------------------------------------------------------------
 *
 * raftrep.h
 *	  Exports from replication/raftrep.c.
 *
 * IDENTIFICATION
 *		src/include/replication/raftrep.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _RAFTREP_H
#define _RAFTREP_H

#include "access/xlogdefs.h"
#include "storage/shmem.h"

/* Values for PGPROC.raftRepState */
#define RAFT_REP_NOT_WAITING		0
#define RAFT_REP_WAITING			1
#define RAFT_REP_WAIT_COMPLETE		2

/* State of the xlog sync among raft quorum, protected by RaftRepLock */
typedef struct
{
	/* Raft replication wait queue, used by leader */
	SHM_QUEUE  RaftRepQueue;

	/* Location of the xlog end+1 acknowledged by raft quorum majority,
	 * used by leader*/
	XLogRecPtr committedRecEnd;

	/* Location of the xlog end+1 received and fsynced, used by followers */
	XLogRecPtr rcvFlushRecEnd;

	/*
	 * Pointer to the raftserver's latch. Used by backends / startup process
	 * to wake up raftserver.
	 */
	Latch      *latch;
} RaftRepCtlData;

/* Pointer of the global raft xlog sync state in shared memory */
extern RaftRepCtlData *RaftRepCtl;

/* State for RaftWalSndWakeupRequest */
extern bool wakeup_raft_server;

/* Allocate and initialize raft xlog sync state in shared memory */
extern Size RaftRepShmemSize(void);
extern void RaftRepShmemInit(void);

/* called by user backend */
extern void RaftRepWaitForLSN(XLogRecPtr lsn);
extern void RaftRepServerWakeup(void);

/*
 * Remember that we want to wakeup raftserver later
 *
 * This is separated from doing the actual wakeup because the xlog writeout
 * is done while holding contended locks.
 */
#define RaftWalSndWakeupRequest() \
	do { wakeup_raft_server = true; } while (0)

/* wakeup raftserver if there is work to be done */
#define RaftWalSndWakeupProcessRequests()	\
	do										\
	{										\
		if (wakeup_raft_server)				\
		{									\
			wakeup_raft_server = false;		\
			RaftRepServerWakeup();			\
		}									\
	} while (0)

/* called at backend exit */
extern void RaftRepCleanupAtProcExit(void);

/* called by raftserver */
extern void RaftRepInit(void);
extern void RaftRepReset(void);
extern void RaftRepReleaseWaiters(void);
extern void RaftRepSetCommittedRecEnd(XLogRecPtr lsn);
extern bool RaftRepGetRecordsForSend(XLogRecPtr lsn, StringInfo buf);
extern XLogRecPtr RaftRepWriteRecords(char *buf, Size nbytes);

/* TODO: called by startup process */
void RaftRepInitRecEnd(XLogRecPtr lsn);

#endif							/* _RAFTREP_H */
