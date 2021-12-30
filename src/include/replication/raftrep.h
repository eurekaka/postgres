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
	/* Raft replication wait queue */
	SHM_QUEUE   RaftRepQueue;
	/* Current location of the xlog acknowledged by raft quorum */
	XLogRecPtr  lsn;
} RaftWalSndCtlData;

/* Pointer of the global raft xlog sync state in shared memory */
extern RaftWalSndCtlData *RaftWalSndCtl;

/* Allocate and initialize raft xlog sync state in shared memory */
extern Size RaftWalSndShmemSize(void);
extern void RaftWalSndShmemInit(void);

/* called by user backend */
extern void RaftRepWaitForLSN(XLogRecPtr lsn);

/* called at backend exit */
extern void RaftRepCleanupAtProcExit(void);

/* called by raftserver */
extern void RaftRepInitConfig(void);
extern void RaftRepReleaseWaiters(void);
extern void SetRaftWalSndCtlLSN(XLogRecPtr lsn);

#endif							/* _RAFTREP_H */
