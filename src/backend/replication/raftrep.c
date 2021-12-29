/*-------------------------------------------------------------------------
 *
 * raftrep.c
 *
 * Transaction commits wait until their commit LSN are
 * acknowledged by majority of the raft quorum.
 *
 * This module contains the code for waiting and release of backends.
 *
 * IDENTIFICATION
 *	  src/backend/replication/raftrep.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "replication/raftrep.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/ps_status.h"

/* Pointer of the global raft xlog sync state in shared memory */
RaftWalSndCtlData *RaftWalSndCtl = NULL;

static void RaftRepQueueInsert(void);
static void RaftRepCancelWait(void);
static int	RaftRepWakeQueue(bool all);

#ifdef USE_ASSERT_CHECKING
static bool RaftRepQueueIsOrderedByLSN(void);
#endif

/*
 * ===========================================================
 * Raft Replication functions for postmaster
 * ===========================================================
 */

/* Report shared-memory space needed by RaftWalSndShmemInit */
Size
RaftWalSndShmemSize(void)
{
	return sizeof(RaftWalSndCtlData);
}

/* Allocate and initialize raft xlog sync state in shared memory */
void
RaftWalSndShmemInit(void)
{
	bool found;

	RaftWalSndCtl = (RaftWalSndCtlData *)
		ShmemInitStruct("Raft Wal Sender Ctl", RaftWalSndShmemSize(), &found);

	if (!found)
	{
		/* First time through, so initialize */
		MemSet(RaftWalSndCtl, 0, RaftWalSndShmemSize());
		SHMQueueInit(&(RaftWalSndCtl->RaftRepQueue));
	}
}

/*
 * ===========================================================
 * Raft Replication functions for normal user backends
 * ===========================================================
 */

/*
 * Wait for raft replication.
 *
 * Initially backends start in state RAFT_REP_NOT_WAITING and then
 * change that state to RAFT_REP_WAITING before adding ourselves
 * to the wait queue. RaftRepWakeQueue() called by raftserver changes
 * the state to RAFT_REP_WAIT_COMPLETE once replication is confirmed.
 * This backend then resets its state to RAFT_REP_NOT_WAITING.
 *
 * 'lsn' represents the LSN to wait for.
 */
void
RaftRepWaitForLSN(XLogRecPtr lsn)
{
	char	   *new_status = NULL;
	const char *old_status;

	Assert(SHMQueueIsDetached(&(MyProc->raftRepLinks)));
	Assert(RaftWalSndCtl != NULL);

	LWLockAcquire(RaftRepLock, LW_EXCLUSIVE);
	Assert(MyProc->raftRepState == RAFT_REP_NOT_WAITING);

	if (lsn <= RaftWalSndCtl->lsn)
	{
		LWLockRelease(RaftRepLock);
		return;
	}

	/*
	 * Set our raftWaitLSN so raftserver will know when to wake us, and add
	 * ourselves to the queue.
	 */
	MyProc->raftWaitLSN = lsn;
	MyProc->raftRepState = RAFT_REP_WAITING;
	RaftRepQueueInsert();
	Assert(RaftRepQueueIsOrderedByLSN());
	LWLockRelease(RaftRepLock);

	/* Alter ps display to show waiting for raft rep. */
	if (update_process_title)
	{
		int			len;

		old_status = get_ps_display(&len);
		new_status = (char *) palloc(len + 32 + 1);
		memcpy(new_status, old_status, len);
		sprintf(new_status + len, " waiting for %X/%X",
				(uint32) (lsn >> 32), (uint32) lsn);
		set_ps_display(new_status, false);
		new_status[len] = '\0'; /* truncate off " waiting ..." */
	}

	/*
	 * Wait for specified LSN to be confirmed.
	 *
	 * Each proc has its own wait latch, so we perform a normal latch
	 * check/wait loop here.
	 */
	for (;;)
	{
		int			rc;

		/* Must reset the latch before testing state. */
		ResetLatch(MyLatch);

		/*
		 * Acquiring the lock is not needed, the latch ensures proper
		 * barriers. If it looks like we're done, we must really be done,
		 * because once raftserver changes the state to RAFT_REP_WAIT_COMPLETE,
		 * it will never update it again, so we can't be seeing a stale value
		 * in that case.
		 */
		if (MyProc->raftRepState == RAFT_REP_WAIT_COMPLETE)
			break;

		/*
		 * If a wait for raft replication is pending, we can neither
		 * acknowledge the commit nor raise ERROR or FATAL.  The latter would
		 * lead the client to believe that the transaction aborted, which is
		 * not true: it's already committed locally. The former is no good
		 * either: the client has requested raft replication, and is
		 * entitled to assume that an acknowledged commit is also replicated,
		 * which might not be true. So in this case we issue a WARNING (which
		 * some clients may be able to interpret) and shut off further output.
		 * We do NOT reset ProcDiePending, so that the process will die after
		 * the commit is cleaned up.
		 */
		if (ProcDiePending)
		{
			ereport(WARNING,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("canceling the wait for raft replication and terminating connection due to administrator command"),
					 errdetail("The transaction has already committed locally, but might not have been replicated to other raft servers.")));
			whereToSendOutput = DestNone;
			RaftRepCancelWait();
			break;
		}

		/*
		 * It's unclear what to do if a query cancel interrupt arrives.  We
		 * can't actually abort at this point, but ignoring the interrupt
		 * altogether is not helpful, so we just terminate the wait with a
		 * suitable warning.
		 */
		if (QueryCancelPending)
		{
			QueryCancelPending = false;
			ereport(WARNING,
					(errmsg("canceling wait for raft replication due to user request"),
					 errdetail("The transaction has already committed locally, but might not have been replicated to other raft servers.")));
			RaftRepCancelWait();
			break;
		}

		/*
		 * Wait on latch.  Any condition that should wake us up will set the
		 * latch, so no need for timeout.
		 */
		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, -1,
					   WAIT_EVENT_RAFT_REP);

		/*
		 * If the postmaster dies, we'll probably never get an acknowledgment,
		 * because raftserver processe will exit. So just bail out.
		 */
		if (rc & WL_POSTMASTER_DEATH)
		{
			ProcDiePending = true;
			whereToSendOutput = DestNone;
			RaftRepCancelWait();
			break;
		}
	}

	/*
	 * raftserver has checked our LSN and has removed us from queue. Clean up
	 * state and leave.  It's OK to reset these shared memory fields without
	 * holding RaftRepLock, because raftserver will ignore us anyway when
	 * we're not on the queue.  We need a read barrier to make sure we see the
	 * changes to the queue link (this might be unnecessary without
	 * assertions, but better safe than sorry).
	 */
	pg_read_barrier();
	Assert(SHMQueueIsDetached(&(MyProc->raftRepLinks)));
	MyProc->raftRepState = RAFT_REP_NOT_WAITING;
	MyProc->raftWaitLSN = 0;

	if (new_status)
	{
		/* Reset ps display */
		set_ps_display(new_status, false);
		pfree(new_status);
	}
}

/*
 * Insert MyProc into the RaftRepQueue, maintaining sorted invariant.
 *
 * Usually we will go at tail of queue, though it's possible that we arrive
 * here out of order, so start at tail and work back to insertion point.
 */
static void
RaftRepQueueInsert()
{
	PGPROC	   *proc;

	proc = (PGPROC *) SHMQueuePrev(&(RaftWalSndCtl->RaftRepQueue),
								   &(RaftWalSndCtl->RaftRepQueue),
								   offsetof(PGPROC, raftRepLinks));

	while (proc)
	{
		/*
		 * Stop at the queue element that we should after to ensure the queue
		 * is ordered by LSN.
		 */
		if (proc->raftWaitLSN < MyProc->raftWaitLSN)
			break;

		proc = (PGPROC *) SHMQueuePrev(&(RaftWalSndCtl->RaftRepQueue),
									   &(proc->raftRepLinks),
									   offsetof(PGPROC, raftRepLinks));
	}

	if (proc)
		SHMQueueInsertAfter(&(proc->raftRepLinks), &(MyProc->raftRepLinks));
	else
		SHMQueueInsertAfter(&(RaftWalSndCtl->RaftRepQueue), &(MyProc->raftRepLinks));
}

/*
 * Acquire RaftRepLock and cancel any wait currently in progress.
 */
static void
RaftRepCancelWait(void)
{
	LWLockAcquire(RaftRepLock, LW_EXCLUSIVE);
	if (!SHMQueueIsDetached(&(MyProc->raftRepLinks)))
		SHMQueueDelete(&(MyProc->raftRepLinks));
	MyProc->raftRepState = RAFT_REP_NOT_WAITING;
	LWLockRelease(RaftRepLock);
}

void
RaftRepCleanupAtProcExit(void)
{
	/*
	 * First check if we are removed from the queue without the lock to not
	 * slow down backend exit.
	 */
	if (!SHMQueueIsDetached(&(MyProc->raftRepLinks)))
	{
		LWLockAcquire(RaftRepLock, LW_EXCLUSIVE);

		/* maybe we have just been removed, so recheck */
		if (!SHMQueueIsDetached(&(MyProc->raftRepLinks)))
			SHMQueueDelete(&(MyProc->raftRepLinks));

		LWLockRelease(RaftRepLock);
	}
}

/*
 * ===========================================================
 * Raft Replication functions for raftserver auxiliary process
 * ===========================================================
 */

/*
 * Take any action required to initialise raft rep state from config
 * data. Called at raftserver startup.
 */
void
RaftRepInitConfig(void)
{
	/* TODO: init the raft server list and id here */
}

/*
 * Update the LSN on the wait queue based upon our latest state, and wakeup
 * backends.
 */
void
RaftRepReleaseWaiters(void)
{
	int num = 0;

	LWLockAcquire(RaftRepLock, LW_EXCLUSIVE);

	num = RaftRepWakeQueue(false);

	LWLockRelease(RaftRepLock);

	elog(DEBUG3, "released %d procs", num);
}

/*
 * Walk the wait queue from head.  Set the state of any backends that
 * need to be woken, remove them from the queue, and then wake them.
 * Pass all = true to wake whole queue; otherwise, just wake up to
 * the raftserver's LSN.
 *
 * Must hold RaftRepLock.
 */
static int
RaftRepWakeQueue(bool all)
{
	volatile RaftWalSndCtlData *walsndctl = RaftWalSndCtl;
	PGPROC	   *proc = NULL;
	PGPROC	   *thisproc = NULL;
	int			numprocs = 0;

	Assert(RaftRepQueueIsOrderedByLSN());

	proc = (PGPROC *) SHMQueueNext(&(RaftWalSndCtl->RaftRepQueue),
								   &(RaftWalSndCtl->RaftRepQueue),
								   offsetof(PGPROC, raftRepLinks));

	while (proc)
	{
		/*
		 * Assume the queue is ordered by LSN
		 */
		if (!all && walsndctl->lsn < proc->raftWaitLSN)
			return numprocs;

		/*
		 * Move to next proc, so we can delete thisproc from the queue.
		 * thisproc is valid, proc may be NULL after this.
		 */
		thisproc = proc;
		proc = (PGPROC *) SHMQueueNext(&(RaftWalSndCtl->RaftRepQueue),
									   &(proc->raftRepLinks),
									   offsetof(PGPROC, raftRepLinks));

		/*
		 * Remove thisproc from queue.
		 */
		SHMQueueDelete(&(thisproc->raftRepLinks));

		/*
		 * RaftRepWaitForLSN() reads raftRepState without holding the lock, so
		 * make sure that it sees the queue link being removed before the
		 * raftRepState change.
		 */
		pg_write_barrier();

		/*
		 * Set state to complete; see RaftRepWaitForLSN() for discussion of
		 * the various states.
		 */
		thisproc->raftRepState = RAFT_REP_WAIT_COMPLETE;

		/*
		 * Wake only when we have set state and removed from queue.
		 */
		SetLatch(&(thisproc->procLatch));

		numprocs++;
	}

	return numprocs;
}

#ifdef USE_ASSERT_CHECKING
static bool
RaftRepQueueIsOrderedByLSN(void)
{
	PGPROC	   *proc = NULL;
	XLogRecPtr	lastLSN;

	lastLSN = 0;

	proc = (PGPROC *) SHMQueueNext(&(RaftWalSndCtl->RaftRepQueue),
								   &(RaftWalSndCtl->RaftRepQueue),
								   offsetof(PGPROC, raftRepLinks));

	while (proc)
	{
		/*
		 * Check the queue is ordered by LSN and that multiple procs don't
		 * have matching LSNs
		 */
		if (proc->raftWaitLSN <= lastLSN)
			return false;

		lastLSN = proc->raftWaitLSN;

		proc = (PGPROC *) SHMQueueNext(&(RaftWalSndCtl->RaftRepQueue),
									   &(proc->raftRepLinks),
									   offsetof(PGPROC, raftRepLinks));
	}

	return true;
}
#endif
