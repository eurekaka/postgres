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
RaftRepCtlData *RaftRepCtl = NULL;

/* State for RaftWalSndWakeupRequest */
bool wakeup_raft_server = false;

/* Variables used by RaftRepXLogRead to track the reading progress */
static int sendFile = -1;
static XLogSegNo sendSegNo = 0;
static uint32 sendOff = 0;

/* Location of the xlog end+1 sent out, used by leader */
static XLogRecPtr SentRecEnd;

/* Variables used by RaftRepXLogWrite to track the writing progress */
static int recvFile = -1;
static XLogSegNo recvSegNo = 0;
static uint32 recvOff = 0;

/*
 * LogRcvResult indicates the byte positions that we have already
 * written/fsynced.
 */
static struct
{
	XLogRecPtr Write;			/* last byte + 1 written out */
	XLogRecPtr Flush;			/* last byte + 1 flushed */
} LogRcvResult;

static void RaftRepQueueInsert(void);
static void RaftRepCancelWait(void);
static int RaftRepWakeQueue(bool all);
static void RaftRepDestroy(void);
static void RaftRepXLogRead(char *buf, XLogRecPtr startPtr, Size count);

#ifdef USE_ASSERT_CHECKING
static bool RaftRepQueueIsOrderedByLSN(void);
#endif

static void RaftRepXLogWrite(char *buf, XLogRecPtr recptr, Size nbytes);
static void RaftRepXLogFlush();
static void RaftRepXLogClose(XLogRecPtr recptr);

/*
 * ===========================================================
 * Raft Replication functions for postmaster
 * ===========================================================
 */

/* Report shared-memory space needed by RaftRepShmemInit */
Size
RaftRepShmemSize(void)
{
	return sizeof(RaftRepCtlData);
}

/* Allocate and initialize raft xlog sync state in shared memory */
void
RaftRepShmemInit(void)
{
	bool found;

	RaftRepCtl = (RaftRepCtlData *)
		ShmemInitStruct("Raft Rep Ctl", RaftRepShmemSize(), &found);

	if (!found)
	{
		/* First time through, so initialize */
		MemSet(RaftRepCtl, 0, RaftRepShmemSize());
		SHMQueueInit(&(RaftRepCtl->RaftRepQueue));
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
	Assert(RaftRepCtl != NULL);

	LWLockAcquire(RaftRepLock, LW_EXCLUSIVE);
	Assert(MyProc->raftRepState == RAFT_REP_NOT_WAITING);

	if (lsn <= RaftRepCtl->committedRecEnd)
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
					 errmsg("canceling the wait for raft replication and \
							terminating connection due to administrator \
							command"),
					 errdetail("The transaction has already committed locally, \
								but might not have been replicated to other \
								raft servers.")));
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
					(errmsg("canceling wait for raft replication \
								due to user request"),
					 errdetail("The transaction has already committed locally, \
								but might not have been replicated to other \
								raft servers.")));
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

	proc = (PGPROC *) SHMQueuePrev(&(RaftRepCtl->RaftRepQueue),
								   &(RaftRepCtl->RaftRepQueue),
								   offsetof(PGPROC, raftRepLinks));

	while (proc)
	{
		/*
		 * Stop at the queue element that we should after to ensure the queue
		 * is ordered by LSN.
		 */
		if (proc->raftWaitLSN < MyProc->raftWaitLSN)
			break;

		proc = (PGPROC *) SHMQueuePrev(&(RaftRepCtl->RaftRepQueue),
									   &(proc->raftRepLinks),
									   offsetof(PGPROC, raftRepLinks));
	}

	if (proc)
		SHMQueueInsertAfter(&(proc->raftRepLinks), &(MyProc->raftRepLinks));
	else
		SHMQueueInsertAfter(&(RaftRepCtl->RaftRepQueue), &(MyProc->raftRepLinks));
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

void
RaftRepServerWakeup(void)
{
	Latch *latch;
	volatile RaftRepCtlData *raftrepctl = RaftRepCtl;
	Assert(raftrepctl != NULL);

	latch = raftrepctl->latch;
	if (latch != NULL)
		SetLatch(latch);
}

/*
 * ===========================================================
 * Raft Replication functions for raftserver auxiliary process
 * ===========================================================
 */

/*
 * Take any action required to initialise raft rep state. Called
 * at raftserver startup.
 */
void
RaftRepInit(void)
{
	/* TODO: init the raft server list and id here */
	volatile RaftRepCtlData *raftrepctl = RaftRepCtl;
	Assert(raftrepctl != NULL);

	/* No lock needed since we are in startup stage */
	raftrepctl->committedRecEnd = InvalidXLogRecPtr;
	raftrepctl->rcvFlushRecEnd = InvalidXLogRecPtr;
	raftrepctl->latch = &MyProc->procLatch;

	/* TODO: init this to EndOfLog, for SentRecEnd as well */
	LogRcvResult.Write = LogRcvResult.Flush = GetXLogReplayRecPtr(NULL);
	SentRecEnd = InvalidXLogRecPtr;

	on_shmem_exit(RaftRepDestroy, 0);
}

static void
RaftRepDestroy(void)
{
	volatile RaftRepCtlData *raftrepctl = RaftRepCtl;
	Assert(raftrepctl != NULL);

	/* No lock needed since we are shutting down */
	raftrepctl->committedRecEnd = InvalidXLogRecPtr;
	raftrepctl->rcvFlushRecEnd = InvalidXLogRecPtr;
	raftrepctl->latch = NULL;
	RaftValSndCtl = NULL;
}

/* Reset replication state when raft state changes */
void
RaftRepReset(void)
{
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

	elog(LOG, "released %d procs", num);
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
	volatile RaftRepCtlData *raftrepctl = RaftRepCtl;
	PGPROC	   *proc = NULL;
	PGPROC	   *thisproc = NULL;
	int			numprocs = 0;

	Assert(RaftRepQueueIsOrderedByLSN());

	proc = (PGPROC *) SHMQueueNext(&(RaftRepCtl->RaftRepQueue),
								   &(RaftRepCtl->RaftRepQueue),
								   offsetof(PGPROC, raftRepLinks));

	while (proc)
	{
		/*
		 * Assume the queue is ordered by LSN
		 */
		if (!all && raftrepctl->committedRecEnd < proc->raftWaitLSN)
			return numprocs;

		/*
		 * Move to next proc, so we can delete thisproc from the queue.
		 * thisproc is valid, proc may be NULL after this.
		 */
		thisproc = proc;
		proc = (PGPROC *) SHMQueueNext(&(RaftRepCtl->RaftRepQueue),
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

void
RaftRepSetCommittedRecEnd(XLogRecPtr lsn)
{
	LWLockAcquire(RaftRepLock, LW_EXCLUSIVE);

	volatile RaftRepCtlData *raftrepctl = RaftRepCtl;
	raftrepctl->committedRecEnd = lsn;

	LWLockRelease(RaftRepLock);

	elog(LOG, "update RaftRepCtl->committedRecEnd to %X/%X",
			(uint32) (lsn >> 32), (uint32) lsn);
}

void
RaftRepInitRecEnd(XLogRecPtr lsn)
{
	volatile RaftRepCtlData *raftrepctl = RaftRepCtl;

	LWLockAcquire(RaftRepLock, LW_EXCLUSIVE);
	raftrepctl->committedRecEnd = lsn;
	LWLockRelease(RaftRepLock);

	SentRecEnd = lsn;
	elog(LOG, "init SentRecEnd to %X/%X",
			(uint32) (lsn >> 32), (uint32) lsn);
}

/*
 * Maximum WAL data payload in a raft message.  Must be >= XLOG_BLCKSZ.
 *
 * We don't have a good idea of what a good value would be; there's some
 * overhead per message in raftserver, but on the other hand sending large
 * batches may not fully utilize the raft majority quorum settings.
 * 128kB (with default 8k blocks) seems like a reasonable guess for now.
 */
#define MAX_RAFT_SEND_SIZE (XLOG_BLCKSZ * 16)

static inline void
encodeRecPtr(StringInfo buf, XLogRecPtr lsn)
{
	uint64 ni = pg_hton64((uint64) lsn);
	enlargeStringInfo(buf, sizeof(ni));
	memcpy((char *) (buf->data + buf->len), &ni, sizeof(ni));
	buf->len += sizeof(ni);
}

static inline void
decodeRecPtr(char **buf, XLogRecPtr *lsn)
{
	uint64 ni;
	memcpy(&ni, *buf, sizeof(ni))
	*buf = (*buf) + sizeof(ni);
	*lsn = (XlogRecPtr) pg_ntoh64(ni);
}

/*
 * Read 'count' bytes from WAL into 'buf', starting at location 'startPtr'
 *
 * Will open, and keep open, one WAL segment stored in the global file
 * descriptor sendFile. This means if XLogRead is used once, there will
 * always be one descriptor left open until the process ends, but never
 * more than one.
 */
static void
RaftRepXLogRead(char *buf, XLogRecPtr startPtr, Size count)
{
	char	   *p;
	XLogRecPtr	recPtr;
	Size		nbytes;
	XLogSegNo	segno;

	p = buf;
	recPtr = startPtr;
	nbytes = count;

	while (nbytes > 0)
	{
		uint32		startOff;
		int			segBytes;
		int			readBytes;

		startOff = XLogSegmentOffset(recPtr, wal_segment_size);

		if (sendFile < 0 || !XLByteInSeg(recPtr, sendSegNo, wal_segment_size))
		{
			char path[MAXPGPATH];

			/* Switch to a new WAL segment */
			if (sendFile >= 0)
				close(sendFile);

			XLByteToSeg(recPtr, sendSegNo, wal_segment_size);

			/* We does not consider timeline switch now, i.e, PITR does not
			 * work when using raft replication */
			XLogFilePath(path, ThisTimeLineID, sendSegNo, wal_segment_size);

			sendFile = BasicOpenFile(path, O_RDONLY | PG_BINARY);
			if (sendFile < 0)
			{
				/*
				 * If the file is not found, i.e, the WAL segment has been
				 * removed or recycled before we replicate it, panic to
				 * transfer the leadership to other servers. Normally
				 * this should not happen.
				 */
				if (errno == ENOENT)
					ereport(PANIC,
							(errcode_for_file_access(),
							 errmsg("requested WAL segment %s has already been removed",
									XLogFileNameP(ThisTimeLineID, sendSegNo))));
				else
					ereport(PANIC,
							(errcode_for_file_access(),
							 errmsg("could not open file \"%s\": %m",
									path)));
			}
			sendOff = 0;
		}

		/* Need to seek in the file? */
		if (sendOff != startOff)
		{
			if (lseek(sendFile, (off_t) startOff, SEEK_SET) < 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not seek in log segment %s to offset %u: %m",
								XLogFileNameP(ThisTimeLineID, sendSegNo),
								startOff)));
			sendOff = startOff;
		}

		/* How many bytes are within this segment? */
		if (nbytes > (wal_segment_size - startOff))
			segBytes = wal_segment_size - startOff;
		else
			segBytes = nbytes;

		readBytes = read(sendFile, p, segBytes);
		if (readBytes < 0)
		{
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not read from log segment %s, offset %u, length %zu: %m",
							XLogFileNameP(ThisTimeLineID, sendSegNo),
							sendOff, (Size) segBytes)));
		}
		else if (readBytes == 0)
		{
			ereport(PANIC,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read from log segment %s, offset %u: read %d of %zu",
							XLogFileNameP(ThisTimeLineID, sendSegNo),
							sendOff, readBytes, (Size) segBytes)));
		}

		/* Update state for read */
		recPtr += readBytes;
		sendOff += readBytes;
		nbytes -= readBytes;
		p += readBytes;
	}

	/*
	 * After reading into the buffer, check that what we read was valid. We do
	 * this after reading, because even though the segment was present when we
	 * opened it, it might get recycled or removed while we read it. The
	 * read() succeeds in that case, but the data we tried to read might
	 * already have been overwritten with new WAL records.
	 */
	XLByteToSeg(startPtr, segno, wal_segment_size);
	/* EUREKA TODO: catch the error and panic? */
	CheckXLogRemoved(segno, ThisTimeLineID);
}

bool
RaftRepGetRecordsForSend(XLogRecPtr lsn, StringInfo buf)
{
	Size nbytes;
	XLogRecPtr sentRecEnd, startPtr, endPtr;
	volatile RaftRepCtlData *raftrepctl = RaftRepCtl;

	if (XLogRecPtrIsInvalid(lsn))
		return false;

	sentRecEnd = SentRecEnd;
	if (XLogRecPtrIsInvalid(sentRecEnd))
		return false;
	Assert(sentRecEnd <= lsn);

	/* No more xlog record to send */
	if (lsn == sentRecEnd)
		return false;

	/*
	 * Figure out how much to send in one RPC. If there's no more than
	 * MAX_RAFT_SEND_SIZE bytes to send, send everything. Otherwise send
	 * MAX_RAFT_SEND_SIZE bytes, but round back to logfile or page boundary.
	 *
	 * The rounding is not only for performance reasons. raftserver relies on
	 * the fact that we never split a WAL record across two RPCs. Since a
	 * long WAL record is split at page boundary into continuation records,
	 * page boundary is always a safe cut-off point. We also assume that
	 * passed in target lsn never points to the middle of a WAL record.
	 */
	startPtr = sentRecEnd;
	endPtr = startPtr + MAX_RAFT_SEND_SIZE;

	/* if we went beyond taget lsn, back off */
	if (lsn <= endPtr)
		endPtr = lsn;
	else
		/* round down to page boundary. */
		endPtr -= (endPtr % XLOG_BLCKSZ);

	nbytes = endPtr - startPtr;
	Assert(nbytes <= MAX_RAFT_SEND_SIZE && nbytes > 0);

	/* OK to read and send the xlog records */
	resetStringInfo(buf);
	/* Include the start / end+1 lsn of the xlog record in the message */
	encodeRecPtr(buf, startPtr);
	encodeRecPtr(buf, endPtr);

	/* Read the xlog directly into the buffer to avoid extra memcpy calls */
	enlargeStringInfo(buf, nbytes);
	RaftRepXLogRead(&buf->data[buf->len], startPtr, nbytes);
	buf->len += nbytes;
	buf->data[buf->len] = '\0';

	SentRecEnd = endPtr;
	/* Update LogRcvResult as well, to avoid writing duplicate XLOG records
	 * in leader */
	LogRcvResult.Write = LogRcvResult.Flush = endPtr;

	/* Report progress of XLOG streaming in PS display */
	if (update_process_title)
	{
		char activitymsg[50];

		snprintf(activitymsg, sizeof(activitymsg), "sending %X/%X",
				 (uint32) (endPtr >> 32), (uint32) endPtr);
		set_ps_display(activitymsg, false);
	}

	return true;
}

#ifdef USE_ASSERT_CHECKING
static bool
RaftRepQueueIsOrderedByLSN(void)
{
	PGPROC	   *proc = NULL;
	XLogRecPtr	lastLSN;

	lastLSN = 0;

	proc = (PGPROC *) SHMQueueNext(&(RaftRepCtl->RaftRepQueue),
								   &(RaftRepCtl->RaftRepQueue),
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

		proc = (PGPROC *) SHMQueueNext(&(RaftRepCtl->RaftRepQueue),
									   &(proc->raftRepLinks),
									   offsetof(PGPROC, raftRepLinks));
	}

	return true;
}
#endif

/* Write and fsync received XLOG extracted from raft log to disk */
XLogRecPtr
RaftRepWriteRecords(char *buf, Size nbytes)
{
	char *cursor;
	XLogRecPtr startPtr, endPtr;

	Assert(nbytes > 2 * sizeof(uint64));
	nbytes -= (2 * sizeof(uint64));

	cursor = buf;
	decodeRecPtr(&cursor, &startPtr);
	decodeRecPtr(&cursor, &endPtr);

	if (endPtr - startPtr != nbytes)
		elog(PANIC, "unexpected XLOG records extracted from raft log, \
			startPtr %X/%X, endPtr %X/%X, length %ld",
			(uint32) (startPtr >> 32), (uint32) startPtr,
			(uint32) (endPtr >> 32), (uint32) endPtr,
			nbytes);

	/* Update SentRecEnd since we may be elected a leader later */
	/* TODO: we need to re-compute this in StartupXLOG? */
	SentRecEnd = endPtr;

	RaftRepXLogWrite(cursor, startPtr, nbytes);

	return endPtr;
}

/* Write and fsync XLOG data to disk */
static void
RaftRepXLogWrite(char *buf, XLogRecPtr recptr, Size nbytes)
{
	int startOff, bytesWritten;

	/* Ignore the log if the local one is more up-to-date */
	if (recptr < LogRcvResult.Write)
	{
		/* Two raft committed logs cannot contain overlapped xlog records */
		Assert(recptr + nbytes <= LogRcvResult.Write);
		return;
	}

	/* Sanity check of the received log records, no hole is allowed in the
	 * log sequence */
	if (recptr != LogRcvResult.Write)
		elog(PANIC, "xlog records %X/%X received, expecting %X/%X",
			(uint32) (recptr >> 32), (uint32) recptr,
			(uint32) (LogRcvResult.Write >> 32), (uint32) LogRcvResult.Write);

	while (nbytes > 0)
	{
		int segBytes;

		/* Close the current segment if it's completed */
		if (recvFile >= 0 && !XLByteInSeg(recptr, recvSegNo, wal_segment_size))
			RaftRepXLogClose(recptr);

		if (recvFile < 0)
		{
			bool reuse = true;

			/* Create / reuse log file */
			XLByteToSeg(recptr, recvSegNo, wal_segment_size);
			recvFile = XLogFileInit(recvSegNo, &reuse, true);
			recvOff = 0;
		}

		/* Calculate the start offset of the received logs */
		startOff = XLogSegmentOffset(recptr, wal_segment_size);

		if (startOff + nbytes > wal_segment_size)
			segBytes = wal_segment_size - startOff;
		else
			segBytes = nbytes;

		/* Need to seek in the file? */
		if (recvOff != startOff)
		{
			if (lseek(recvFile, (off_t) startOff, SEEK_SET) < 0)
				ereport(PANIC,
						(errcode_for_file_access(),
						 errmsg("could not seek in log segment %s to offset %u: %m",
								XLogFileNameP(ThisTimeLineID, recvSegNo),
								startOff)));
			recvOff = startOff;
		}

		/* OK to write the logs */
		errno = 0;

		bytesWritten = write(recvFile, buf, segBytes);
		if (bytesWritten <= 0)
		{
			/* if write didn't set errno, assume no disk space */
			if (errno == 0)
				errno = ENOSPC;
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write to log segment %s "
							"at offset %u, length %lu: %m",
							XLogFileNameP(ThisTimeLineID, recvSegNo),
							recvOff, (unsigned long) segBytes)));
		}

		/* Proceed the state for write */
		recptr += bytesWritten;
		recvOff += bytesWritten;
		nbytes -= bytesWritten;
		buf += bytesWritten;

		LogRcvResult.Write = recptr;
	}

	/*
	 * Close the current segment if it's fully written up in the last
	 * cycle of the loop.
	 */
	if (recvFile >= 0 && !XLByteInSeg(recptr, recvSegNo, wal_segment_size))
		RaftRepXLogClose(recptr);

	RaftRepXLogFlush();
}

/* Flush the log to disk */
static void
RaftRepXLogFlush()
{
	if (LogRcvResult.Flush < LogRcvResult.Write)
	{
		volatile RaftRepCtlData *raftrepctl = RaftRepCtl;
		Assert(raftrepctl != NULL);

		issue_xlog_fsync(recvFile, recvSegNo);
		LogRcvResult.Flush = LogRcvResult.Write;

		/* Update shared-memory status */
		LWLockAcquire(RaftRepLock, LW_EXCLUSIVE);
		if (raftrepctl->rcvFlushRecEnd < LogRcvResult.Flush)
			raftrepctl->rcvFlushRecEnd = LogRcvResult.Flush;
		LWLockRelease(RaftRepLock);

		/* Report XLOG receiving progress in PS display */
		if (update_process_title)
		{
			char activitymsg[50];
			snprintf(activitymsg, sizeof(activitymsg), "receive flushed %X/%X",
					 (uint32) (LogRcvResult.Flush >> 32),
					 (uint32) LogRcvResult.Flush);
			set_ps_display(activitymsg, false);
		}
	}
}

/*
 * Close the current segment.
 *
 * Flush the segment to disk before closing it.
 */
static void
RaftRepXLogClose(XLogRecPtr recptr)
{
	char xlogfname[MAXFNAMELEN];

	Assert(recvFile >= 0 && !XLByteInSeg(recptr, recvSegNo, wal_segment_size));

	RaftRepXLogFlush();

	XLogFileName(xlogfname, ThisTimeLineID, recvSegNo, wal_segment_size);

	if (close(recvFile) != 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close log segment %s: %m",
						xlogfname)));

	recvFile = -1;
}
