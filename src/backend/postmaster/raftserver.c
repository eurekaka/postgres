/*-------------------------------------------------------------------------
 *
 * raftserver.c
 *
 * The raft server acts similarly as the combination of wal sender and wal
 * receiver.
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/raftserver.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/raftserver.h"
#include "replication/raftrep.h"
#include "raft/pg_raft.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"

/* GUC parameter, cannot be changed after postmaster start */
int raft_server_id;

static char *raft_log_directory = "raft_log";
static pg_raft_node *raft_node = NULL;

static pg_raft_node_id raft_cluster_ids[] = {0, 1, 2};
static char *raft_cluster_addrs[] = {\
"127.0.0.1:5000",\
"127.0.0.1:5001",\
"127.0.0.1:5002"\
};

static volatile sig_atomic_t shutdown_requested = false;
static StringInfo sendBuf = NULL;

static void raftserver_sigusr1_handler(SIGNAL_ARGS);
static void raftserver_shutdown_handler(SIGNAL_ARGS);
static void raftserver_quickdie(SIGNAL_ARGS);

static void shutdown_raft_server(void);
static void start_raft_server(void);
static void maybe_apply_raft_log(void);

/*
 * Main entry point for raftserver process
 */
void
RaftServerMain()
{
	MemoryContext raftserver_context;

	/*
	 * Properly accept or ignore signals the postmaster might send us
	 *
	 * Note: like checkpointer subprocess, we deliberately ignore SIGTERM,
	 * because during a standard Unix system shutdown cycle, init will
	 * SIGTERM all processes at once.  We want to wait for the backends to
	 * exit, whereupon the postmaster will tell us it's okay to shut
	 * down (via SIGUSR2).
	 */
	pqsignal(SIGHUP, SIG_IGN);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, SIG_IGN); /* ignore SIGTERM */
	pqsignal(SIGQUIT, raftserver_quickdie); /* hard crash time */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, raftserver_sigusr1_handler);
	pqsignal(SIGUSR2, raftserver_shutdown_handler);
	/* Reset some signals that are accepted by postmaster but not here */
	pqsignal(SIGCHLD, SIG_DFL);

	/* Unblock SIGQUIT as soon as possible */
	sigdelset(&BlockSig, SIGQUIT);

	if (raft_server_id == INT_MAX)
		proc_exit(0);

	/* Create raft log directory if not present; ignore errors */
	(void) MakePGDirectory(raft_log_directory);

	raftserver_context = AllocSetContextCreate(TopMemoryContext,
												"RaftServer",
												ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(raftserver_context);

	RaftRepInit();
	start_raft_server();

	/* Notify postmaster we are ready */
	SendPostmasterSignal(PMSIGNAL_RAFTSERVER_READY);

	/* Unblock signals (they were blocked when the postmaster forked us) */
	PG_SETMASK(&UnBlockSig);

	sendBuf = makeStringInfo();

	/* main worker loop */
	for (;;)
	{
		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		/*
		 * Process any signals received recently.
		 */
		if (shutdown_requested)
		{
			/* Finish replication of the remained xlog records */
			maybe_apply_raft_log();
			shutdown_raft_server();
			/* Normal exit from the raftserver is here */
			proc_exit(0);
		}

		maybe_apply_raft_log();

		/* Sleep until there's something to do */
		(void) WaitLatch(MyLatch,
					WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
					WAIT_EVENT_RAFT_SERVER_MAIN);
	}
}

static void
start_raft_server(void)
{
	int rc = 0;

	if (raft_server_id > 2)
		ereport(PANIC,
				(errmsg("invalid raft_server_id value %d", raft_server_id)));

	rc = pg_raft_node_create((pg_raft_node_id) raft_cluster_ids[raft_server_id],
							raft_cluster_addrs[raft_server_id],
							raft_log_directory, &raft_node);
	if (rc != 0)
		ereport(PANIC,
				(errmsg("pg_raft_node_create failed with return code %d", rc)));

	/* Pass down the hard-coded raft cluster config for bootstrap */
	pg_raft_cluster_config_init(raft_node,
								raft_cluster_ids,
								raft_cluster_addrs);

	rc = pg_raft_node_start(raft_node, true);
	if (rc != 0)
		ereport(PANIC,
				(errmsg("pg_raft_node_start failed with return code %d, \
				error msg %s", rc, pg_raft_node_errmsg(raft_node))));
}

static void
shutdown_raft_server(void)
{
	int rc = 0;

	rc = pg_raft_node_stop(raft_node);
	if (rc != 0)
		ereport(PANIC,
				(errmsg("pg_raft_node_stop failed with return code %d, \
				error msg %s", rc, pg_raft_node_errmsg(raft_node))));

	pg_raft_node_destroy(raft_node);
}

static void
maybe_apply_raft_log(void)
{
	int rv = 0;
	XLogRecPtr flushRecEnd;

	if (!pg_raft_node_is_leader(raft_node))
		return;

	flushRecEnd = GetFlushRecPtr();
	while (RaftRepGetRecordsForSend(flushRecEnd, sendBuf))
	{
		rv = pg_raft_node_apply_log(raft_node,
						(void *) sendBuf->data, (size_t) sendBuf->len);
		if (rv != 0)
			ereport(PANIC,
					(errmsg("raftserver failed in sending xlog(%d), \
					error msg %s", rv, pg_raft_node_errmsg(raft_node))));
		else
			ereport(LOG,
					(errmsg("raftserver succeeded in sending xlog")));
	}

	return;
}

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */

/* SIGUSR1: used for latch wakeups. */
static void
raftserver_sigusr1_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	latch_sigusr1_handler();

	errno = save_errno;
}

/* SIGUSR2: shut down the raft server. */
static void
raftserver_shutdown_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	shutdown_requested = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGQUIT: immediate shutdown. */
static void
raftserver_quickdie(SIGNAL_ARGS)
{
	/*
	 * We DO NOT want to run proc_exit() or atexit() callbacks -- we're here
	 * because shared memory may be corrupted, so we don't want to try to
	 * clean up our transaction.  Just nail the windows shut and get out of
	 * town.  The callbacks wouldn't be safe to run from a signal handler,
	 * anyway.
	 *
	 * Note we do _exit(2) not _exit(0).  This is to force the postmaster into
	 * a system reset cycle if someone sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
	 * should ensure the postmaster sees this as a crash, too, but no harm in
	 * being doubly sure.)
	 */
	_exit(2);
}