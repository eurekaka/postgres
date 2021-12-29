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
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"

/*
 * TODO: GUC parameters.  Logging_collector cannot be changed after postmaster
 * start, but the rest can change at SIGHUP.
 */
char *Raft_log_directory = "raft_log";

static volatile sig_atomic_t check_requested = false;
static volatile sig_atomic_t shutdown_requested = false;

static void raftserver_sigusr1_handler(SIGNAL_ARGS);
static void raftserver_shutdown_handler(SIGNAL_ARGS);
static void raftserver_check_handler(SIGNAL_ARGS);
static void raftserver_quickdie(SIGNAL_ARGS);

static int shutdown_raft_server(void);
static int start_raft_server(void);
static int maybe_send_raft_log(void);

/*
 * Main entry point for raftserver process
 */
void
RaftServerMain()
{
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
	pqsignal(SIGINT, raftserver_check_handler);
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

	/* Create raft log directory if not present; ignore errors */
	(void) MakePGDirectory(Raft_log_directory);

	RaftRepInitConfig();
	(void) start_raft_server();

	/* Notify postmaster we are ready */
	SendPostmasterSignal(PMSIGNAL_RAFTSERVER_READY);

	/* Unblock signals (they were blocked when the postmaster forked us) */
	PG_SETMASK(&UnBlockSig);

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
			/* Finish sync of the remaining raft logs */
			(void) maybe_send_raft_log();
			(void) shutdown_raft_server();
			/* Normal exit from the raftserver is here */
			proc_exit(0);
		}

		if (check_requested)
		{
			check_requested = false;
			(void) maybe_send_raft_log();
		}

		/* Sleep until there's something to do */
		(void) WaitLatch(MyLatch,
					WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
					-1, WAIT_EVENT_RAFT_SERVER_MAIN);
	}
}

static int
shutdown_raft_server(void)
{
	return 0;
}

static int
start_raft_server(void)
{
	return 0;
}

static int
maybe_send_raft_log(void)
{
	return 0;
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

/* SIGINT: check if new raft logs need sync. */
static void
raftserver_check_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	check_requested = true;
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
