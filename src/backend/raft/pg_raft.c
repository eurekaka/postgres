#include "postgres.h"

#include <inttypes.h>
#include "raft/pg_raft.h"

/* Special ID for the bootstrap node. Equals to raft_digest("1", 0). */
#define BOOTSTRAP_ID 0x2dc171858c3155be

/* This global variable is only written once at startup and is only read
 * from there on. Users should not manipulate the value of this variable. */
bool pg_raft_tracing_enabled = false;

// TODO use elog instead of fprintf
#define tracef(...) do {                                                                   \
	if (pg_raft_tracing_enabled)                                                           \
	{                                                                                      \
		int64_t ns;                                                                        \
		static char _msg[1024];                                                            \
		struct timespec ts = {0};                                                          \
		snprintf(_msg, sizeof(_msg), __VA_ARGS__);                                         \
		/* Ignore errors */                                                                \
		clock_gettime(CLOCK_REALTIME, &ts);                                                \
		ns = ts.tv_sec * 1000000000 + ts.tv_nsec;                                          \
		fprintf(stderr, "pg_raft %" PRId64 " %s:%d %s\n", ns, __func__, __LINE__, _msg);   \
	}                                                                                      \
} while(0)                                                                                 \

#define PG_RAFT_TRACE "PG_RAFT_TRACE"

static void
pg_raft_tracing_maybe_enable(bool enable)
{
	if (getenv(PG_RAFT_TRACE) != NULL)
		pg_raft_tracing_enabled = enable;
}

#define PTR_TO_UINT64(p) ((uint64_t)((uintptr_t)(p)))
#define UINT64_TO_PTR(u, ptr_type) ((ptr_type)((uintptr_t)(u)))

/**
 * Initialize the config object with required values and set the rest to sane
 * defaults. A copy will be made of the given @address.
 */
static int pg_raft_config_init(struct pg_raft_config *c,
								pg_raft_node_id id, const char *address);

/**
 * Release any memory held by the config object.
 */
static void pg_raft_config_close(struct pg_raft_config *c);

static int pg_raft_fsm_init(struct raft_fsm *fsm);
static void pg_raft_fsm_close(struct raft_fsm *f);

int
pg_raft_node_init(struct pg_raft_node *n,
		pg_raft_node_id id,
		const char *address,
		const char *dir)
{
	int rv;
	memset(n->errmsg, 0, sizeof n->errmsg);

	rv = pg_raft_config_init(&n->config, id, address);
	if (rv != 0)
		goto err;

	rv = uv_loop_init(&n->loop);
	if (rv != 0)
		goto err_after_config_init;

	/* Initialize the TCP-based RPC transport. */
	rv = raft_uv_tcp_init(&n->raft_transport, &n->loop);
	if (rv != 0)
		goto err_after_loop_init;

	/* Initialize the libuv-based I/O backend. */
	rv = raft_uv_init(&n->raft_io, &n->loop, dir, &n->raft_transport);
	if (rv != 0)
		goto err_after_raft_transport_init;

	/* Initialize the finite state machine. */
	rv = pg_raft_fsm_init(&n->raft_fsm);
	if (rv != 0)
		goto err_after_raft_io_init;

	/* Initialize and start the engine, using the libuv-based I/O backend. */
	/* TODO: add clean up for raft_init if this function fails later? */
	rv = raft_init(&n->raft, &n->raft_io, &n->raft_fsm, n->config.id, n->config.address);
	if (rv != 0)
	{
		snprintf(n->errmsg, RAFT_ERRMSG_BUF_SIZE, "raft_init(): %s", raft_errmsg(&n->raft));
		goto err_after_raft_fsm_init;
	}
	n->raft.data = n;

	/* Initialize raft internal parameters. */
	raft_set_election_timeout(&n->raft, 3000);
	raft_set_heartbeat_timeout(&n->raft, 500);
	// TODO: tune these params when replicating xlog.
	raft_set_snapshot_threshold(&n->raft, 64);
	raft_set_snapshot_trailing(&n->raft, 128);
	raft_set_max_catch_up_rounds(&n->raft, 100);
	raft_set_max_catch_up_round_duration(&n->raft, 50 * 1000); /* 50 secs */
	raft_set_pre_vote(&n->raft, true);

	/* Initialize semephore used to sync between raft thread and main thread. */
	// TODO pg_sem?
	rv = sem_init(&n->ready, 0, 0);
	if (rv != 0)
		goto err_after_raft_fsm_init;

	n->raft_state = RAFT_UNAVAILABLE;
	n->running = false;
	return 0;

err_after_raft_fsm_init:
	pg_raft_fsm_close(&n->raft_fsm);
err_after_raft_io_init:
	raft_uv_close(&n->raft_io);
err_after_raft_transport_init:
	raft_uv_tcp_close(&n->raft_transport);
err_after_loop_init:
	uv_loop_close(&n->loop);
err_after_config_init:
	pg_raft_config_close(&n->config);
err:
	return rv;
}

void
pg_raft_node_close(struct pg_raft_node *n)
{
	int rv;

	rv = sem_destroy(&n->ready);
	Assert(rv == 0); /* Fails only if sem object is invalid */
	pg_raft_fsm_close(&n->raft_fsm);
	/* raft_io has already been closed in pg_raft_close_cb */
	raft_uv_tcp_close(&n->raft_transport);
	uv_loop_close(&n->loop);
	pg_raft_config_close(&n->config);
}

int
pg_raft_node_create(pg_raft_node_id id,
		const char *address,
		const char *data_dir,
		pg_raft_node **n)
{
	int rv;

	// TODO: call raft_heap_set to use palloc.
	*n = malloc(sizeof **n);
	if (*n == NULL)
		return 1;

	rv = pg_raft_node_init(*n, id, address, data_dir);
	if (rv != 0)
	{
		// TODO
		free(*n);
		*n = NULL;
		return rv;
	}

	return 0;
}

void
pg_raft_node_destroy(pg_raft_node *n)
{
	pg_raft_node_close(n);
	// TODO
	free(n);
}

int
pg_raft_node_set_network_latency_ms(pg_raft_node *n, unsigned milliseconds)
{
	if (n->running)
		return 1;

	/* Currently we accept at least 1 millisecond latency and maximum 3600 s
	 * of latency */
	if (milliseconds == 0 || milliseconds > 3600U * 1000U)
		return 1;
	raft_set_heartbeat_timeout(&n->raft, (milliseconds * 15) / 10);
	raft_set_election_timeout(&n->raft, milliseconds * 15);
	return 0;
}

int
pg_raft_node_set_snapshot_params(pg_raft_node *n,
			unsigned snapshot_threshold,
			unsigned snapshot_trailing)
{
	if (n->running)
		return 1;

	if (snapshot_trailing < 1024)
		return 1;

	/* This is a safety precaution and allows to recover data from the second
	 * last raft snapshot and segment files in case the last raft snapshot is
	 * unusable. */
	if (snapshot_trailing < snapshot_threshold)
		return 1;

	raft_set_snapshot_threshold(&n->raft, snapshot_threshold);
	raft_set_snapshot_trailing(&n->raft, snapshot_trailing);
	return 0;
}

static int
pg_raft_bootstrap(pg_raft_node *n)
{
	int rv;
	struct raft_configuration configuration;

	/* Bootstrap the initial configuration. */
	raft_configuration_init(&configuration);
	for (int i = 0; i < FIXED_RAFT_CLUSTER_SIZE; i++)
	{
		rv = raft_configuration_add(&configuration, n->cluster_conf.ids[i],
									n->cluster_conf.addrs[i], RAFT_VOTER);
		if (rv != 0)
			goto out;
	}

	rv = raft_bootstrap(&n->raft, &configuration);
	if (rv != 0)
	{
		if (rv == RAFT_CANTBOOTSTRAP)
			rv = 0;
		else
			snprintf(n->errmsg, RAFT_ERRMSG_BUF_SIZE, "raft_bootstrap(): %s",
				 raft_errmsg(&n->raft));
	}

out:
	raft_configuration_close(&configuration);
	return rv;
}

/* Callback invoked when the stop async handle gets fired.
 *
 * This callback will walk through all active handles and close them. After the
 * last handle (which must be the 'stop' async handle) is closed, the loop gets
 * stopped.
 */
static void
pg_raft_close_cb(struct raft *raft)
{
	struct pg_raft_node *n = raft->data;
	raft_uv_close(&n->raft_io);
	// TODO: null callback?
	uv_close((struct uv_handle_s *)&n->stop, NULL);
	uv_close((struct uv_handle_s *)&n->startup, NULL);
	uv_close((struct uv_handle_s *)&n->monitor, NULL);
}

static void
pg_raft_stop_cb(uv_async_t *stop)
{
	struct pg_raft_node *n = stop->data;
	/* We expect that we're being executed after pg_raft_node_stop, so the
	 * running flag is off. */
	Assert(!n->running);
	raft_close(&n->raft, pg_raft_close_cb);
}

/* Callback invoked as soon as the loop has started.
 *
 * It unblocks the s->ready semaphore.
 */
static void
pg_raft_startup_cb(uv_timer_t *startup)
{
	int rv;
	struct pg_raft_node *n = startup->data;
	n->running = true;
	rv = sem_post(&n->ready);
	Assert(rv == 0); /* No reason for which posting should fail */
}

static void
pg_raft_monitor_cb(uv_prepare_t *monitor)
{
	struct pg_raft_node *n = monitor->data;
	int state = raft_state(&n->raft);

	if (state == RAFT_UNAVAILABLE)
		return;

	n->raft_state = state;
}

static int
task_run(struct pg_raft_node *n)
{
	int rv;

	/* Initialize stop notification handles. */
	n->stop.data = n;
	rv = uv_async_init(&n->loop, &n->stop, pg_raft_stop_cb);
	Assert(rv == 0);

	/* Schedule pg_raft_startup_cb to be fired as soon as the loop starts. It will
	 * unblock task_ready in the main thread. */
	n->startup.data = n;
	rv = uv_timer_init(&n->loop, &n->startup);
	Assert(rv == 0);
	rv = uv_timer_start(&n->startup, pg_raft_startup_cb, 0, 0);
	Assert(rv == 0);

	/* Schedule raft state change monitor. */
	n->monitor.data = n;
	rv = uv_prepare_init(&n->loop, &n->monitor);
	Assert(rv == 0);
	rv = uv_prepare_start(&n->monitor, pg_raft_monitor_cb);
	Assert(rv == 0);

	/* Start the raft node indeed. */
	n->raft.data = n;
	rv = raft_start(&n->raft);
	if (rv != 0)
	{
		snprintf(n->errmsg, RAFT_ERRMSG_BUF_SIZE, "raft_start(): %s",
			 raft_errmsg(&n->raft));
		/* Unblock any task_ready. */
		sem_post(&n->ready);
		return rv;
	}

	/* Enter the uv loop. The pg_raft_startup_cb would unblock the task_ready. */
	rv = uv_run(&n->loop, UV_RUN_DEFAULT);
	Assert(rv == 0);

	/* Unblock any task_ready. Just in case uv_run is returned without
	 * invoking pg_raft_startup_cb. */
	rv = sem_post(&n->ready);
	Assert(rv == 0); /* no reason for which posting should fail */

	return 0;
}

const char *
pg_raft_node_errmsg(pg_raft_node *n)
{
	return n->errmsg;
}

static void *
task_start(void *arg)
{
	struct pg_raft_node *n = arg;
	int rv;
	rv = task_run(n);
	if (rv != 0)
	{
		uintptr_t result = (uintptr_t)rv;
		return (void *)result;
	}
	return NULL;
}

/* Wait until a postgres raft server is ready.
 *
 * Returns true if the server has been successfully started, false otherwise.
 *
 * This is a thread-safe API, but must be invoked before any call to
 * pg_raft_node_stop.
 */
static bool
task_ready(struct pg_raft_node *n)
{
	/* Wait for the ready semaphore */
	sem_wait(&n->ready);
	return n->running;
}

int
pg_raft_node_start(pg_raft_node *n, bool bootstrap)
{
	int rv;

	/* Enable tracing if PG_RAFT_TRACE env is specified. */
	pg_raft_tracing_maybe_enable(true);
	tracef("postgres raft node start");
	if (bootstrap)
	{
		rv = pg_raft_bootstrap(n);
		if (rv != 0)
		{
			tracef("raft bootstrap failed %d", rv);
			goto err;
		}
	}

	rv = pthread_create(&n->thread, 0, &task_start, n);
	if (rv != 0)
	{
		tracef("raft thread create failed %d", rv);
		goto err;
	}

	/* Wait for the raft thread to be ready. */
	if (!task_ready(n))
	{
		tracef("raft thread not ready");
		rv = 1;
		goto err;
	}

	return 0;

err:
	return rv;
}

int
pg_raft_node_stop(pg_raft_node *n)
{
	int rv;
	void *result;

	tracef("postgres raft node stop");

	/* Turn off the running flag. This needs to happen before we send
	 * the stop signal since the stop callback expects to see that the
	 * flag is off. */
	n->running = false;

	rv = uv_async_send(&n->stop);
	Assert(rv == 0);

	/* Wait for the raft thread to exit. */
	rv = pthread_join(n->thread, &result);
	Assert(rv == 0);

	return (int)((uintptr_t)result);
}

static bool
node_info_valid(struct pg_raft_node_info *info)
{
	if (info->size != PG_RAFT_NODE_INFO_SZ)
		return false;

	return true;
}

int
pg_raft_node_recover(pg_raft_node *n,
			struct pg_raft_node_info infos[],
			int n_info)
{
	int i, rv, raft_role;
	const char *address;
	struct raft_configuration configuration;
	tracef("postgres raft node recover");

	raft_configuration_init(&configuration);
	for (i = 0; i < n_info; i++)
	{
		struct pg_raft_node_info *info = &infos[i];
		if (!node_info_valid(info))
		{
			rv = 1;
			goto out;
		}
		raft_role = (int)info->pg_raft_role;
		address = UINT64_TO_PTR(info->address, const char *);
		rv = raft_configuration_add(&configuration, info->id,
						address, raft_role);
		if (rv != 0)
		{
			Assert(rv == RAFT_NOMEM);
			goto out;
		};
	}

	rv = raft_recover(&n->raft, &configuration);
	if (rv != 0)
		goto out;

out:
	raft_configuration_close(&configuration);
	return rv;
}

pg_raft_node_id
pg_raft_generate_node_id(const char *address)
{
	int rv;
	struct timespec ts;
	unsigned long long n;
	tracef("generate node id");

	rv = clock_gettime(CLOCK_REALTIME, &ts);
	Assert(rv == 0);

	n = (unsigned long long)(ts.tv_sec * 1000 * 1000 * 1000 + ts.tv_nsec);

	return raft_digest(address, n);
}

static int
pg_raft_config_init(struct pg_raft_config *c, pg_raft_node_id id, const char *address)
{
	c->id = id;
	c->address = raft_malloc((int)strlen(address) + 1);
	if (c->address == NULL)
		return 1;
	strcpy(c->address, address);
	return 0;
}

static void
pg_raft_config_close(struct pg_raft_config *c)
{
	raft_free(c->address);
}

/* TODO: remove this, and use raft_add instead. */
void
pg_raft_cluster_config_init(pg_raft_node *n,
							pg_raft_node_id *ids, char **addrs)
{
	for (int i = 0; i < FIXED_RAFT_CLUSTER_SIZE; i++)
	{
		n->cluster_conf.ids[i] = ids[i];
		n->cluster_conf.addrs[i] = addrs[i];
	}
}

struct pg_raft_fsm
{
	unsigned long long count;
};

static int
pg_raft_fsm_apply(struct raft_fsm *fsm, const struct raft_buffer *buf, void **result)
{
	struct pg_raft_fsm *f = fsm->data;
	if (buf->len != sizeof(uint64_t))
		return RAFT_MALFORMED;
	f->count = *(uint64_t *)buf->base;
	*result = &f->count;
	return 0;
}

static int
pg_raft_fsm_snapshot(struct raft_fsm *fsm, struct raft_buffer *bufs[], unsigned *n_bufs)
{
	struct pg_raft_fsm *f = fsm->data;
	*n_bufs = 1;
	*bufs = raft_malloc(sizeof **bufs);
	if (*bufs == NULL)
		return RAFT_NOMEM;
	(*bufs)[0].len = sizeof(uint64_t);
	(*bufs)[0].base = raft_malloc((*bufs)[0].len);
	if ((*bufs)[0].base == NULL)
		return RAFT_NOMEM;
	*(uint64_t *)(*bufs)[0].base = f->count;
	return 0;
}

static int
pg_raft_fsm_restore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
	struct pg_raft_fsm *f = fsm->data;
	if (buf->len != sizeof(uint64_t))
		return RAFT_MALFORMED;
	f->count = *(uint64_t *)buf->base;
	// TODO normally the snapshot and restore happens on different nodes,
	// we may need another pair of free / malloc?
	raft_free(buf->base);
	return 0;
}

static int
pg_raft_fsm_init(struct raft_fsm *fsm)
{
	struct pg_raft_fsm *f = raft_malloc(sizeof *f);
	if (f == NULL)
		return RAFT_NOMEM;
	f->count = 0;
	fsm->version = 1;
	fsm->data = f;
	fsm->apply = pg_raft_fsm_apply;
	fsm->snapshot = pg_raft_fsm_snapshot;
	fsm->restore = pg_raft_fsm_restore;
	return 0;
}

static void
pg_raft_fsm_close(struct raft_fsm *f)
{
	if (f->data != NULL)
		raft_free(f->data);
}
