#ifndef PG_RAFT_H
#define PG_RAFT_H

#include <raft.h>
#include <raft/uv.h>

/**
 * 64-bit long ID of the postgres raft node.
 */
typedef unsigned long long pg_raft_node_id;

struct pg_raft_config
{
	pg_raft_node_id id;            /* Unique instance ID */
	char *address;                 /* Instance address */
};

/**
 * A logical postgres raft node instance.
 */
struct pg_raft_node
{
	pthread_t thread;                        /* Main run loop thread. */
	struct pg_raft_config config;            /* Config values */
	struct uv_loop_s loop;                   /* UV loop */
	struct raft_uv_transport raft_transport; /* Raft libuv transport */
	struct raft_io raft_io;                  /* Raft libuv I/O */
	struct raft_fsm raft_fsm;                /* Postgres raft FSM */
	sem_t ready;                             /* Node is ready */
	sem_t stopped;                           /* Notifiy loop stopped */
	bool running;                            /* Loop is running */
	struct raft raft;                        /* Raft instance */
	struct uv_async_s stop;                  /* Trigger UV loop stop */
	struct uv_timer_s startup;               /* Unblock ready sem */
	struct uv_prepare_s monitor;             /* Raft state change monitor */
	int raft_state;                          /* Previous raft state */
	char errmsg[RAFT_ERRMSG_BUF_SIZE];       /* Last error occurred */
};

typedef struct pg_raft_node pg_raft_node;

/**
 * Create a new postgres raft node object.
 *
 * The @id argument is a positive number that identifies this particular postgres
 * node in the cluster. Each postgres node of the same cluster must be
 * created with a different ID. The very first node, used to bootstrap a new
 * cluster, must have ID #1. Every time a node is started again, it must be
 * passed the same ID.
 *
 * The @address argument is the network address that other nodes in
 * the cluster must use to connect to this dqlite node. The format of the string
 * must be "<HOST>:<PORT>".
 *
 * The @data_dir argument is the file system path where the node should store its
 * durable data, i.e, the raft log entries.
 *
 * Memory pointed to by @address and @data_dir is copied, so any memory associated
 * with them can be released after the function returns.
 */
int pg_raft_node_create(pg_raft_node_id id,
			const char *address,
			const char *data_dir,
			pg_raft_node **n);

/**
 * Destroy a postgres raft node object.
 *
 * This will release all memory that was allocated by the node. If
 * pg_raft_node_start() was successfully invoked, then pg_raft_node_stop() must be
 * invoked before destroying the node.
 */
void pg_raft_node_destroy(pg_raft_node *n);

int pg_raft_node_init(pg_raft_node *d,
		 pg_raft_node_id id,
		 const char *address,
		 const char *dir);

void pg_raft_node_close(pg_raft_node *d);

/**
 * Set the average one-way network latency, expressed in milliseconds.
 *
 * This value is used internally by raft to decide how frequently the leader
 * node should send heartbeats to other nodes in order to maintain its
 * leadership, and how long other nodes should wait before deciding that the
 * leader has died and initiate a failover.
 *
 * This function must be called before calling pg_raft_node_start().
 *
 * Latency should not be 0 or larger than 3600000 milliseconds.
 */
int pg_raft_node_set_network_latency_ms(pg_raft_node *n, unsigned milliseconds);

/**
 * Set the snapshot parameters for this node.
 *
 * This function determines how frequently a node will snapshot the state
 * of the fsm and how many raft log entries will be kept around after
 * a snapshot has been taken.
 *
 * `snapshot_threshold`: Determines the frequency of taking a snapshot, the
 * lower the number, the higher the frequency.
 *
 * `snapshot_trailing`: Determines the amount of log entries kept around after
 * taking a snapshot. Lowering this number decreases disk and memory footprint
 * but increases the chance of having to send a full snapshot (instead of a
 * number of log entries) to a node that has fallen behind.
 *
 * This function must be called before calling pg_raft_node_start().
 */
int pg_raft_node_set_snapshot_params(pg_raft_node *n,
			unsigned snapshot_threshold,
			unsigned snapshot_trailing);

/**
 * Start a logical postgres raft node.
 *
 * A background thread will be spawned which will run the node's main loop. If
 * this function returns successfully, the postgres raft node is ready to accept new
 * connections from other raft nodes.
 */
int pg_raft_node_start(pg_raft_node *n);

/**
 * Stop a logical postgres raft node.
 *
 * The background thread running the main loop will be notified and the node
 * will not accept any new connections from other raft nodes. Once running RPCs are
 * completed, those connections get closed and then the thread exits.
 */
int pg_raft_node_stop(pg_raft_node *n);

struct pg_raft_node_info
{
	uint64_t size; /* The size of this struct */
	uint64_t id; /* pg_raft_node_id */
	uint64_t address;
	uint64_t pg_raft_role;
};
typedef struct pg_raft_node_info pg_raft_node_info;
#define PG_RAFT_NODE_INFO_SZ 32U /* (4 * 64) / 8 */

/**
 * Force recovering a postgres raft node which is part of a cluster whose majority of
 * nodes have died, and therefore has become unavailable.
 *
 * In order for this operation to be safe you must follow these steps:
 *
 * 1. Make sure no postgres raft node in the cluster is running.
 *
 * 2. Identify all postgres raft nodes that have survived and that you want to be part
 *    of the recovered cluster.
 *
 * 3. Among the survived postgres raft nodes, find the one with the most up-to-date
 *    raft term and log.
 *
 * 4. Invoke @pg_raft_node_recover exactly one time, on the node you found in
 *    step 3, and pass it an array of #pg_raft_node_info filled with the IDs,
 *    addresses and roles of the survived nodes, including the one being recovered.
 *
 * 5. Copy the data directory of the node you ran @pg_raft_node_recover on to all
 *    other non-dead nodes in the cluster, replacing their current data
 *    directory.
 *
 * 6. Restart all nodes.
 */
int pg_raft_node_recover(pg_raft_node *n, pg_raft_node_info infos[], int n_info);

/**
 * Return a human-readable description of the last error occurred.
 */
const char *pg_raft_node_errmsg(pg_raft_node *n);

/**
 * Generate a unique ID for the given address.
 */
pg_raft_node_id pg_raft_generate_node_id(const char *address);

#endif /* PG_RAFT_H */
