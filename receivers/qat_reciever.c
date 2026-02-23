#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <qat/cpa.h>
#include <qat/cpa_dc.h>
#include <qat/icp_sal_poll.h>
#include <qat/icp_sal_user.h>
#include <qat/qae_mem.h>
#include <sched.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define MAGIC_COMPRESSED 0x51415443
#define MAGIC_UNCOMPRESSED 0x51415452
#define MAX_BUFFER_SIZE 131072
#define TEMP_BUFFER_SIZE (MAX_BUFFER_SIZE * 2)
#define RING_SIZE 32768
#define CACHE_LINE_SIZE 64

static volatile bool g_shutdown_requested = false;

typedef enum {
  JOB_EMPTY = 0,
  JOB_READY = 1,     // Received from network, ready to submit to QAT
  JOB_SUBMITTED = 2, // Submitted to QAT, waiting for callback
  JOB_COMPLETED = 3, // QAT callback completed
  JOB_ERROR = 4,
  JOB_SUBMITTING = 5 // Transient: being submitted to QAT
} job_status_t;

typedef struct {
  uint32_t magic;
  uint32_t uncompressed_size;
  uint32_t compressed_size;
  uint64_t sequence_number;
  uint8_t algorithm;
  uint8_t compression_level;
  uint16_t checksum;
} __attribute__((packed)) compression_header_t;

typedef struct ring_buffer_s ring_buffer_t;

typedef struct {
  volatile job_status_t status;
  uint64_t sequence_number;

  CpaBufferList *pSrcBuffer;
  CpaBufferList *pDstBuffer;
  CpaFlatBuffer *pFlatSrcBuffer;
  CpaFlatBuffer *pFlatDstBuffer;

  Cpa8U *pSrcData;
  Cpa8U *pDstData;

  Cpa32U srcDataSize;
  Cpa32U dstDataSize;
  Cpa32U producedSize;

  CpaDcRqResults dcResults;
  compression_header_t header;

  uint64_t submit_time;
  uint64_t complete_time;
  ring_buffer_t *parent_ring;
} ring_entry_t;

struct ring_buffer_s {
  ring_entry_t entries[RING_SIZE];

  struct {
    volatile uint32_t tail;
    uint64_t packets_received;
    uint64_t bytes_received;
    uint64_t jobs_submitted;
    char pad[CACHE_LINE_SIZE - 48];
  } __attribute__((aligned(CACHE_LINE_SIZE))) producer;

  struct {
    volatile uint32_t head;
    volatile uint32_t
        submit_head; // NEW: atomic pointer for workers to claim READY jobs
    uint64_t jobs_completed;
    uint64_t jobs_processed;
    uint32_t retirement_lock; // NEW: simple lock for ordered head advancement
    char pad[CACHE_LINE_SIZE - 32];
  } __attribute__((aligned(CACHE_LINE_SIZE))) consumer;

  CpaInstanceHandle dcInstance;
  CpaDcSessionHandle sessionHandle;
  void *sessionMemory;

  uint32_t mask;
  volatile bool running;

  volatile uint64_t decompressed_packets;
  volatile uint64_t decompressed_bytes;
  volatile uint64_t uncompressed_bytes;
  volatile uint64_t decompress_errors;
  volatile uint64_t ring_full_count;
  volatile uint64_t compressed_packets_recv;
  volatile uint64_t uncompressed_packets_recv;
  struct timeval start_time;
  struct {
    uint64_t total_bytes_in;
    uint64_t total_bytes_out;
  } qat_stats;
};

#define NUM_QAT_INSTANCES 1
static ring_buffer_t *g_rings[NUM_QAT_INSTANCES] = {0};
static bool qat_instances_used[16] = {0};

void signal_handler(int signum) {
  printf("\nReceived signal %d, initiating graceful shutdown...\n", signum);
  g_shutdown_requested = true;
  for (int i = 0; i < NUM_QAT_INSTANCES; i++) {
    if (g_rings[i]) {
      g_rings[i]->running = false;
    }
  }
}

void decompress_callback(void *pCallbackTag, CpaStatus status) {
  ring_entry_t *entry = (ring_entry_t *)pCallbackTag;
  ring_buffer_t *ring = entry->parent_ring;

  if (status == CPA_STATUS_SUCCESS) {
    entry->producedSize = entry->dcResults.produced;
    __sync_synchronize();
    entry->status = JOB_COMPLETED;
    __sync_fetch_and_add(&ring->qat_stats.total_bytes_in, entry->srcDataSize);
    __sync_fetch_and_add(&ring->qat_stats.total_bytes_out, entry->producedSize);
  } else {
    __sync_synchronize();
    entry->status = JOB_ERROR;
  }
}

int allocate_qat_buffers(ring_entry_t *entry, CpaInstanceHandle dcInstance) {
  Cpa32U metaSize = 0;
  CpaStatus status;

  status = cpaDcBufferListGetMetaSize(dcInstance, 1, &metaSize);
  if (status != CPA_STATUS_SUCCESS) {
    printf("Failed to get metadata size\n");
    return -1;
  }

  entry->pSrcBuffer = qaeMemAllocNUMA(sizeof(CpaBufferList), 0, 64);
  if (!entry->pSrcBuffer)
    return -1;

  if (metaSize > 0) {
    entry->pSrcBuffer->pPrivateMetaData = qaeMemAllocNUMA(metaSize, 0, 64);
    if (!entry->pSrcBuffer->pPrivateMetaData)
      return -1;
  } else {
    entry->pSrcBuffer->pPrivateMetaData = NULL;
  }

  entry->pFlatSrcBuffer = qaeMemAllocNUMA(sizeof(CpaFlatBuffer), 0, 64);
  if (!entry->pFlatSrcBuffer)
    return -1;

  // Aligned with sender max output (256KB)
  entry->pSrcData = qaeMemAllocNUMA(MAX_BUFFER_SIZE * 2, 0, 64);
  if (!entry->pSrcData)
    return -1;

  entry->pSrcBuffer->pBuffers = entry->pFlatSrcBuffer;
  entry->pSrcBuffer->numBuffers = 1;

  entry->pFlatSrcBuffer->pData = entry->pSrcData;
  entry->pFlatSrcBuffer->dataLenInBytes = 0;

  entry->pDstBuffer = qaeMemAllocNUMA(sizeof(CpaBufferList), 0, 64);
  if (!entry->pDstBuffer)
    return -1;

  entry->pFlatDstBuffer = qaeMemAllocNUMA(sizeof(CpaFlatBuffer), 0, 64);
  if (!entry->pFlatDstBuffer)
    return -1;

  if (metaSize > 0) {
    entry->pDstBuffer->pPrivateMetaData = qaeMemAllocNUMA(metaSize, 0, 64);
    if (!entry->pDstBuffer->pPrivateMetaData)
      return -1;
  } else {
    entry->pDstBuffer->pPrivateMetaData = NULL;
  }

  entry->pDstData = qaeMemAllocNUMA(MAX_BUFFER_SIZE * 2, 0, 64);
  if (!entry->pDstData)
    return -1;

  entry->pDstBuffer->pBuffers = entry->pFlatDstBuffer;
  entry->pDstBuffer->numBuffers = 1;

  entry->pFlatDstBuffer->pData = entry->pDstData;
  entry->pFlatDstBuffer->dataLenInBytes = MAX_BUFFER_SIZE * 2;

  return 0;
}

int qat_init(ring_buffer_t *ring, int instance_target_index) {
  CpaStatus status;
  Cpa16U numInstances = 0;
  CpaInstanceHandle *instances = NULL;
  CpaDcSessionSetupData sessionSetupData = {0};
  Cpa32U sessionSize = 0;
  Cpa32U contextSize = 0;

  status = icp_sal_userStartMultiProcess("SSL", CPA_FALSE);
  if (status != CPA_STATUS_SUCCESS) {
    printf("Failed to start QAT: %d\n", status);
    return -1;
  }

  status = cpaDcGetNumInstances(&numInstances);
  if (status != CPA_STATUS_SUCCESS || numInstances == 0) {
    printf("No QAT instances\n");
    return -1;
  }

  instances = malloc(numInstances * sizeof(CpaInstanceHandle));
  status = cpaDcGetInstances(numInstances, instances);

  int target_numa_nodes[] = {2, 3};
  int target_numa_node = target_numa_nodes[instance_target_index % 2];
  int selected_instance = -1;

  for (int i = 0; i < numInstances; i++) {
    CpaInstanceInfo2 info;
    if (cpaDcInstanceGetInfo2(instances[i], &info) == CPA_STATUS_SUCCESS) {
      if (info.nodeAffinity == target_numa_node && !qat_instances_used[i]) {
        selected_instance = i;
        break;
      }
    }
  }

  if (selected_instance == -1) {
    for (int i = 0; i < numInstances; i++) {
      if (!qat_instances_used[i]) {
        selected_instance = i;
        break;
      }
    }
  }

  if (selected_instance == -1) {
    printf("Error: All QAT instances in use\n");
    free(instances);
    return -1;
  }

  qat_instances_used[selected_instance] = true;
  ring->dcInstance = instances[selected_instance];
  free(instances);

  status = cpaDcSetAddressTranslation(ring->dcInstance, qaeVirtToPhysNUMA);
  if (status != CPA_STATUS_SUCCESS)
    return -1;

  Cpa16U numBuffers = 4096;
  status = cpaDcStartInstance(ring->dcInstance, numBuffers, NULL);
  if (status != CPA_STATUS_SUCCESS)
    return -1;

  CpaDcCompLvl qat_level = CPA_DC_L6;

  sessionSetupData.compLevel = qat_level;
  sessionSetupData.compType = CPA_DC_DEFLATE;
  sessionSetupData.huffType = CPA_DC_HT_STATIC;
  sessionSetupData.sessDirection = CPA_DC_DIR_DECOMPRESS;
  sessionSetupData.sessState = CPA_DC_STATELESS;
  sessionSetupData.checksum = CPA_DC_CRC32;

  status = cpaDcGetSessionSize(ring->dcInstance, &sessionSetupData,
                               &sessionSize, &contextSize);
  if (status != CPA_STATUS_SUCCESS)
    return -1;

  ring->sessionMemory = qaeMemAllocNUMA(sessionSize, 0, 64);
  if (!ring->sessionMemory)
    return -1;

  ring->sessionHandle = ring->sessionMemory;

  status = cpaDcInitSession(ring->dcInstance, ring->sessionHandle,
                            &sessionSetupData, NULL, decompress_callback);
  if (status != CPA_STATUS_SUCCESS) {
    qaeMemFreeNUMA(&ring->sessionMemory);
    return -1;
  }

  printf("QAT initialized with async callback\n");
  return 0;
}

ring_buffer_t *ring_buffer_init(int instance_id) {
  ring_buffer_t *ring = aligned_alloc(CACHE_LINE_SIZE, sizeof(ring_buffer_t));
  if (!ring)
    return NULL;

  memset(ring, 0, sizeof(ring_buffer_t));

  if (qat_init(ring, instance_id) != 0) {
    free(ring);
    return NULL;
  }

  // Pre-allocate all buffers in the ring
  for (int i = 0; i < RING_SIZE; i++) {
    ring->entries[i].status = JOB_EMPTY;
    ring->entries[i].parent_ring = ring;
    if (allocate_qat_buffers(&ring->entries[i], ring->dcInstance) != 0) {
      printf("Failed to allocate buffers for entry %d\n", i);
      free(ring);
      return NULL;
    }
  }

  ring->mask = RING_SIZE - 1;
  ring->running = true;
  ring->consumer.head = 0;
  ring->consumer.submit_head = 0;
  ring->consumer.retirement_lock = 0;
  g_rings[instance_id] = ring;

  printf(
      "Ring buffer for instance %d initialized with %d pre-allocated entries\n",
      instance_id, RING_SIZE);
  return ring;
}

int recv_exact(int sockfd, void *buffer, size_t len) {
  size_t total = 0;
  while (total < len && !g_shutdown_requested) {
    ssize_t n = recv(sockfd, (char *)buffer + total, len - total, 0);
    if (n <= 0) {
      if (n == 0) {
        // Connection closed
      } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
        continue;
      } else if (errno == EINTR) {
        continue;
      } else {
        perror("recv_exact error");
      }
      return -1;
    }
    total += n;
  }
  return g_shutdown_requested ? -1 : 0;
}

typedef struct {
  int sockfd;
  ring_buffer_t *ring;
} net_args_t;

void *network_thread(void *arg) {
  net_args_t *args = (net_args_t *)arg;
  int sockfd = args->sockfd;
  ring_buffer_t *ring = args->ring;
  uint8_t *temp_buffer = malloc(TEMP_BUFFER_SIZE);
  compression_header_t header;

  printf("Network thread started handling socket %d on ring %p\n", sockfd,
         (void *)ring);

  while (ring->running) {
    if (recv_exact(sockfd, &header, sizeof(compression_header_t)) < 0) {
      break;
    }

    if (header.magic == MAGIC_COMPRESSED) {
      if (header.compressed_size > MAX_BUFFER_SIZE * 2) {
        printf("Error: Compressed size too large (%u bytes) on socket %d\n",
               header.compressed_size, sockfd);
        break;
      }

      uint32_t current_tail, next_tail;
      ring_entry_t *entry = NULL;

      // Lock-free tail claiming
      do {
        current_tail = __atomic_load_n(&ring->producer.tail, __ATOMIC_ACQUIRE);
        next_tail = (current_tail + 1) & ring->mask;

        if (next_tail ==
            __atomic_load_n(&ring->consumer.head, __ATOMIC_ACQUIRE)) {
          // Ring full: we must drain this packet or we'll hang
          if (recv_exact(sockfd, temp_buffer, header.compressed_size) < 0)
            break;
          __sync_fetch_and_add(&ring->ring_full_count, 1);
          goto skip_compressed;
        }

        entry = &ring->entries[current_tail];
        // Ensure slot is really empty (ABA or overlap protection)
        if (__atomic_load_n(&entry->status, __ATOMIC_ACQUIRE) != JOB_EMPTY) {
          __builtin_ia32_pause();
          continue;
        }

      } while (!__atomic_compare_exchange_n(&ring->producer.tail, &current_tail,
                                            next_tail, true, __ATOMIC_ACQ_REL,
                                            __ATOMIC_ACQUIRE));

      if (!ring->running)
        break;

      entry->header = header;
      if (recv_exact(sockfd, entry->pSrcData, header.compressed_size) < 0)
        break;

      entry->srcDataSize = header.compressed_size;
      entry->pFlatSrcBuffer->dataLenInBytes = header.compressed_size;
      entry->sequence_number = header.sequence_number;
      __atomic_store_n(&entry->status, JOB_READY, __ATOMIC_RELEASE);

    skip_compressed:
      __sync_fetch_and_add(&ring->compressed_packets_recv, 1);
      __sync_fetch_and_add(&ring->producer.packets_received, 1);
      __sync_fetch_and_add(&ring->producer.bytes_received,
                           sizeof(compression_header_t) +
                               header.compressed_size);

    } else if (header.magic == MAGIC_UNCOMPRESSED) {
      if (recv_exact(sockfd, temp_buffer, header.uncompressed_size) < 0)
        break;

      __sync_fetch_and_add(&ring->uncompressed_packets_recv, 1);
      __sync_fetch_and_add(&ring->producer.packets_received, 1);
      __sync_fetch_and_add(&ring->uncompressed_bytes, header.uncompressed_size);
      __sync_fetch_and_add(&ring->producer.bytes_received,
                           sizeof(compression_header_t) +
                               header.uncompressed_size);

    } else {
      printf("Error: Magic Mismatch (got 0x%08x) on socket %d after %lu "
             "compressed and %lu uncompressed packets. Last seq: %lu\n",
             header.magic, sockfd, ring->compressed_packets_recv,
             ring->uncompressed_packets_recv, header.sequence_number);

      // Peek at next bytes to see if we can find a magic
      uint32_t next_magic = 0;
      if (recv(sockfd, &next_magic, sizeof(uint32_t), MSG_PEEK | MSG_DONTWAIT) >
          0) {
        printf("Next 4 bytes on wire: 0x%08x\n", next_magic);
      }
      break;
    }
  }

  free(temp_buffer);
  printf("Network thread on socket %d exiting\n", sockfd);
  return NULL;
}

void *decompress_thread(void *arg) {
  uint64_t local_completed = 0;
  uint64_t local_submitted = 0;

  printf("Decompress thread started (polling %d rings)\n", NUM_QAT_INSTANCES);

  while (!g_shutdown_requested) {
    bool worked = false;

    for (int i = 0; i < NUM_QAT_INSTANCES; i++) {
      ring_buffer_t *ring = g_rings[i];
      if (!ring || !ring->running)
        continue;

      // 1. Polling
      icp_sal_DcPollInstance(ring->dcInstance, 0);

      // 2. Greedy Job Submission (Batch up to 512)
      int submission_batch = 0;
      while (submission_batch < 512) {
        uint32_t s_head = ring->consumer.submit_head;
        uint32_t tail = __atomic_load_n(&ring->producer.tail, __ATOMIC_ACQUIRE);

        if (s_head == tail)
          break;

        ring_entry_t *s_entry = &ring->entries[s_head];
        if (__atomic_load_n(&s_entry->status, __ATOMIC_ACQUIRE) != JOB_READY)
          break;

        __atomic_store_n(&s_entry->status, JOB_SUBMITTING, __ATOMIC_RELEASE);

        CpaDcOpData opData = {0};
        opData.flushFlag = CPA_DC_FLUSH_FINAL;
        opData.compressAndVerify = CPA_TRUE;

        CpaStatus status = cpaDcDecompressData2(
            ring->dcInstance, ring->sessionHandle, s_entry->pSrcBuffer,
            s_entry->pDstBuffer, &opData, &s_entry->dcResults, s_entry);

        if (status == CPA_STATUS_SUCCESS) {
          __atomic_store_n(&s_entry->status, JOB_SUBMITTED, __ATOMIC_RELEASE);
          __sync_fetch_and_add(&ring->producer.jobs_submitted, 1);
          ring->consumer.submit_head = (s_head + 1) & ring->mask;
          local_submitted++;
          submission_batch++;
          worked = true;
        } else if (status == CPA_STATUS_RETRY) {
          __atomic_store_n(&s_entry->status, JOB_READY, __ATOMIC_RELEASE);
          break;
        } else {
          __atomic_store_n(&s_entry->status, JOB_ERROR, __ATOMIC_RELEASE);
          ring->consumer.submit_head = (s_head + 1) & ring->mask;
          submission_batch++;
          worked = true;
        }
      }

      // 3. Greedy Ordered Retirement
      uint32_t h = ring->consumer.head;
      int retirement_batch = 0;
      while (retirement_batch < 1024) {
        ring_entry_t *h_entry = &ring->entries[h];
        job_status_t status =
            __atomic_load_n(&h_entry->status, __ATOMIC_ACQUIRE);

        if (status == JOB_COMPLETED) {
          __sync_fetch_and_add(&ring->decompressed_packets, 1);
          __sync_fetch_and_add(&ring->decompressed_bytes,
                               h_entry->producedSize);
          __atomic_store_n(&h_entry->status, JOB_EMPTY, __ATOMIC_RELEASE);
          h = (h + 1) & ring->mask;
          local_completed++;
          __sync_fetch_and_add(&ring->consumer.jobs_completed, 1);
          __sync_fetch_and_add(&ring->consumer.jobs_processed, 1);
          retirement_batch++;
          worked = true;
        } else if (status == JOB_ERROR) {
          __sync_fetch_and_add(&ring->decompress_errors, 1);
          __atomic_store_n(&h_entry->status, JOB_EMPTY, __ATOMIC_RELEASE);
          h = (h + 1) & ring->mask;
          __sync_fetch_and_add(&ring->consumer.jobs_processed, 1);
          retirement_batch++;
          worked = true;
        } else {
          break;
        }
      }
      ring->consumer.head = h;
    }

    if (!worked) {
      __builtin_ia32_pause();
    }
  }

  printf("Decompress thread exiting (submitted: %lu, completed: %lu)\n",
         local_submitted, local_completed);
  return NULL;
}

void *stats_thread(void *arg) {
  uint64_t last_packets = 0;
  uint64_t last_bytes = 0;
  uint64_t last_decompressed = 0;
  uint64_t last_uncompressed = 0;
  time_t last_time = time(NULL);

  while (!g_shutdown_requested) {
    sleep(5);

    time_t now = time(NULL);
    uint64_t current_packets = 0, current_bytes = 0;
    uint64_t current_decompressed = 0, current_uncompressed = 0;
    uint32_t total_depth = 0;
    uint64_t total_submitted = 0, total_completed = 0, total_errors = 0;

    for (int i = 0; i < NUM_QAT_INSTANCES; i++) {
      ring_buffer_t *ring = g_rings[i];
      if (!ring)
        continue;
      current_packets += ring->producer.packets_received;
      current_bytes += ring->producer.bytes_received;
      current_decompressed += ring->decompressed_bytes;
      current_uncompressed += ring->uncompressed_bytes;
      total_depth += (ring->producer.tail - ring->consumer.head) & ring->mask;
      total_submitted += ring->producer.jobs_submitted;
      total_completed += ring->consumer.jobs_completed;
      total_errors += ring->decompress_errors;
    }

    uint64_t pkt_delta = current_packets - last_packets;
    uint64_t byte_delta = current_bytes - last_bytes;
    uint64_t decomp_delta = current_decompressed - last_decompressed;
    uint64_t uncomp_delta = current_uncompressed - last_uncompressed;
    double elapsed = now - last_time;

    if (elapsed > 0) {
      double rx_gbps = (byte_delta * 8.0) / (elapsed * 1e9);
      double decomp_gbps =
          ((decomp_delta + uncomp_delta) * 8.0) / (elapsed * 1e9);

      printf("RX: %.3f Gbps (wire) | Decompressed: %.3f Gbps | "
             "%.0f pkt/s | Q:%u | Submitted:%lu Completed:%lu | Err:%lu\n",
             rx_gbps, decomp_gbps, pkt_delta / elapsed, total_depth,
             total_submitted, total_completed, total_errors);
    }

    last_packets = current_packets;
    last_bytes = current_bytes;
    last_decompressed = current_decompressed;
    last_uncompressed = current_uncompressed;
    last_time = now;
  }

  return NULL;
}

void cleanup_qat_buffers(ring_entry_t *entry) {
  if (entry->pSrcData)
    qaeMemFreeNUMA((void **)&entry->pSrcData);
  if (entry->pDstData)
    qaeMemFreeNUMA((void **)&entry->pDstData);
  if (entry->pFlatSrcBuffer)
    qaeMemFreeNUMA((void **)&entry->pFlatSrcBuffer);
  if (entry->pFlatDstBuffer)
    qaeMemFreeNUMA((void **)&entry->pFlatDstBuffer);
  if (entry->pSrcBuffer && entry->pSrcBuffer->pPrivateMetaData)
    qaeMemFreeNUMA((void **)&entry->pSrcBuffer->pPrivateMetaData);
  if (entry->pDstBuffer && entry->pDstBuffer->pPrivateMetaData)
    qaeMemFreeNUMA((void **)&entry->pDstBuffer->pPrivateMetaData);
  if (entry->pSrcBuffer)
    qaeMemFreeNUMA((void **)&entry->pSrcBuffer);
  if (entry->pDstBuffer)
    qaeMemFreeNUMA((void **)&entry->pDstBuffer);
}

void ring_buffer_cleanup(ring_buffer_t *ring) {
  if (!ring)
    return;

  printf("Cleaning up ring buffer for instance %p...\n", (void *)ring);

  if (ring->dcInstance) {
    cpaDcStopInstance(ring->dcInstance);
  }

  if (ring->sessionHandle) {
    cpaDcRemoveSession(ring->dcInstance, ring->sessionHandle);
  }

  if (ring->sessionMemory) {
    qaeMemFreeNUMA(&ring->sessionMemory);
  }

  for (int i = 0; i < RING_SIZE; i++) {
    cleanup_qat_buffers(&ring->entries[i]);
  }

  free(ring);
}

int main(int argc, char *argv[]) {
  const char *host = "192.168.100.2";
  int port = 9999;
  int num_connections = 1;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 && i + 1 < argc) {
      host = argv[++i];
    } else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
      port = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
      num_connections = atoi(argv[++i]);
    }
  }

  printf("=== Ring Buffer QAT Receiver ===\n");
  printf("Connections: %d | QAT Instances: %d\n", num_connections,
         NUM_QAT_INSTANCES);

  // Initialize all rings
  for (int i = 0; i < NUM_QAT_INSTANCES; i++) {
    if (!ring_buffer_init(i)) {
      printf("Failed to initialize ring buffer for instance %d\n", i);
      return 1;
    }
  }

  struct sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  inet_pton(AF_INET, host, &server_addr.sin_addr);

  // Global start time from first ring
  gettimeofday(&g_rings[0]->start_time, NULL);

  pthread_t *net_tids = malloc(num_connections * sizeof(pthread_t));
  pthread_t stats_tid;
  pthread_t decompress_tid;
  net_args_t *net_args = malloc(num_connections * sizeof(net_args_t));
  int *sockfds = malloc(num_connections * sizeof(int));

  for (int i = 0; i < num_connections; i++) {
    sockfds[i] = socket(AF_INET, SOCK_STREAM, 0);
    int rcvbuf = 1024 * 1024 * 1024;
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 0;
    setsockopt(sockfds[i], SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    setsockopt(sockfds[i], SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv,
               sizeof tv);

    printf("Connecting connection %d to %s:%d...\n", i, host, port);
    if (connect(sockfds[i], (struct sockaddr *)&server_addr,
                sizeof(server_addr)) < 0) {
      perror("connect");
      return 1;
    }

    net_args[i].sockfd = sockfds[i];
    // Distribute connections across rings
    net_args[i].ring = g_rings[i % NUM_QAT_INSTANCES];
    pthread_create(&net_tids[i], NULL, network_thread, &net_args[i]);
  }

  // Single stats and decompress thread
  pthread_create(&stats_tid, NULL, stats_thread, NULL);
  pthread_create(&decompress_tid, NULL, decompress_thread, NULL);

  printf("All threads started, waiting for completion...\n\n");

  for (int i = 0; i < num_connections; i++) {
    pthread_join(net_tids[i], NULL);
  }
  printf("All network threads completed\n");

  for (int i = 0; i < NUM_QAT_INSTANCES; i++) {
    g_rings[i]->running = false;
  }

  pthread_join(decompress_tid, NULL);
  printf("Decompress thread completed\n");

  pthread_join(stats_tid, NULL);

  // Final reporting (aggregate)
  double elapsed = 0;
  struct timeval end_time;
  gettimeofday(&end_time, NULL);
  elapsed = (end_time.tv_sec - g_rings[0]->start_time.tv_sec) +
            (end_time.tv_usec - g_rings[0]->start_time.tv_usec) / 1e6;

  uint64_t total_received_bytes = 0;
  uint64_t total_qat_in = 0, total_qat_out = 0;
  uint64_t total_uncomp_recv = 0, total_comp_recv = 0;

  for (int i = 0; i < NUM_QAT_INSTANCES; i++) {
    total_received_bytes += g_rings[i]->producer.bytes_received;
    total_qat_in += g_rings[i]->qat_stats.total_bytes_in;
    total_qat_out += g_rings[i]->qat_stats.total_bytes_out;
    total_uncomp_recv += g_rings[i]->uncompressed_packets_recv;
    total_comp_recv += g_rings[i]->compressed_packets_recv;
  }

  double network_in_gbps = (total_received_bytes * 8.0) / (elapsed * 1e9);
  double qat_tput_in = (total_qat_in * 8.0) / (elapsed * 1e9);
  double qat_tput_out = (total_qat_out * 8.0) / (elapsed * 1e9);

  printf("\n=== FINAL STATISTICS (AGGREGATED) ===\n");
  printf("Duration: %.2f sec\n", elapsed);
  printf("\nRX Statistics:\n");
  printf("    RX Network Bytes In: %lu --> Throughput(%.2f Gbps)\n",
         total_received_bytes, network_in_gbps);
  printf("    RX QAT Bytes In: %lu --> Throughput(%.2f Gbps)\n", total_qat_in,
         qat_tput_in);
  printf("    RX QAT Bytes Out: %lu --> Throughput(%.2f Gbps)\n", total_qat_out,
         qat_tput_out);
  printf("    RX Uncompressed Packets Recieved: %lu\n", total_uncomp_recv);
  printf("    RX Compressed Packets Recieved: %lu\n", total_comp_recv);

  for (int i = 0; i < num_connections; i++) {
    close(sockfds[i]);
  }
  for (int i = 0; i < NUM_QAT_INSTANCES; i++) {
    ring_buffer_cleanup(g_rings[i]);
  }
  icp_sal_userStop();

  free(net_tids);
  free(sockfds);
  free(net_args);

  printf("\nReceiver shutdown complete\n");
  return 0;
}
