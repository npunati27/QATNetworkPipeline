#define _GNU_SOURCE
#include <qat/cpa.h>
#include <qat/cpa_dc.h>
#include <qat/icp_sal_poll.h>
#include <qat/icp_sal_user.h>
#include <qat/qae_mem.h>
#include <sched.h>

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#define TEST_DATA_SIZE 64 * 1024
#define RING_SIZE 32768
#define CACHE_LINE_SIZE 64
#define MAX_BUFFER_SIZE 128 * 1024
#define SAMPLE_SIZE 256
#define SEND_QUEUE_SIZE 128 * 1024 // 128
#define NUM_PRODUCERS 16
#define PRODUCERS_PER_INSTANCE 4
#define NUM_QAT_INSTANCES 4
#define NUM_CONSUMERS 4
// #define NUM_SENDERS NUM_QAT_INSTANCES
#define MAGIC_COMPRESSED 0x51415443
#define MAGIC_UNCOMPRESSED 0x51415452

static int g_num_senders = NUM_QAT_INSTANCES; // Default

static bool g_debug_enabled = false;

#define DEBUG_PRINT(fmt, ...)                                                  \
  do {                                                                         \
    if (g_debug_enabled)                                                       \
      fprintf(stderr, "[DEBUG] " fmt "", ##__VA_ARGS__);                       \
  } while (0)

#define INFO_PRINT(fmt, ...) fprintf(stderr, "[INFO] " fmt "", ##__VA_ARGS__)

typedef enum {
  JOB_EMPTY = 0,
  JOB_SUBMITTED = 1,
  JOB_COMPLETED = 2,
  JOB_ERROR = 3,
  SLOT_CLAIM = 5
} job_status_t;

typedef struct ring_buffer_s ring_buffer_t;

typedef struct {
  uint32_t magic; // helps receiver recognize compressed packets
  uint32_t uncompressed_size;
  uint32_t compressed_size;
  uint64_t sequence_number;
  uint8_t algorithm;
  uint8_t compression_level;
  uint16_t checksum;
} __attribute__((packed)) compression_header_t;

/* Ring Buffer Entry:
 * scatterlist for QAT source + destination buffer
 * flat buffer stores contiguous memory region and has data buffer pointer
 * compression job results (dcResults)
 */
typedef struct {
  volatile job_status_t status;
  uint64_t sequence_number;

  // scatterlist (required for submission) DMA
  // contains multiple CpaFlatBuffer entries
  CpaBufferList *pSrcBuffer;
  CpaBufferList *pDstBuffer;

  // represents a contiguous memory region
  CpaFlatBuffer *pFlatSrcBuffer;
  CpaFlatBuffer *pFlatDstBuffer;

  // data buffer pointer
  Cpa8U *pSrcData;
  Cpa8U *pDstData;

  // metadata necessary for correctnesns
  Cpa32U srcDataSize;
  Cpa32U dstDataSize;
  Cpa32U producedSize;

  CpaDcRqResults dcResults;

  compression_header_t header;

  uint64_t submit_time;
  uint64_t complete_time;

  ring_buffer_t *parent_ring;
} ring_entry_t;

// compressed send queue entry is a ring entry
typedef struct {
  ring_entry_t *entry;
  volatile int busy;
} compressed_send_entry_t;

// compressed send queue
typedef struct {
  compressed_send_entry_t entries[SEND_QUEUE_SIZE];
  volatile uint32_t head;
  volatile uint32_t tail;
  uint32_t mask;
  char pad[CACHE_LINE_SIZE];
} __attribute__((aligned(CACHE_LINE_SIZE))) compressed_send_queue_t;

// uncompressed send entry just has pointer to data
typedef struct {
  compression_header_t header;
  uint8_t *data;
  size_t length;
  uint64_t sequence_number;
  volatile int ready;
} uncompressed_send_entry_t;

// uncompressed send queue
typedef struct {
  uncompressed_send_entry_t entries[SEND_QUEUE_SIZE];
  volatile uint32_t head;
  volatile uint32_t tail;
  uint32_t mask;
  char pad[CACHE_LINE_SIZE];
} __attribute__((aligned(CACHE_LINE_SIZE))) uncompressed_send_queue_t;

/* Ring Buffer:
 * Ring Buffer Entries
 * File Reader
 * producer head, consumer tail
 * compression instance handle
 * compressed/uncompressed send queue
 * statistics trackers
 */
struct ring_buffer_s {
  ring_entry_t entries[RING_SIZE];
  int compression_level;
  float compressed_fraction;
  int instance_id;

  struct {
    volatile uint32_t tail;
    uint64_t jobs_submitted;
    uint64_t app_bytes_generated;
    char pad[CACHE_LINE_SIZE - 16];
  } __attribute__((aligned(CACHE_LINE_SIZE))) producer;

  struct {
    volatile uint32_t head;
    uint64_t jobs_sent;
    uint64_t bytes_sent;
    char pad[CACHE_LINE_SIZE - 24];
  } __attribute__((aligned(CACHE_LINE_SIZE))) consumer;

  CpaInstanceHandle dcInstance;
  CpaDcSessionHandle sessionHandle;
  CpaDcSessionSetupData sessionSetupData;

  uint32_t mask;
  // int socket_fd;
  volatile bool running;

  compressed_send_queue_t compressed_send_queue;
  uncompressed_send_queue_t uncompressed_send_queue;

  volatile uint64_t sequence_counter;
  volatile uint64_t uncompressed_packets_sent;
  volatile uint64_t compressed_packets_sent;
  volatile uint64_t *socket_blocked_count;
  volatile uint64_t network_bytes_in;
  volatile uint64_t network_bytes_out;
  struct {
    uint64_t total_compressions;
    uint64_t total_bytes_in;
    uint64_t total_bytes_out;
    uint64_t total_compression_time_us;
    uint64_t min_compression_time_us;
    uint64_t max_compression_time_us;
    struct timespec first_submit_time;
    struct timespec last_callback_time;
    volatile bool timing_started;
  } qat_stats;
};

// args for network, consumer, producer threads
typedef struct {
  ring_buffer_t *ring;
  int thread_id;
  int core_id;
  int instance_id;
  const char *file_path;
} thread_args_t;

// static ring_buffer_t *g_ring = NULL;

typedef struct {
  ring_buffer_t *rings[NUM_QAT_INSTANCES];
  int *socket_fds;
  volatile bool running;
  bool qat_instances_used[16];
} global_state_t;

static global_state_t *g_state = NULL;

void print_ring_status_summary(global_state_t *state) {

  for (int j = 0; j < NUM_QAT_INSTANCES; j++) {
    uint32_t empty = 0;
    uint32_t submitted = 0;
    uint32_t completed = 0;
    uint32_t error = 0;
    uint32_t unknown = 0;
    uint32_t claimed = 0;

    for (uint32_t i = 0; i < RING_SIZE; i++) {
      ring_buffer_t *ring = g_state->rings[j];
      job_status_t status =
          __atomic_load_n(&ring->entries[i].status, __ATOMIC_ACQUIRE);

      switch (status) {
      case JOB_EMPTY:
        empty++;
        break;
      case JOB_SUBMITTED:
        submitted++;
        break;
      case JOB_COMPLETED:
        completed++;
        break;
      case JOB_ERROR:
        error++;
        break;
      case SLOT_CLAIM:
        claimed++;
        break;
      default:
        unknown++;
        break;
      }
    }

    uint32_t head =
        __atomic_load_n(&(g_state->rings[j])->consumer.head, __ATOMIC_ACQUIRE);
    uint32_t tail =
        __atomic_load_n(&(g_state->rings[j])->producer.tail, __ATOMIC_ACQUIRE);
    uint32_t in_flight = (tail - head) & (g_state->rings[j])->mask;

    DEBUG_PRINT("\n--- Ring Buffer Snapshot ---\n");
    DEBUG_PRINT("  [EMPTY]     : %u\n", empty);
    DEBUG_PRINT("  [SUBMITTED] : %u (Pending QAT Hardware)\n", submitted);
    DEBUG_PRINT("  [COMPLETED] : %u (Waiting for Consumer/Network)\n",
                completed);
    DEBUG_PRINT("  [ERROR]     : %u (Waiting for Consumer Cleanup)\n", error);
    DEBUG_PRINT("  [CLAIM]     : %u (Waiting for Consumer Cleanup)\n", claimed);
    if (unknown > 0)
      printf("  [UNKNOWN!!] : %u\n", unknown);
    DEBUG_PRINT("----------------------------\n");
    DEBUG_PRINT("  Pointer Logic: Head=%u, Tail=%u (Occupancy: %u/%d)\n", head,
                tail, in_flight, RING_SIZE);
    ring_entry_t *entry = &(g_state->rings[j])->entries[head];
    DEBUG_PRINT("Head STATUS: %d\n", entry->status);
  }
}

// ensure each thread is pinned to different cores (on same NUMA node)
int pin_thread_to_core(int core_id) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);

  pthread_t current_thread = pthread_self();
  int result =
      pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);

  if (result == 0) {
    INFO_PRINT("Thread pinned to core %d\n", core_id);
  } else {
    fprintf(stderr, "Failed to pin thread to core %d: %s\n", core_id,
            strerror(result));
  }

  return result;
}

// callback gets fired on polling when QAT jobs complete
void qat_dc_callback(void *pCallbackTag, CpaStatus status) {
  static uint64_t callback_count = 0;
  callback_count++;

  ring_entry_t *entry = (ring_entry_t *)pCallbackTag;
  ring_buffer_t *ring = entry->parent_ring;

  if (status == CPA_STATUS_SUCCESS) {
    uint64_t compression_time = clock() - entry->submit_time;
    uint64_t time_us = (compression_time * 1000000) / CLOCKS_PER_SEC;

    __sync_fetch_and_add(&ring->qat_stats.total_compressions, 1);
    __sync_fetch_and_add(&ring->qat_stats.total_bytes_in, entry->srcDataSize);
    __sync_fetch_and_add(&ring->qat_stats.total_bytes_out, entry->producedSize);
    __sync_fetch_and_add(&ring->qat_stats.total_compression_time_us, time_us);
    clock_gettime(CLOCK_MONOTONIC, &ring->qat_stats.last_callback_time);

    entry->producedSize = entry->dcResults.produced;
    entry->header.compressed_size = entry->producedSize;
    entry->header.checksum = 0;

    __atomic_store_n(&entry->status, JOB_COMPLETED, __ATOMIC_RELEASE);

    if (callback_count % 100000 == 0) {
      DEBUG_PRINT("Callback: %lu successful compressions\n", callback_count);
    }
  } else {
    printf("QAT compression failed: %d (callback #%lu)\n", status,
           callback_count);
    __sync_synchronize();
    entry->status = JOB_ERROR;
  }

  entry->complete_time = clock();
}

/* Initialize QAT
 * Find instances on NUMA Nodes 2 and 3
 * Set up Address Translation for DMA
 * Set up session data (algorithm, level, etc)
 */
int qat_init(ring_buffer_t *ring, int instance_target_index) {
  CpaStatus status;
  Cpa16U numInstances = 0;
  CpaInstanceHandle *instances = NULL;
  Cpa32U sessionSize = 0;
  Cpa32U contextSize = 0;

  status = cpaDcGetNumInstances(&numInstances);
  if (status != CPA_STATUS_SUCCESS || numInstances == 0) {
    printf("No QAT DC instances available\n");
    return -1;
  }

  instances = malloc(numInstances * sizeof(CpaInstanceHandle));
  status = cpaDcGetInstances(numInstances, instances);
  if (status != CPA_STATUS_SUCCESS) {
    printf("Failed to get DC instances\n");
    free(instances);
    return -1;
  }

  int target_numa_nodes[] = {0, 1, 2, 3};
  int target_numa_node = target_numa_nodes[instance_target_index % 4];
  int selected_instance = -1;
  int instances_on_numa[16] = {0};
  int numa_instance_count = 0;

  // First pass: find all instances on target NUMA node
  for (int i = 0; i < numInstances; i++) {
    CpaInstanceInfo2 info;
    status = cpaDcInstanceGetInfo2(instances[i], &info);

    if (status == CPA_STATUS_SUCCESS) {
      DEBUG_PRINT("Instance %d: nodeAffinity = %d\n", i, info.nodeAffinity);

      if (info.nodeAffinity == target_numa_node) {
        instances_on_numa[numa_instance_count++] = i;
      }
    }
  }

  // Find an unused instance on the target NUMA node
  if (numa_instance_count == 0) {
    // Fallback: no instances on target NUMA, find any unused instance
    for (int i = 0; i < numInstances; i++) {
      if (!g_state->qat_instances_used[i]) {
        selected_instance = i;
        INFO_PRINT("Warning: No QAT on NUMA %d, using instance %d from "
                   "different NUMA\n",
                   target_numa_node, selected_instance);
        break;
      }
    }

    if (selected_instance == -1) {
      printf("Error: All QAT instances are already in use\n");
      free(instances);
      return -1;
    }
  } else {
    // Find an unused instance on target NUMA node
    bool found = false;
    for (int i = 0; i < numa_instance_count; i++) {
      int instance_id = instances_on_numa[i];
      if (!g_state->qat_instances_used[instance_id]) {
        selected_instance = instance_id;
        found = true;
        INFO_PRINT("Selected QAT instance %d (NUMA-local instance #%d) on NUMA "
                   "node %d\n",
                   selected_instance, i, target_numa_node);
        break;
      }
    }

    if (!found) {
      printf("Error: All QAT instances on NUMA %d are already in use\n",
             target_numa_node);
      free(instances);
      return -1;
    }
  }

  // Mark this instance as used
  g_state->qat_instances_used[selected_instance] = true;

  ring->dcInstance = instances[selected_instance];
  ring->instance_id = instance_target_index;
  free(instances);

  status = cpaDcSetAddressTranslation(ring->dcInstance, qaeVirtToPhysNUMA);
  if (status != CPA_STATUS_SUCCESS) {
    printf("Failed to set address translation: %d\n", status);
    return -1;
  }

  Cpa16U numBuffers = 4096;
  status = cpaDcStartInstance(ring->dcInstance, numBuffers, NULL);
  if (status != CPA_STATUS_SUCCESS) {
    printf("Failed to start DC instance: %d\n", status);
    return -1;
  }

  CpaDcCompLvl qat_level;
  switch (ring->compression_level) {
  case 1:
    qat_level = CPA_DC_L1;
    break;
  case 2:
    qat_level = CPA_DC_L2;
    break;
  case 3:
    qat_level = CPA_DC_L3;
    break;
  case 4:
    qat_level = CPA_DC_L4;
    break;
  case 5:
    qat_level = CPA_DC_L5;
    break;
  case 6:
    qat_level = CPA_DC_L6;
    break;
  case 7:
    qat_level = CPA_DC_L7;
    break;
  case 8:
    qat_level = CPA_DC_L8;
    break;
  case 9:
    qat_level = CPA_DC_L9;
    break;
  default:
    qat_level = CPA_DC_L1;
  }

  ring->sessionSetupData.compLevel = qat_level;
  ring->sessionSetupData.compType = CPA_DC_DEFLATE;   // DEFLATE
  ring->sessionSetupData.huffType = CPA_DC_HT_STATIC; // static huffman
  ring->sessionSetupData.autoSelectBestHuffmanTree = CPA_FALSE;
  ring->sessionSetupData.sessDirection = CPA_DC_DIR_COMPRESS;
  ring->sessionSetupData.sessState = CPA_DC_STATELESS;
  ring->sessionSetupData.checksum = CPA_DC_CRC32;
  ring->sessionSetupData.windowSize = 15;

  // ring->sessionSetupData.compLevel = qat_level;
  // ring->sessionSetupData.compType = CPA_DC_LZ4;              // config for
  // LZ4 ring->sessionSetupData.sessDirection = CPA_DC_DIR_COMPRESS;
  // ring->sessionSetupData.sessState = CPA_DC_STATELESS;
  // ring->sessionSetupData.checksum = CPA_DC_NONE;

  status = cpaDcGetSessionSize(ring->dcInstance, &ring->sessionSetupData,
                               &sessionSize, &contextSize);
  if (status != CPA_STATUS_SUCCESS) {
    printf("Failed to get session size\n");
    return -1;
  }

  void *pSessionMemory = qaeMemAllocNUMA(sessionSize, 0, 64);
  if (pSessionMemory == NULL) {
    printf("Failed to allocate session memory\n");
    return -1;
  }

  ring->sessionHandle = pSessionMemory;
  status = cpaDcInitSession(ring->dcInstance, ring->sessionHandle,
                            &ring->sessionSetupData, pSessionMemory,
                            qat_dc_callback);

  if (status != CPA_STATUS_SUCCESS) {
    printf("Failed to init session: %d\n", status);
    qaeMemFreeNUMA(&pSessionMemory);
    return -1;
  }

  INFO_PRINT("QAT instance %d initialization successful\n",
             instance_target_index);
  return 0;
}

// Function to allocate buffers for each ring entry
int allocate_qat_buffers(ring_entry_t *entry, CpaInstanceHandle dcInstance) {
  Cpa32U metaSize = 0;
  CpaStatus status;

  status = cpaDcBufferListGetMetaSize(dcInstance, 1, &metaSize);
  if (status != CPA_STATUS_SUCCESS) {
    printf("Failed to get metadata size\n");
    return -1;
  }

  // Allocate source buffer list structure
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

  entry->pSrcData = qaeMemAllocNUMA(MAX_BUFFER_SIZE, 0, 64);
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

// Set up Ring Buffer - file reader (load benchmark file), send queues, ring
// buffer entries, initialize qat
ring_buffer_t *ring_buffer_init(int instance_id, int compression_level,
                                float compressed_fraction) {
  ring_buffer_t *ring = aligned_alloc(CACHE_LINE_SIZE, sizeof(ring_buffer_t));
  if (!ring)
    return NULL;

  memset(ring, 0, sizeof(ring_buffer_t));
  ring->compression_level = compression_level;
  ring->compressed_fraction = compressed_fraction;

  ring->compressed_send_queue.mask = SEND_QUEUE_SIZE - 1;
  ring->uncompressed_send_queue.mask = SEND_QUEUE_SIZE - 1;

  ring->socket_blocked_count = calloc(g_num_senders, sizeof(uint64_t));
  if (!ring->socket_blocked_count) {
    free(ring);
    return NULL;
  }

  ring->compressed_send_queue.head = 0;
  ring->compressed_send_queue.tail = 0;
  ring->compressed_send_queue.mask = SEND_QUEUE_SIZE - 1;

  ring->uncompressed_send_queue.head = 0;
  ring->uncompressed_send_queue.tail = 0;
  ring->uncompressed_send_queue.mask = SEND_QUEUE_SIZE - 1;

  for (int i = 0; i < SEND_QUEUE_SIZE; i++) {
    ring->uncompressed_send_queue.entries[i].data = malloc(MAX_BUFFER_SIZE);
    if (!ring->uncompressed_send_queue.entries[i].data) {
      printf("Failed to allocate uncompressed buffer\n");
      return NULL;
    }
  }

  if (qat_init(ring, instance_id) != 0) {
    free(ring);
    return NULL;
  }

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

  return ring;
}

/* PRODUCER THREAD
 * Depending on user input compression fraction, reads file data and submits
 * compression jobs (or leaves data uncompressed)
 * TODO: change this to choose compressed path except when ring buffer full OR
 * QAT backpressure 64 KB chunks per compression job
 */
void *producer_thread(void *arg) {
  thread_args_t *args = (thread_args_t *)arg;
  ring_buffer_t *ring = args->ring;
  int thread_id = args->thread_id;
  int instance_id = args->instance_id;
  const char *file_path = args->file_path;
  pin_thread_to_core(args->core_id);
  INFO_PRINT("Producer thread %d started on core %d (QAT instance %d)\n",
             thread_id, args->core_id, instance_id);

  // CpaStatus status;
  uint64_t ring_full_count = 0;
  uint64_t queue_full_count = 0;
  uint64_t qat_backpressure_count = 0;
  uint64_t bytes_generated = 0;
  uint64_t packets_generated = 0;
  time_t last_report = time(NULL);
  int local_fd = open(file_path, O_RDONLY);
  if (local_fd < 0) {
    fprintf(stderr, "Producer %d: Failed to open file %s: %s\n", thread_id,
            file_path, strerror(errno));
    return NULL;
  }

  uint64_t my_offset = thread_id * (64 * 1024);
  const size_t CHUNK_SIZE = 64 * 1024;
  // char temp_buffer[CHUNK_SIZE];

  while (ring->running && g_state->running) {
    uint8_t temp_buffer[CHUNK_SIZE];
    ssize_t bytes_read = pread(local_fd, temp_buffer, CHUNK_SIZE, my_offset);

    if (bytes_read <= 0) {
      my_offset = 0;
      continue;
    }
    my_offset += bytes_read;

    uint64_t current_seq = __sync_fetch_and_add(&ring->sequence_counter, 1);
    uint32_t current_tail, next_tail;
    ring_entry_t *entry = NULL;
    bool ring_claimed = false;
    bool qat_submitted = false;

    // Optimized lock-free ring claim - no retry here, if ring full go
    // uncompressed
    current_tail = __atomic_load_n(&ring->producer.tail, __ATOMIC_ACQUIRE);
    next_tail = (current_tail + 1) & ring->mask;
    entry = &ring->entries[current_tail];

    if (next_tail != __atomic_load_n(&ring->consumer.head, __ATOMIC_ACQUIRE)) {
      if (__atomic_load_n(&entry->status, __ATOMIC_ACQUIRE) == JOB_EMPTY) {
        if (__atomic_compare_exchange_n(
                &entry->status, &(job_status_t){JOB_EMPTY}, SLOT_CLAIM, false,
                __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
          __atomic_store_n(&ring->producer.tail, next_tail, __ATOMIC_RELEASE);
          ring_claimed = true;
        }
      }
    }

    if (ring_claimed) {
      // Prepare and submit to QAT
      memcpy(entry->pSrcData, temp_buffer, bytes_read);
      entry->srcDataSize = bytes_read;
      entry->pFlatSrcBuffer->dataLenInBytes = bytes_read;
      entry->pFlatDstBuffer->dataLenInBytes = MAX_BUFFER_SIZE * 2;

      entry->sequence_number = current_seq;
      entry->header.magic = MAGIC_COMPRESSED;
      entry->header.sequence_number = entry->sequence_number;
      entry->header.uncompressed_size = bytes_read;
      entry->header.algorithm = 0;
      entry->header.compression_level = ring->compression_level;

      CpaDcOpData opData = {0};
      opData.flushFlag = CPA_DC_FLUSH_FINAL;
      opData.compressAndVerify = CPA_TRUE;
      entry->submit_time = clock();

      // Try QAT submission exactly ONCE for maximum throughput
      CpaStatus status = cpaDcCompressData2(
          ring->dcInstance, ring->sessionHandle, entry->pSrcBuffer,
          entry->pDstBuffer, &opData, &entry->dcResults, entry);

      if (status == CPA_STATUS_SUCCESS) {
        if (!ring->qat_stats.timing_started) {
          clock_gettime(CLOCK_MONOTONIC, &ring->qat_stats.first_submit_time);
          __sync_synchronize();
          ring->qat_stats.timing_started = true;
        }
        __atomic_store_n(&entry->status, JOB_SUBMITTED, __ATOMIC_RELEASE);
        __sync_fetch_and_add(&ring->producer.jobs_submitted, 1);
        __sync_fetch_and_add(&ring->producer.app_bytes_generated, bytes_read);
        bytes_generated += bytes_read;
        packets_generated++;
        qat_submitted = true;
      } else {
        // Hardware busy or error: clean up the ring slot and fall back
        qat_backpressure_count++;
        __atomic_store_n(&entry->status, JOB_ERROR, __ATOMIC_RELEASE);
      }
    }

    if (!qat_submitted) {
      // UNCOMPRESSED FALLBACK PATH
      uncompressed_send_queue_t *queue = &ring->uncompressed_send_queue;
      uint32_t u_current_tail, u_next_tail;

      u_current_tail = __atomic_load_n(&queue->tail, __ATOMIC_ACQUIRE);
      u_next_tail = (u_current_tail + 1) & queue->mask;

      if (u_next_tail != __atomic_load_n(&queue->head, __ATOMIC_ACQUIRE) &&
          __atomic_load_n(&queue->entries[u_current_tail].ready,
                          __ATOMIC_ACQUIRE) == 0) {
        if (__atomic_compare_exchange_n(&queue->tail, &u_current_tail,
                                        u_next_tail, false, __ATOMIC_RELEASE,
                                        __ATOMIC_ACQUIRE)) {

          uncompressed_send_entry_t *u_entry = &queue->entries[u_current_tail];
          memcpy(u_entry->data, temp_buffer, bytes_read);
          u_entry->length = bytes_read;
          u_entry->sequence_number = current_seq;
          u_entry->header.magic = MAGIC_UNCOMPRESSED;
          u_entry->header.sequence_number = u_entry->sequence_number;
          u_entry->header.uncompressed_size = bytes_read;
          u_entry->header.compressed_size = bytes_read;
          u_entry->header.algorithm = 99;
          __atomic_store_n(&u_entry->ready, 1, __ATOMIC_RELEASE);

          __sync_fetch_and_add(&ring->producer.app_bytes_generated, bytes_read);
          bytes_generated += bytes_read;
          packets_generated++;
        }
      } else {
        queue_full_count++;
      }
    }

    if (packets_generated % 10000 == 0) {
      time_t now = time(NULL);
      if (now - last_report >= 5) {
        double elapsed = (double)(now - last_report);
        double gbps = (bytes_generated * 8.0 / elapsed) / 1000000000.0;

        INFO_PRINT(
            "Producer %d (Inst %d): %.3f Gbps | RingFull: %lu | QATBusy: "
            "%lu | UncompQFull: %lu\n",
            thread_id, instance_id, gbps, ring_full_count,
            qat_backpressure_count, queue_full_count);

        bytes_generated = 0;
        ring_full_count = 0;
        qat_backpressure_count = 0;
        queue_full_count = 0;
        last_report = now;
      }
    }
  }

  close(local_fd);
  INFO_PRINT("Producer thread %d (instance %d) exiting\n", thread_id,
             instance_id);
  return NULL;
}

/* CONSUMER THREAD
 * Polls QAT so callbacks fire
 * Polls on ring buffer head for status to be COMPLETED
 * Pushes results into the compressed send queue
 */
void *consumer_thread(void *arg) {
  thread_args_t *args = (thread_args_t *)arg;
  ring_buffer_t *ring = args->ring;
  int thread_id = args->thread_id;
  int instance_id = args->instance_id;
  pin_thread_to_core(args->core_id);
  // bool stuck_reached = false;
  INFO_PRINT("Consumer thread %d started on core %d (QAT instance %d)\n",
             thread_id, args->core_id, instance_id);

  uint64_t send_count = 0;
  uint64_t stuck_count = 0;
  uint64_t compressed_full = 0;
  uint64_t poll_count = 0;

  while (ring->running && g_state->running) {
    poll_count++;
    // Poll QAT instance
    CpaStatus poll_status = icp_sal_DcPollInstance(ring->dcInstance, 0);
    if (poll_status != CPA_STATUS_SUCCESS && poll_status != CPA_STATUS_RETRY) {
      if (stuck_count % 10000000 == 0) {
        INFO_PRINT("Consumer: Poll failed with status %d\n", poll_status);
      }
    }
    // if(poll_count % 100000000 == 0) {
    //     DEBUG_PRINT("Consumer: Polled %lu times...\n", poll_count);
    // }

    uint32_t current_head = ring->consumer.head;
    ring_entry_t *entry = &ring->entries[current_head];
    job_status_t current_status =
        __atomic_load_n(&entry->status, __ATOMIC_ACQUIRE);

    if (poll_count % 100000000 == 0) {
      DEBUG_PRINT("Consumer %d (Inst %d): Polled %lu times, HEAD STATUS: %d\n",
                  thread_id, instance_id, poll_count, current_status);
    }
    if (current_status == JOB_COMPLETED) {
      // current head is completed
      stuck_count = 0;
      compressed_send_queue_t *queue = &ring->compressed_send_queue;
      uint32_t next_tail = (queue->tail + 1) & queue->mask;

      // compressed send queue is full
      if (next_tail == queue->head) {
        static uint64_t discard_count = 0;
        discard_count++;

        if (compressed_full % 1000000 == 0) {
          DEBUG_PRINT("Consumer: Compressed Queue Full, We are Spinning...\n");
        }
        compressed_full++;

        if (discard_count % 1000000 == 0) {
          DEBUG_PRINT("Consumer: Discarded %lu compressed entries\n",
                      discard_count);
        }

        // after a while change the status to empty and move to next
        // compressed send queue entry
        //  ring->consumer.head = (current_head + 1) & ring->mask;
        uint32_t next_head = (current_head + 1) & ring->mask;
        __atomic_store_n(&ring->consumer.head, next_head, __ATOMIC_RELEASE);
        __atomic_store_n(&entry->status, JOB_EMPTY, __ATOMIC_RELEASE);
        continue;
      }

      while (__atomic_load_n(&queue->entries[queue->tail].busy,
                             __ATOMIC_ACQUIRE) != 0) {
        if (!ring->running)
          return NULL;
        __builtin_ia32_pause();
      }

      queue->entries[queue->tail].entry = entry;
      __atomic_store_n(&queue->entries[queue->tail].busy, 1, __ATOMIC_RELEASE);
      __atomic_store_n(&queue->tail, next_tail, __ATOMIC_RELEASE);

      // mark job as empty after its complete and advance head
      //__atomic_store_n(&entry->status, JOB_EMPTY, __ATOMIC_RELEASE);
      ring->consumer.jobs_sent++;
      uint32_t next_head = (current_head + 1) & ring->mask;
      __atomic_store_n(&ring->consumer.head, next_head, __ATOMIC_RELEASE);

      send_count++;
      if (send_count % 10000 == 0) {
        DEBUG_PRINT("Consumer: Sent %lu entries to network thread\n",
                    send_count);
      }

    } else if (current_status == JOB_ERROR) {
      // job is error state, change it emoty and advance head
      __atomic_store_n(&entry->status, JOB_EMPTY, __ATOMIC_RELEASE);
      uint32_t next_head = (current_head + 1) & ring->mask;
      __atomic_store_n(&ring->consumer.head, next_head, __ATOMIC_RELEASE);
      stuck_count = 0;
      // INFO_PRINT("Consumer: JOB %lu STUCK IN ERROR\n",
      // entry->sequence_number);
      //  INFO_PRINT("  Entry details: seq=%lu, srcSize=%u, dstSize=%u\n",
      //          entry->sequence_number, entry->srcDataSize,
      //          entry->dstDataSize);
    } else if (current_status == JOB_SUBMITTED) {
      // job is stuck in submitted..
      stuck_count++;

      if (stuck_count % 1000000 == 0) {
        // INFO_PRINT("Consumer: JOB STUCK IN SUBMITTED %lu times\n",
        // stuck_count); INFO_PRINT("  Forcing to ERROR state and
        // skipping...\n"); entry->status = JOB_EMPTY; ring->consumer.head =
        // (current_head + 1) & ring->mask; stuck_count = 0;
        INFO_PRINT("Consumer: JOB %lu STUCK IN SUBMITTED %lu times\n",
                   entry->sequence_number, stuck_count);
        // INFO_PRINT("  Entry details: seq=%lu, srcSize=%u, dstSize=%u\n",
        // entry->sequence_number, entry->srcDataSize, entry->dstDataSize);
        __atomic_store_n(&entry->status, JOB_EMPTY, __ATOMIC_RELEASE);
        uint32_t next_head = (current_head + 1) & ring->mask;
        __atomic_store_n(&ring->consumer.head, next_head, __ATOMIC_RELEASE);
        // entry->status = JOB_EMPTY;
        // ring->consumer.head = (current_head + 1) & ring->mask;
        // stuck_count = 0;
      }
    } else if (current_status == JOB_EMPTY) {
      // uint32_t tail = __atomic_load_n(&ring->producer.tail,
      // __ATOMIC_ACQUIRE); uint32_t occupancy = (tail - current_head) &
      // ring->mask;

      // empty_count++;
      // if(empty_count % 1000000 == 0) {
      //     uint32_t next_head = (current_head + 1) & ring->mask;
      //     __atomic_store_n(&ring->consumer.head, next_head,
      //     __ATOMIC_RELEASE); INFO_PRINT("Consumer: Mitigated leaked slot at
      //     %u\n", current_head);
      // }
      uint32_t next_head = (current_head + 1) & ring->mask;
      __atomic_store_n(&ring->consumer.head, next_head, __ATOMIC_RELEASE);

      // // Is the ring mathematically full? (e.g., 10239/10240)
      // if (occupancy >= (ring->mask - 10)) {
      //     // We have a deadlock. A producer claimed this but never filled
      //     it.
      //     // We MUST advance to keep the 40Gbps flow alive.
      //     uint32_t next_head = (current_head + 1) & ring->mask;
      //     __atomic_store_n(&ring->consumer.head, next_head,
      //     __ATOMIC_RELEASE);

      //     // Log this! If this happens a lot, your producer logic has a
      //     bug. static uint64_t leaks = 0; if (leaks++ % 100 == 0)
      //     INFO_PRINT("Consumer: Mitigated leaked slot at %u\n",
      //     current_head);
      // }
    } else if (current_status == SLOT_CLAIM) {
      continue;
    }
  }

  INFO_PRINT("Consumer thread exiting\n");
  return NULL;
}

// Helper functions - same as before
bool try_send_compressed(ring_buffer_t *ring, int socket_fd, uint64_t *counter,
                         int thread_id) {
  compressed_send_queue_t *comp_queue = &ring->compressed_send_queue;
  uint32_t head, tail, next_head;

  do {
    head = __atomic_load_n(&comp_queue->head, __ATOMIC_ACQUIRE);
    tail = __atomic_load_n(&comp_queue->tail, __ATOMIC_ACQUIRE);

    if (head == tail) {
      return false;
    }

    next_head = (head + 1) & comp_queue->mask;

    if (__atomic_compare_exchange_n(&comp_queue->head, &head, next_head, false,
                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
      break;
    }
  } while (true);

  compressed_send_entry_t *send_msg = &comp_queue->entries[head];
  ring_entry_t *entry = send_msg->entry;

  if (socket_fd > 0) {
    struct iovec iov[2];
    iov[0].iov_base = &entry->header;
    iov[0].iov_len = sizeof(compression_header_t);
    iov[1].iov_base = entry->pDstData;
    iov[1].iov_len = entry->producedSize;

    ssize_t total_to_send = sizeof(compression_header_t) + entry->producedSize;
    ssize_t total_sent = 0;

    struct iovec current_iov[2];
    current_iov[0] = iov[0];
    current_iov[1] = iov[1];

    while (total_sent < total_to_send) {
      ssize_t sent = writev(socket_fd, current_iov, 2);

      if (sent > 0) {
        total_sent += sent;
        __atomic_fetch_add(&ring->network_bytes_out, sent, __ATOMIC_RELAXED);

        if (total_sent >= total_to_send)
          break;

        if ((size_t)sent >= current_iov[0].iov_len) {
          size_t overflow = sent - current_iov[0].iov_len;
          current_iov[0].iov_len = 0;
          current_iov[1].iov_base =
              (uint8_t *)current_iov[1].iov_base + overflow;
          current_iov[1].iov_len -= overflow;
        } else {
          current_iov[0].iov_base = (uint8_t *)current_iov[0].iov_base + sent;
          current_iov[0].iov_len -= sent;
        }
      } else if (sent < 0 &&
                 (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        if (errno != EINTR) {
          __atomic_fetch_add(&ring->socket_blocked_count[thread_id], 1,
                             __ATOMIC_RELAXED);
        }
        __builtin_ia32_pause();
        continue;
      } else {
        perror("compressed send failed");
        break;
      }
    }

    if (total_sent == total_to_send) {
      ring->consumer.bytes_sent += entry->producedSize;
      __sync_fetch_and_add(counter, 1);
      __atomic_fetch_add(&ring->compressed_packets_sent, 1, __ATOMIC_RELAXED);
      __atomic_fetch_add(&ring->network_bytes_in,
                         sizeof(compression_header_t) + entry->producedSize,
                         __ATOMIC_RELAXED);
      __atomic_store_n(&entry->status, JOB_EMPTY, __ATOMIC_RELEASE);
      __atomic_store_n(&send_msg->busy, 0, __ATOMIC_RELEASE);
      return true;
    } else {
      __atomic_store_n(&entry->status, JOB_EMPTY, __ATOMIC_RELEASE);
      __atomic_store_n(&send_msg->busy, 0, __ATOMIC_RELEASE);
      return false;
    }
  }

  __atomic_store_n(&entry->status, JOB_EMPTY, __ATOMIC_RELEASE);
  __atomic_store_n(&send_msg->busy, 0, __ATOMIC_RELEASE);
  return false;
}

bool try_send_uncompressed(ring_buffer_t *ring, int socket_fd,
                           uint64_t *counter, int thread_id) {
  uncompressed_send_queue_t *uncomp_queue = &ring->uncompressed_send_queue;
  uint32_t head, tail, next_head;

  do {
    head = __atomic_load_n(&uncomp_queue->head, __ATOMIC_ACQUIRE);
    tail = __atomic_load_n(&uncomp_queue->tail, __ATOMIC_ACQUIRE);

    if (head == tail) {
      return false;
    }

    next_head = (head + 1) & uncomp_queue->mask;

    if (__atomic_compare_exchange_n(&uncomp_queue->head, &head, next_head,
                                    false, __ATOMIC_ACQ_REL,
                                    __ATOMIC_ACQUIRE)) {
      break;
    }
  } while (true);

  // Claimed slot 'head'. Now we own it.
  uncompressed_send_entry_t *u_entry = &uncomp_queue->entries[head];

  // Wait for data to be ready (Producer moves tail BEFORE writing data)
  while (__atomic_load_n(&u_entry->ready, __ATOMIC_ACQUIRE) == 0) {
    __builtin_ia32_pause();
  }

  if (socket_fd > 0) {
    struct iovec iov[2];
    iov[0].iov_base = &u_entry->header;
    iov[0].iov_len = sizeof(compression_header_t);
    iov[1].iov_base = u_entry->data;
    iov[1].iov_len = u_entry->length;

    ssize_t total_to_send = u_entry->length + sizeof(compression_header_t);
    ssize_t total_sent = 0;

    struct iovec current_iov[2];
    current_iov[0] = iov[0];
    current_iov[1] = iov[1];

    while (total_sent < total_to_send) {
      ssize_t sent = writev(socket_fd, current_iov, 2);

      if (sent > 0) {
        total_sent += sent;
        __atomic_fetch_add(&ring->network_bytes_out, sent, __ATOMIC_RELAXED);

        if (total_sent >= total_to_send)
          break;

        if ((size_t)sent >= current_iov[0].iov_len) {
          size_t overflow = sent - current_iov[0].iov_len;
          current_iov[0].iov_len = 0;
          current_iov[1].iov_base =
              (uint8_t *)current_iov[1].iov_base + overflow;
          current_iov[1].iov_len -= overflow;
        } else {
          current_iov[0].iov_base = (uint8_t *)current_iov[0].iov_base + sent;
          current_iov[0].iov_len -= sent;
        }
      } else if (sent < 0 &&
                 (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        if (errno != EINTR) {
          __atomic_fetch_add(&ring->socket_blocked_count[thread_id], 1,
                             __ATOMIC_RELAXED);
        }
        __builtin_ia32_pause();
        continue;
      } else {
        perror("uncompressed send failed");
        break;
      }
    }

    // Reset ready flag for reuse
    __atomic_store_n(&u_entry->ready, 0, __ATOMIC_RELEASE);

    if (total_sent == total_to_send) {
      ring->consumer.bytes_sent += u_entry->length;
      __sync_fetch_and_add(counter, 1);
      __atomic_fetch_add(&ring->uncompressed_packets_sent, 1, __ATOMIC_RELAXED);
      __atomic_fetch_add(&ring->network_bytes_in, u_entry->length,
                         __ATOMIC_RELAXED);
      return true;
    } else {
      return false;
    }
  }

  // Socket invalid? Should not happen if checked before call.
  // Still reset ready.
  __atomic_store_n(&u_entry->ready, 0, __ATOMIC_RELEASE);
  return false;
}

// free(u_entry->data); // Safety: data is freed above.
// Wait, the previous block has free(u_entry->data) before return true.

// Actually, let's just make sure the function ends cleanly.
// The Atomic CAS version handles the head update.

/* NETWORK THREAD
    * if there is data in compressed send queue, send first --> then fallback
   to uncompressed queue

*/
void *network_sender_thread(void *arg) {
  thread_args_t *args = (thread_args_t *)arg;
  int thread_id = args->thread_id;
  int socket_fd = g_state->socket_fds[thread_id];
  ring_buffer_t *assigned_ring = args->ring;

  pin_thread_to_core(args->core_id);
  INFO_PRINT("Network sender thread %d started on core %d, using socket_fd %d "
             "for ring %p\n",
             thread_id, args->core_id, socket_fd, (void *)assigned_ring);

  uint64_t total_compressed = 0;
  uint64_t total_uncompressed = 0;
  uint64_t last_packets_sent = 0;
  time_t last_report_time = time(NULL);

  while (g_state->running) {
    bool worked = false;
    time_t now = time(NULL);

    if (now - last_report_time >= 5) {
      uint64_t current_packets = total_compressed + total_uncompressed;
      uint64_t packets_per_sec =
          (current_packets - last_packets_sent) / (now - last_report_time);

      DEBUG_PRINT("Network %d: %lu pkt/s (comp:%lu uncomp:%lu)\n", thread_id,
                  packets_per_sec, total_compressed, total_uncompressed);

      last_packets_sent = current_packets;
      last_report_time = now;
    }

    // Optimized: Only drain the ring assigned to this sender thread
    ring_buffer_t *ring = assigned_ring;
    if (ring && ring->running) {
      uint64_t sent_bytes = 0;
      int batch_count = 0;
      // Batch drain compressed
      while (batch_count < 64 &&
             try_send_compressed(ring, socket_fd, &sent_bytes, thread_id)) {
        total_compressed++;
        worked = true;
        batch_count++;
      }

      batch_count = 0;
      // Batch drain uncompressed
      while (batch_count < 64 &&
             try_send_uncompressed(ring, socket_fd, &sent_bytes, thread_id)) {
        total_uncompressed++;
        worked = true;
        batch_count++;
      }
    }

    if (!worked) {
      __builtin_ia32_pause();
    }
  }

  INFO_PRINT("Network sender thread %d exiting (comp:%lu uncomp:%lu)\n",
             thread_id, total_compressed, total_uncompressed);
  return NULL;
}

// periodically prints queue occupancies
void *monitor_thread(void *arg) {
  thread_args_t *args = (thread_args_t *)arg;
  pin_thread_to_core(args->core_id);
  INFO_PRINT("Monitor thread started on core %d\n", args->core_id);

  while (g_state->running) {
    for (int i = 0; i < NUM_QAT_INSTANCES; i++) {
      ring_buffer_t *r = g_state->rings[i];
      if (!r)
        continue;

      uint32_t r_occ = (r->producer.tail - r->consumer.head) & r->mask;
      uint32_t c_occ =
          (r->compressed_send_queue.tail - r->compressed_send_queue.head) &
          r->compressed_send_queue.mask;
      uint32_t u_occ =
          (r->uncompressed_send_queue.tail - r->uncompressed_send_queue.head) &
          r->uncompressed_send_queue.mask;

      INFO_PRINT(
          "Queue Occupancy (Ring %d): Ring=%u/%d, Comp=%u/%d, Uncomp=%u/%d\n",
          i, r_occ, RING_SIZE, c_occ, SEND_QUEUE_SIZE, u_occ, SEND_QUEUE_SIZE);
    }
    sleep(2);
  }

  INFO_PRINT("Monitor thread exiting\n");
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
  if (entry->pSrcBuffer)
    qaeMemFreeNUMA((void **)&entry->pSrcBuffer);
  if (entry->pDstBuffer)
    qaeMemFreeNUMA((void **)&entry->pDstBuffer);
  if (entry->pSrcBuffer && entry->pSrcBuffer->pPrivateMetaData)
    qaeMemFreeNUMA((void **)&entry->pSrcBuffer->pPrivateMetaData);
  if (entry->pDstBuffer && entry->pDstBuffer->pPrivateMetaData)
    qaeMemFreeNUMA((void **)&entry->pDstBuffer->pPrivateMetaData);
}

void drain_qat_operations(ring_buffer_t *ring) {
  int timeout = 1000;
  while (timeout > 0) {
    bool all_empty = true;
    for (int i = 0; i < RING_SIZE; i++) {
      if (ring->entries[i].status == JOB_SUBMITTED) {
        all_empty = false;
        icp_sal_DcPollInstance(ring->dcInstance, 0);
        break;
      }
    }
    if (all_empty)
      break;
    usleep(10000);
    timeout--;
  }
}

void ring_buffer_cleanup(ring_buffer_t *ring) {
  if (!ring)
    return;

  if (ring->dcInstance) {
    drain_qat_operations(ring);
    cpaDcStopInstance(ring->dcInstance);
  }

  if (ring->sessionHandle) {
    cpaDcRemoveSession(ring->dcInstance, ring->sessionHandle);
  }

  for (int i = 0; i < RING_SIZE; i++) {
    cleanup_qat_buffers(&ring->entries[i]);
  }

  icp_sal_userStop();

  if (ring->socket_blocked_count) {
    free((void *)ring->socket_blocked_count);
  }
  free(ring);
}

int setup_server_socket(int port, int num_connections, int *client_fds) {
  int sockfd;
  struct sockaddr_in server_addr;
  int opt = 1;

  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    perror("socket creation failed");
    return -1;
  }

  if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
                 sizeof(opt))) {
    perror("setsockopt failed");
    close(sockfd);
    return -1;
  }

  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(port);

  if (bind(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    perror("bind failed");
    close(sockfd);
    return -1;
  }

  if (listen(sockfd, num_connections) < 0) {
    perror("listen failed");
    close(sockfd);
    return -1;
  }

  INFO_PRINT("Server listening on port %d for %d connections...\n", port,
             num_connections);

  for (int i = 0; i < num_connections; i++) {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    client_fds[i] =
        accept(sockfd, (struct sockaddr *)&client_addr, &client_len);

    if (client_fds[i] < 0) {
      perror("accept failed");
      close(sockfd);
      return -1;
    }

    INFO_PRINT("Client %d connected from %s:%d\n", i,
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

    int sndbuf = 1024 * 1024 * 1024; // 1 GB
    int rcvbuf = 1024 * 1024 * 1024; // 1 GB

    setsockopt(client_fds[i], SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
    setsockopt(client_fds[i], SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    int flags = fcntl(client_fds[i], F_GETFL, 0);
    fcntl(client_fds[i], F_SETFL, flags | O_NONBLOCK);
  }

  close(sockfd);
  return 0;
}

// print qat_stats
//  void print_qat_stats(double elapsed_sec)
//  {
//      uint64_t total_compressions = 0;
//      uint64_t total_bytes_in = 0;
//      uint64_t total_bytes_out = 0;

//     // Aggregate stats from all QAT instances
//     for (int i = 0; i < NUM_QAT_INSTANCES; i++) {
//         ring_buffer_t *ring = g_state->rings[i];
//         total_compressions += ring->qat_stats.total_compressions;
//         total_bytes_in += ring->qat_stats.total_bytes_in;
//         total_bytes_out += ring->qat_stats.total_bytes_out;
//     }

//     if (total_compressions > 0) {
//         double net_in_gbps = (total_bytes_in * 8.0) / (elapsed_sec * 1e9);
//         double net_out_gbps = (total_bytes_out * 8.0) / (elapsed_sec *
//         1e9); double compression_ratio = (double)total_bytes_out /
//         (double)total_bytes_in;

//         INFO_PRINT("Aggregated QAT Stats (All Instances):\n");
//         INFO_PRINT("  Total Compressions: %lu\n", total_compressions);
//         INFO_PRINT("  TX QAT Bytes In:  %lu --> Throughput: %.2f Gbps\n",
//                    total_bytes_in, net_in_gbps);
//         INFO_PRINT("  TX QAT Bytes Out: %lu --> Throughput: %.2f Gbps\n",
//                    total_bytes_out, net_out_gbps);
//         INFO_PRINT("  Compression Ratio: %.2f%%\n", compression_ratio *
//         100);
//     } else {
//         INFO_PRINT("Aggregated QAT Stats: No compressions recorded\n");
//     }
// }

void print_qat_stats(double elapsed_sec) {
  for (int i = 0; i < NUM_QAT_INSTANCES; i++) {
    ring_buffer_t *ring = g_state->rings[i];

    if (ring->qat_stats.total_compressions > 0) {
      double net_in_gbps =
          (ring->qat_stats.total_bytes_in * 8.0) / (elapsed_sec * 1e9);
      double net_out_gbps =
          (ring->qat_stats.total_bytes_out * 8.0) / (elapsed_sec * 1e9);
      double compression_ratio = (double)ring->qat_stats.total_bytes_out /
                                 (double)ring->qat_stats.total_bytes_in;

      INFO_PRINT("  QAT Instance %d Stats:\n", i);
      INFO_PRINT("    Total Compressions: %lu\n",
                 ring->qat_stats.total_compressions);
      INFO_PRINT("    TX QAT Bytes In:  %lu --> Throughput: %.2f Gbps\n",
                 ring->qat_stats.total_bytes_in, net_in_gbps);
      INFO_PRINT("    TX QAT Bytes Out: %lu --> Throughput: %.2f Gbps\n",
                 ring->qat_stats.total_bytes_out, net_out_gbps);
      INFO_PRINT("    Compression Ratio: %.2f%%\n", compression_ratio * 100);
    } else {
      INFO_PRINT("  QAT Instance %d Stats: No compressions recorded\n", i);
    }
  }
}

// determine which benchmark to use based on command line arg
const char *map_file_number(const char *input) {
  static const char *base_path =
      "/home/npunati2/SquashBench/"; // TODO: change to where squash
                                     // benchmarks are located
  static char full_path[512];

  if (strlen(input) == 1 && input[0] >= '1' && input[0] <= '5') {
    int file_num = input[0] - '0';
    const char *filename = NULL;

    switch (file_num) {
    case 1:
      filename = "geo.protodata";
      break;
    case 2:
      filename = "nci";
      break;
    case 3:
      filename = "ptt5";
      break;
    case 4:
      filename = "sum";
      break;
    case 5:
      filename = "xml";
      break;
    default:
      return NULL;
    }

    snprintf(full_path, sizeof(full_path), "%s%s", base_path, filename);
    return full_path;
  }

  return input;
}

int main(int argc, char *argv[]) {
  int compression_level = 1;
  float compressed_fraction = 0.5;
  int port = 9999;
  int numa_node0_base = 0;
  int numa_node1_base = 32;
  int numa_node2_base = 64;
  int numa_node3_base = 96;
  int numa_bases[] = {numa_node0_base, numa_node1_base, numa_node2_base,
                      numa_node3_base};
  char *input_arg = NULL;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-l") == 0 && i + 1 < argc) {
      compression_level = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-d") == 0 || strcmp(argv[i], "--debug") == 0) {
      g_debug_enabled = true;
      INFO_PRINT("Debug mode enabled\n");
    } else if (strcmp(argv[i], "-f") == 0 && i + 1 < argc) {
      compressed_fraction = atof(argv[++i]);
      if (compressed_fraction < 0.0 || compressed_fraction > 1.0) {
        printf("Error: fraction must be 0.0-1.0\n");
        return 1;
      }
    } else if (strcmp(argv[i], "-n") == 0 && i + 1 < argc) {
      g_num_senders = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-i") == 0 && i + 1 < argc) {
      input_arg = argv[++i];
    }
  }

  if (!input_arg) {
    fprintf(stderr, "Error: Input file is required (use -i option)\n\n");
    return 1;
  }

  const char *input_file = map_file_number(input_arg);
  if (!input_file) {
    fprintf(stderr, "Error: Invalid file number or path: %s\n\n", input_arg);
    return 1;
  }

  INFO_PRINT("Initializing QAT compression pipeline...\n");
  INFO_PRINT("Configuration: input_file=%s, compression_level=%d, "
             "compressed_fraction=%.2f\n",
             input_file, compression_level, compressed_fraction);

  g_state = malloc(sizeof(global_state_t));
  if (!g_state) {
    fprintf(stderr, "Failed to allocate global state\n");
    return 1;
  }
  memset(g_state, 0, sizeof(global_state_t));
  g_state->running = true;
  g_state->socket_fds = malloc(g_num_senders * sizeof(int));

  // Initialize QAT once per process
  CpaStatus qat_status = icp_sal_userStartMultiProcess("SSL", CPA_FALSE);
  if (qat_status != CPA_STATUS_SUCCESS) {
    printf("Failed to start QAT process: %d\n", qat_status);
    return 1;
  }

  for (int i = 0; i < NUM_QAT_INSTANCES; i++) {
    g_state->rings[i] =
        ring_buffer_init(i, compression_level, compressed_fraction);
    if (!g_state->rings[i]) {
      printf("Failed to initialize ring buffer %d\n", i);
      return 1;
    }
  }

  if (setup_server_socket(port, g_num_senders, g_state->socket_fds) < 0) {
    printf("Warning: Failed to setup sockets, continuing without network\n");
    for (int i = 0; i < g_num_senders; i++)
      g_state->socket_fds[i] = -1;
  }

  pthread_t producer_tids[NUM_PRODUCERS];
  pthread_t consumer_tids[NUM_CONSUMERS];
  pthread_t *sender_tids = malloc(g_num_senders * sizeof(pthread_t));
  pthread_t monitor_tid;

  for (int i = 0; i < NUM_PRODUCERS; i++) {
    thread_args_t *args = malloc(sizeof(thread_args_t));
    int instance_id = i / PRODUCERS_PER_INSTANCE;

    args->ring = g_state->rings[instance_id];
    args->thread_id = i;
    args->instance_id = instance_id;
    args->core_id = numa_bases[instance_id % 4] + (i % PRODUCERS_PER_INSTANCE);
    args->file_path = input_file;

    if (pthread_create(&producer_tids[i], NULL, producer_thread, args) != 0) {
      printf("Failed to create producer thread %d\n", i);
      g_state->running = false;
      return 1;
    }
  }

  for (int i = 0; i < NUM_CONSUMERS; i++) {
    thread_args_t *args = malloc(sizeof(thread_args_t));
    args->ring = g_state->rings[i];
    args->thread_id = i;
    args->instance_id = i;
    args->core_id = numa_bases[i % 4] + PRODUCERS_PER_INSTANCE + i;
    args->file_path = NULL; // Not needed for consumers

    if (pthread_create(&consumer_tids[i], NULL, consumer_thread, args) != 0) {
      printf("Failed to create consumer thread %d\n", i);
      g_state->running = false;
      return 1;
    }
  }

  for (int i = 0; i < g_num_senders; i++) {
    thread_args_t *args = malloc(sizeof(thread_args_t));
    int ring_idx = i % NUM_QAT_INSTANCES;
    args->ring = g_state->rings[ring_idx];
    args->thread_id = i;
    args->core_id = numa_bases[ring_idx % 4] + PRODUCERS_PER_INSTANCE +
                    (NUM_CONSUMERS / NUM_QAT_INSTANCES) + i;
    args->file_path = NULL;

    if (pthread_create(&sender_tids[i], NULL, network_sender_thread, args) !=
        0) {
      printf("Failed to create sender thread %d\n", i);
      g_state->running = false;
      return 1;
    }
  }

  thread_args_t *monitor_args = malloc(sizeof(thread_args_t));
  monitor_args->ring = NULL;
  monitor_args->thread_id = 0;
  monitor_args->core_id = numa_node2_base + NUM_PRODUCERS +
                          (NUM_CONSUMERS / NUM_QAT_INSTANCES) + g_num_senders;

  if (pthread_create(&monitor_tid, NULL, monitor_thread, monitor_args) !=
      0) { // core 67
    printf("Failed to create monitor thread\n");
    g_state->running = false;
    return 1;
  }

  struct timespec start_ts, end_ts;
  clock_gettime(CLOCK_MONOTONIC, &start_ts);
  INFO_PRINT("Pipeline running with %d producers, %d consumers, %d senders\n",
             NUM_PRODUCERS, NUM_CONSUMERS, g_num_senders);
  INFO_PRINT("Threads distributed across 4 NUMA nodes (0, 1, 2, 3)\n");

  sleep(30);

  INFO_PRINT("Shutting down...\n");
  g_state->running = false;
  for (int i = 0; i < NUM_QAT_INSTANCES; i++) {
    g_state->rings[i]->running = false;
  }

  for (int i = 0; i < NUM_PRODUCERS; i++) {
    pthread_join(producer_tids[i], NULL);
  }
  for (int i = 0; i < NUM_CONSUMERS; i++) {
    pthread_join(consumer_tids[i], NULL);
  }
  for (int i = 0; i < g_num_senders; i++) {
    pthread_join(sender_tids[i], NULL);
  }
  pthread_join(monitor_tid, NULL);

  clock_gettime(CLOCK_MONOTONIC, &end_ts);

  double elapsed_sec = (end_ts.tv_sec - start_ts.tv_sec) +
                       (end_ts.tv_nsec - start_ts.tv_nsec) / 1e9;
  INFO_PRINT("Elapsed Seconds: %f\n", elapsed_sec);

  uint64_t total_app_bytes = 0;
  uint64_t total_net_in = 0;
  uint64_t total_net_out = 0;
  uint64_t total_compressed = 0;
  uint64_t total_uncompressed = 0;

  for (int i = 0; i < NUM_QAT_INSTANCES; i++) {
    ring_buffer_t *ring = g_state->rings[i];
    total_app_bytes += ring->producer.app_bytes_generated;
    total_net_in += ring->network_bytes_in;
    total_net_out += ring->network_bytes_out;
    total_compressed += ring->compressed_packets_sent;
    total_uncompressed += ring->uncompressed_packets_sent;

    INFO_PRINT("Instance %d Statistics:\n", i);
    INFO_PRINT("  App bytes: %lu\n", ring->producer.app_bytes_generated);
    INFO_PRINT("  Compressed packets: %lu\n", ring->compressed_packets_sent);
    INFO_PRINT("  Uncompressed packets: %lu\n",
               ring->uncompressed_packets_sent);
  }

  double app_gbps = (total_app_bytes * 8.0) / (elapsed_sec * 1e9);
  double net_in_gbps = (total_net_in * 8.0) / (elapsed_sec * 1e9);
  double net_out_gbps = (total_net_out * 8.0) / (elapsed_sec * 1e9);

  INFO_PRINT("TX Statistics:\n");
  INFO_PRINT("    TX App Bytes Generated: %lu --> Throughput(%.2f Gbps)\n",
             total_app_bytes, app_gbps);
  INFO_PRINT("    TX Network Bytes In: %lu --> Throughput(%.2f Gbps)\n",
             total_net_in, net_in_gbps);
  INFO_PRINT("    TX Network Bytes Out: %lu --> Throughput(%.2f Gbps)\n",
             total_net_out, net_out_gbps);
  print_qat_stats(elapsed_sec);
  INFO_PRINT("  Compressed packets: %lu\n", total_compressed);
  INFO_PRINT("  Uncompressed packets: %lu\n", total_uncompressed);
  print_ring_status_summary(g_state);

  // printf("  Jobs submitted: %lu\n", ring->producer.jobs_submitted);
  // printf("  Jobs sent: %lu\n", ring->consumer.jobs_sent);
  // printf("  Bytes sent: %lu\n", ring->consumer.bytes_sent);
  // printf("  Compressed packets: %lu\n", ring->compressed_packets_sent);
  // printf("  Uncompressed packets: %lu\n", ring->uncompressed_packets_sent);
  // printf("  Socket Blocked Count: %lu\n", ring->socket_blocked_count);
  // print_qat_stats(ring);

  for (int i = 0; i < g_num_senders; i++) {
    if (g_state->socket_fds[i] > 0) {
      close(g_state->socket_fds[i]);
    }
  }
  free(g_state->socket_fds);
  free(sender_tids);

  INFO_PRINT("Pipeline shutdown complete\n");
  exit(0);
}
