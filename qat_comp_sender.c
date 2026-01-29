#define _GNU_SOURCE
#include <sched.h>
#include <qat/cpa.h>              
#include <qat/cpa_dc.h>           
#include <qat/icp_sal_user.h>     
#include <qat/icp_sal_poll.h>     
#include <qat/qae_mem.h>         

#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h> 
#include <sys/socket.h>
#include <sys/uio.h>
#include <errno.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>


#define TEST_DATA_SIZE 64 * 1024    
#define RING_SIZE 10 * 1024
#define CACHE_LINE_SIZE 64
#define MAX_BUFFER_SIZE 128 * 1024  
#define SAMPLE_SIZE 256
#define SEND_QUEUE_SIZE 128 * 1024   //128
#define NUM_PRODUCERS 2
#define NUM_CONSUMERS 1
#define NUM_SENDERS 1
#define MAGIC_COMPRESSED   0x51415443
#define MAGIC_UNCOMPRESSED 0x51415452

static bool g_debug_enabled = false;

#define DEBUG_PRINT(fmt, ...) \
    do { if (g_debug_enabled) fprintf(stderr, "[DEBUG] " fmt "", ##__VA_ARGS__); } while (0)

#define INFO_PRINT(fmt, ...) \
    fprintf(stderr, "[INFO] " fmt "", ##__VA_ARGS__)

typedef enum {
    JOB_EMPTY = 0,
    JOB_SUBMITTED = 1,
    JOB_COMPLETED = 2,
    JOB_ERROR = 3
} job_status_t;

//compression benchmark file reader
typedef struct {
    char *file_path;
    FILE *current_file;
    uint64_t current_file_offset;
    uint64_t current_file_size;
    uint64_t file_reopen_count;
    pthread_mutex_t file_lock;
} file_reader_t;

typedef struct {
    uint32_t magic; //helps receiver recognize compressed packets
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
    
    //scatterlist (required for submission) DMA
    //contains multiple CpaFlatBuffer entries
    CpaBufferList *pSrcBuffer;
    CpaBufferList *pDstBuffer;

    //represents a contiguous memory region
    CpaFlatBuffer *pFlatSrcBuffer;
    CpaFlatBuffer *pFlatDstBuffer;
    
    //data buffer pointer
    Cpa8U *pSrcData;
    Cpa8U *pDstData;
    
    //metadata necessary for correctnesns
    Cpa32U srcDataSize;
    Cpa32U dstDataSize;
    Cpa32U producedSize;  
    
    CpaDcRqResults dcResults;
    
    compression_header_t header;
    
    uint64_t submit_time;
    uint64_t complete_time;
} ring_entry_t;

// compressed send queue entry is a ring entry
typedef struct {
    ring_entry_t *entry; 
} compressed_send_entry_t;

//compressed send queue
typedef struct {
    compressed_send_entry_t entries[SEND_QUEUE_SIZE];
    volatile uint32_t head;
    volatile uint32_t tail;
    uint32_t mask;
    char pad[CACHE_LINE_SIZE];
} __attribute__((aligned(CACHE_LINE_SIZE))) compressed_send_queue_t;

//uncompressed send entry just has pointer to data
typedef struct {
    compression_header_t header;
    uint8_t *data;        
    size_t length;
    uint64_t sequence_number;
    volatile int ready;
} uncompressed_send_entry_t;

//uncompressed send queue
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
typedef struct {
    ring_entry_t entries[RING_SIZE];
    int compression_level;
    float compressed_fraction; 
    file_reader_t file_reader;
    
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
    int socket_fd;
    volatile bool running;

    compressed_send_queue_t compressed_send_queue;
    uncompressed_send_queue_t uncompressed_send_queue;
    
    volatile uint64_t sequence_counter;
    volatile uint64_t uncompressed_packets_sent;
    volatile uint64_t compressed_packets_sent; 
    volatile uint64_t socket_blocked_count;
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

} ring_buffer_t;

//args for network, consumer, producer threads
typedef struct {
    ring_buffer_t *ring;
    int thread_id;
    int core_id;
} thread_args_t;

static ring_buffer_t *g_ring = NULL;

//ensure each thread is pinned to different cores (on same NUMA node)
int pin_thread_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    
    pthread_t current_thread = pthread_self();
    int result = pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
    
    if (result == 0) {
        INFO_PRINT("Thread pinned to core %d\n", core_id);
    } else {
        fprintf(stderr, "Failed to pin thread to core %d: %s\n", core_id, strerror(result));
    }
    
    return result;
}

//functions for handling compression benchmark files
//---------------------------------------------------
int load_single_file(file_reader_t *reader, const char *file_path) {
    struct stat st;
    if (stat(file_path, &st) != 0) {
        fprintf(stderr, "Failed to stat file: %s\n", file_path);
        return -1;
    }
    
    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "Not a regular file: %s\n", file_path);
        return -1;
    }
    
    reader->file_path = strdup(file_path);
    if (!reader->file_path) {
        fprintf(stderr, "Failed to allocate memory for file path\n");
        return -1;
    }
    
    reader->current_file = NULL;
    reader->current_file_offset = 0;
    reader->current_file_size = st.st_size;
    reader->file_reopen_count = 0;
    pthread_mutex_init(&reader->file_lock, NULL);
    
    INFO_PRINT("Loaded benchmark file: %s (size: %lu bytes)\n", file_path, reader->current_file_size);
    return 0;
}

int open_file(file_reader_t *reader) {
    if (reader->current_file) {
        fclose(reader->current_file);
        reader->current_file = NULL;
    }
    
    if (!reader->file_path) {
        return -1;
    }
    
    reader->current_file = fopen(reader->file_path, "rb");
    
    if (!reader->current_file) {
        fprintf(stderr, "Failed to open file: %s\n", reader->file_path);
        return -1;
    }
    
    reader->current_file_offset = 0;
    reader->file_reopen_count++;
    
    //DEBUG_PRINT("Reopened file: %s (reopen count: %lu)\n", reader->file_path, reader->file_reopen_count);
    return 0;
}

ssize_t read_chunk_from_files(file_reader_t *reader, uint8_t *buffer, size_t chunk_size) {
    pthread_mutex_lock(&reader->file_lock);
    
    if (!reader->current_file) {
        if (open_file(reader) != 0) {
            pthread_mutex_unlock(&reader->file_lock);
            return -1;
        }
    }
    
    size_t total_read = 0;
    
    while (total_read < chunk_size) {
        size_t to_read = chunk_size - total_read;
        size_t bytes_read = fread(buffer + total_read, 1, to_read, reader->current_file);
        
        if (bytes_read > 0) {
            total_read += bytes_read;
            reader->current_file_offset += bytes_read;
        }
        
        // Only reopen if we hit EOF or error (not just a short read)
        if (bytes_read == 0) {
            if (feof(reader->current_file)) {
                if (open_file(reader) != 0) {
                    pthread_mutex_unlock(&reader->file_lock);
                    return total_read > 0 ? total_read : -1;
                }
            } else if (ferror(reader->current_file)) {
                // Error occurred
                pthread_mutex_unlock(&reader->file_lock);
                return total_read > 0 ? total_read : -1;
            }
        }
        
        if (total_read >= chunk_size) {
            break;
        }
    }
    
    pthread_mutex_unlock(&reader->file_lock);
    return total_read;
}

void cleanup_file_reader(file_reader_t *reader) {
    if (reader->current_file) {
        fclose(reader->current_file);
        reader->current_file = NULL;
    }
    
    if (reader->file_path) {
        free(reader->file_path);
        reader->file_path = NULL;
    }
    
    pthread_mutex_destroy(&reader->file_lock);
    
    INFO_PRINT("File was reopened %lu times during experiment\n", reader->file_reopen_count);
}

// callback gets fired on polling when QAT jobs complete
void qat_dc_callback(void *pCallbackTag, CpaStatus status)
{
    static uint64_t callback_count = 0;
    callback_count++;
    
    ring_entry_t *entry = (ring_entry_t *)pCallbackTag;
    
    if (status == CPA_STATUS_SUCCESS) {
        uint64_t compression_time = clock() - entry->submit_time;
        uint64_t time_us = (compression_time * 1000000) / CLOCKS_PER_SEC;
        
        __sync_fetch_and_add(&g_ring->qat_stats.total_compressions, 1);
        __sync_fetch_and_add(&g_ring->qat_stats.total_bytes_in, entry->srcDataSize);
        __sync_fetch_and_add(&g_ring->qat_stats.total_bytes_out, entry->producedSize);
        __sync_fetch_and_add(&g_ring->qat_stats.total_compression_time_us, time_us);
        clock_gettime(CLOCK_MONOTONIC, &g_ring->qat_stats.last_callback_time);


        entry->producedSize = entry->dcResults.produced;
        entry->header.compressed_size = entry->producedSize;
        entry->header.checksum = 0;
        
        __sync_synchronize();
        entry->status = JOB_COMPLETED;
        __sync_synchronize();
        
        if (callback_count % 100000 == 0) {
            DEBUG_PRINT("Callback: %lu successful compressions\n", callback_count);
        }
    } else {
        printf("QAT compression failed: %d (callback #%lu)\n", status, callback_count);
        __sync_synchronize();
        entry->status = JOB_ERROR;
    }
    
    entry->complete_time = clock();
}

/* Initialize QAT
    * Find instance on NUMA Node 2
    * Set up Address Translation for DMA
    * Set up session data (algorithm, level, etc)
*/
int qat_init(ring_buffer_t *ring)
{
    CpaStatus status;
    Cpa16U numInstances = 0;
    CpaInstanceHandle *instances = NULL;
    Cpa32U sessionSize = 0;
    Cpa32U contextSize = 0;
    
    status = icp_sal_userStartMultiProcess("SSL", CPA_FALSE);
    if (status != CPA_STATUS_SUCCESS) {
        printf("Failed to start QAT process: %d\n", status);
        return -1;
    }
    
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

    int target_numa_node = 2;  //nic location
    int selected_instance = -1;

    for (int i = 0; i < numInstances; i++) {
        CpaInstanceInfo2 info;
        status = cpaDcInstanceGetInfo2(instances[i], &info);
        
        if (status == CPA_STATUS_SUCCESS) {
            DEBUG_PRINT("Instance %d: nodeAffinity = %d\n", i, info.nodeAffinity);
            
            if (info.nodeAffinity == target_numa_node && selected_instance == -1) {
                selected_instance = i;
            }
        }
    }

    if (selected_instance == -1) {
        INFO_PRINT("Warning: No QAT on NUMA %d, using instance 0\n", target_numa_node);
        selected_instance = 0;
    } else {
        INFO_PRINT("Selected QAT instance %d on NUMA node %d\n", 
               selected_instance, target_numa_node);
    }
    
    ring->dcInstance = instances[selected_instance];
    free(instances);


    status = cpaDcSetAddressTranslation(ring->dcInstance, qaeVirtToPhysNUMA);
    if (status != CPA_STATUS_SUCCESS) {
        printf("Failed to set address translation: %d\n", status);
        return -1;
    }
    
    Cpa16U numBuffers = 2048; 
    status = cpaDcStartInstance(ring->dcInstance, numBuffers, NULL);
    if (status != CPA_STATUS_SUCCESS) {
        printf("Failed to start DC instance: %d\n", status);
        return -1;
    }

    CpaDcCompLvl qat_level;
    switch(ring->compression_level) {
        case 1: qat_level = CPA_DC_L1; break;
        case 2: qat_level = CPA_DC_L2; break;
        case 3: qat_level = CPA_DC_L3; break;
        case 4: qat_level = CPA_DC_L4; break;
        case 5: qat_level = CPA_DC_L5; break;
        case 6: qat_level = CPA_DC_L6; break;
        case 7: qat_level = CPA_DC_L7; break;
        case 8: qat_level = CPA_DC_L8; break;
        case 9: qat_level = CPA_DC_L9; break;
        default: qat_level = CPA_DC_L1;
    } 

    
    ring->sessionSetupData.compLevel = qat_level;            
    ring->sessionSetupData.compType = CPA_DC_DEFLATE;          // DEFLATE
    ring->sessionSetupData.huffType = CPA_DC_HT_STATIC;        //static huffman
    ring->sessionSetupData.autoSelectBestHuffmanTree = CPA_FALSE;
    ring->sessionSetupData.sessDirection = CPA_DC_DIR_COMPRESS;
    ring->sessionSetupData.sessState = CPA_DC_STATELESS;       
    ring->sessionSetupData.checksum = CPA_DC_CRC32;
    ring->sessionSetupData.windowSize = 15;       
    
    // ring->sessionSetupData.compLevel = qat_level;              
    // ring->sessionSetupData.compType = CPA_DC_LZ4;              // config for LZ4
    // ring->sessionSetupData.sessDirection = CPA_DC_DIR_COMPRESS;
    // ring->sessionSetupData.sessState = CPA_DC_STATELESS;       
    // ring->sessionSetupData.checksum = CPA_DC_NONE;   
    
    status = cpaDcGetSessionSize(ring->dcInstance, 
                                 &ring->sessionSetupData,
                                 &sessionSize, 
                                 &contextSize);
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
    status = cpaDcInitSession(ring->dcInstance,
                             ring->sessionHandle,
                             &ring->sessionSetupData,
                             pSessionMemory,
                             qat_dc_callback); 
    
    if (status != CPA_STATUS_SUCCESS) {
        printf("Failed to init session: %d\n", status);
        qaeMemFreeNUMA(&pSessionMemory);
        return -1;
    }
    
    INFO_PRINT("QAT initialization successful\n");
    return 0;
}

// Function to allocate buffers for each ring entry
int allocate_qat_buffers(ring_entry_t *entry, CpaInstanceHandle dcInstance)
{
    Cpa32U metaSize = 0;
    CpaStatus status;
    
    status = cpaDcBufferListGetMetaSize(dcInstance, 1, &metaSize);
    if (status != CPA_STATUS_SUCCESS) {
        printf("Failed to get metadata size\n");
        return -1;
    }

    // Allocate source buffer list structure
    entry->pSrcBuffer = qaeMemAllocNUMA(sizeof(CpaBufferList), 0, 64);
    if (!entry->pSrcBuffer) return -1;

    if (metaSize > 0) {
        entry->pSrcBuffer->pPrivateMetaData = qaeMemAllocNUMA(metaSize, 0, 64);
        if (!entry->pSrcBuffer->pPrivateMetaData) return -1;
    } else {
        entry->pSrcBuffer->pPrivateMetaData = NULL;
    }
    
    entry->pFlatSrcBuffer = qaeMemAllocNUMA(sizeof(CpaFlatBuffer), 0, 64);
    if (!entry->pFlatSrcBuffer) return -1;
    
    entry->pSrcData = qaeMemAllocNUMA(MAX_BUFFER_SIZE, 0, 64);
    if (!entry->pSrcData) return -1;
    
    entry->pSrcBuffer->pBuffers = entry->pFlatSrcBuffer;
    entry->pSrcBuffer->numBuffers = 1;
    
    entry->pFlatSrcBuffer->pData = entry->pSrcData;
    entry->pFlatSrcBuffer->dataLenInBytes = 0; 
    
    entry->pDstBuffer = qaeMemAllocNUMA(sizeof(CpaBufferList), 0, 64);
    if (!entry->pDstBuffer) return -1;
    
    entry->pFlatDstBuffer = qaeMemAllocNUMA(sizeof(CpaFlatBuffer), 0, 64);
    if (!entry->pFlatDstBuffer) return -1;

    if (metaSize > 0) {
        entry->pDstBuffer->pPrivateMetaData = qaeMemAllocNUMA(metaSize, 0, 64);
        if (!entry->pDstBuffer->pPrivateMetaData) return -1;
    } else {
        entry->pDstBuffer->pPrivateMetaData = NULL;
    }
    
    entry->pDstData = qaeMemAllocNUMA(MAX_BUFFER_SIZE * 2, 0, 64);
    if (!entry->pDstData) return -1;
    
    entry->pDstBuffer->pBuffers = entry->pFlatDstBuffer;
    entry->pDstBuffer->numBuffers = 1;
    
    entry->pFlatDstBuffer->pData = entry->pDstData;
    entry->pFlatDstBuffer->dataLenInBytes = MAX_BUFFER_SIZE * 2;
    
    return 0;
}

//Set up Ring Buffer - file reader (load benchmark file), send queues, ring buffer entries, initialize qat 
ring_buffer_t* ring_buffer_init(int compression_level, float compressed_fraction, const char *input_file)
{
    ring_buffer_t *ring = aligned_alloc(CACHE_LINE_SIZE, sizeof(ring_buffer_t));
    if (!ring) return NULL;
    
    memset(ring, 0, sizeof(ring_buffer_t));
    ring->compression_level = compression_level;
    ring->compressed_fraction = compressed_fraction;

    ring->compressed_send_queue.mask = SEND_QUEUE_SIZE - 1;
    ring->uncompressed_send_queue.mask = SEND_QUEUE_SIZE - 1; 

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
    
    if (qat_init(ring) != 0) {
        free(ring);
        return NULL;
    }

    if (load_single_file(&ring->file_reader, input_file) != 0) {
        fprintf(stderr, "Failed to load input file: %s\n", input_file);
        free(ring);
        return NULL;
    }
    
    for (int i = 0; i < RING_SIZE; i++) {
        ring->entries[i].status = JOB_EMPTY;
        if (allocate_qat_buffers(&ring->entries[i], ring->dcInstance) != 0) {
            printf("Failed to allocate buffers for entry %d\n", i);
            free(ring);
            return NULL;
        }
    }
    
    ring->mask = RING_SIZE - 1;
    ring->running = true;
    g_ring = ring;  
    
    return ring;
}

/* PRODUCER THREAD
    * Depending on user input compression fraction, reads file data and submits compression jobs (or leaves data uncompressed)
        * TODO: change this to choose compressed path except when ring buffer full OR QAT backpressure
    * 64 KB chunks per compression job
*/
void* producer_thread(void *arg)
{
    thread_args_t* args = (thread_args_t*) arg;
    ring_buffer_t *ring = args->ring;
    int thread_id = args->thread_id;
    pin_thread_to_core(args->core_id);
    INFO_PRINT("Producer thread %d started on core %d\n", thread_id, args->core_id);


    CpaStatus status;
    uint64_t ring_full_count = 0;
    uint64_t queue_full_count = 0;
    uint64_t qat_backpressure_count = 0;
    uint64_t bytes_generated = 0;
    uint64_t packets_generated = 0;
    time_t last_report = time(NULL);
  
    const size_t CHUNK_SIZE = 64 * 1024;
    //char temp_buffer[CHUNK_SIZE];
  
    while (ring->running) {
        //float rand_val = (float)rand() / RAND_MAX;
        //bool use_compressed = (rand_val < ring->compressed_fraction);

        uint8_t temp_buffer[CHUNK_SIZE];
        ssize_t bytes_read = read_chunk_from_files(&ring->file_reader, temp_buffer, CHUNK_SIZE);
        __sync_fetch_and_add(&ring->producer.app_bytes_generated, bytes_read);

        if (bytes_read <= 0) continue;

        uint32_t current_tail, next_tail;
        ring_entry_t *entry;
        bool ring_claimed = false;
        current_tail = __atomic_load_n(&ring->producer.tail, __ATOMIC_ACQUIRE);
        next_tail = (current_tail + 1) & ring->mask;
        entry = &ring->entries[current_tail];

        if (next_tail != __atomic_load_n(&ring->consumer.head, __ATOMIC_ACQUIRE) &&
            __atomic_load_n(&entry->status, __ATOMIC_ACQUIRE) == JOB_EMPTY) {
            
            if (__atomic_compare_exchange_n(&ring->producer.tail, &current_tail, next_tail,
                                           false, __ATOMIC_RELEASE, __ATOMIC_ACQUIRE)) {
                ring_claimed = true;
            }
        }

        if (!ring_claimed) {
            ring_full_count++;
            // Fall through to uncompressed path
        } else {
            // Step 3: Prepare and submit to QAT
            memcpy(entry->pSrcData, temp_buffer, bytes_read);
            entry->srcDataSize = bytes_read;
            entry->pFlatSrcBuffer->dataLenInBytes = bytes_read;
            entry->pFlatDstBuffer->dataLenInBytes = MAX_BUFFER_SIZE * 2;
            
            entry->sequence_number = __sync_fetch_and_add(&ring->sequence_counter, 1);
            entry->header.magic = MAGIC_COMPRESSED;
            entry->header.sequence_number = entry->sequence_number;
            entry->header.uncompressed_size = bytes_read;
            entry->header.algorithm = 0;
            entry->header.compression_level = ring->compression_level;
            
            CpaDcOpData opData = {0};
            opData.flushFlag = CPA_DC_FLUSH_FINAL;
            opData.compressAndVerify = CPA_TRUE;
            entry->submit_time = clock();

            CpaStatus status = cpaDcCompressData2(ring->dcInstance, ring->sessionHandle,
                                                  entry->pSrcBuffer, entry->pDstBuffer,
                                                  &opData, &entry->dcResults, entry);
            
            if (status == CPA_STATUS_SUCCESS) {
                if (!ring->qat_stats.timing_started) {
                    clock_gettime(CLOCK_MONOTONIC, &ring->qat_stats.first_submit_time);
                    __sync_synchronize();
                    ring->qat_stats.timing_started = true;
                }
                __atomic_store_n(&entry->status, JOB_SUBMITTED, __ATOMIC_RELEASE);
                __sync_fetch_and_add(&ring->producer.jobs_submitted, 1);
                bytes_generated += bytes_read;
                packets_generated++;
                continue;
                
            } else {
                //qat error or backpressure -> error state allows consumer to skip it
                qat_backpressure_count++;
                __atomic_store_n(&entry->status, JOB_ERROR, __ATOMIC_RELEASE);  
            }
        }

        uncompressed_send_queue_t *queue = &ring->uncompressed_send_queue;
        uint32_t u_current_tail, u_next_tail;
        
        u_current_tail = __atomic_load_n(&queue->tail, __ATOMIC_ACQUIRE);
        u_next_tail = (u_current_tail + 1) & queue->mask;
        
        if (u_next_tail != __atomic_load_n(&queue->head, __ATOMIC_ACQUIRE)) {
            if (__atomic_compare_exchange_n(&queue->tail, &u_current_tail, u_next_tail,
                                           false, __ATOMIC_RELEASE, __ATOMIC_ACQUIRE)) {
                
                uncompressed_send_entry_t *u_entry = &queue->entries[u_current_tail];
                
                // Use data from temp_buffer (already read once)
                memcpy(u_entry->data, temp_buffer, bytes_read);
                u_entry->length = bytes_read;
                u_entry->sequence_number = __sync_fetch_and_add(&ring->sequence_counter, 1);
                u_entry->header.magic = MAGIC_UNCOMPRESSED;
                u_entry->header.sequence_number = u_entry->sequence_number;
                u_entry->header.uncompressed_size = bytes_read;
                u_entry->header.compressed_size = bytes_read;
                u_entry->header.algorithm = 99;
                __atomic_store_n(&u_entry->ready, 1, __ATOMIC_RELEASE);
                
                __sync_fetch_and_add(&ring->producer.jobs_submitted, 1);
                bytes_generated += bytes_read;
                packets_generated++;
            }
        } else {
            queue_full_count++;
        }

        time_t now = time(NULL);
        if (now - last_report >= 5) {
            double elapsed = (double)(now - last_report);
            double gbps = (bytes_generated * 8.0 / elapsed) / 1000000000.0;
            
            INFO_PRINT("Producer %d: %.3f Gbps | RingFull: %lu | QATBusy: %lu | QueueFull: %lu\n", thread_id, gbps, ring_full_count, qat_backpressure_count, queue_full_count);
          
            bytes_generated = 0;
            packets_generated = 0;
            last_report = now;
        }
    }

    INFO_PRINT("Producer thread exiting (submitted: %lu)\n", ring->producer.jobs_submitted);
    return NULL;
}

/* CONSUMER THREAD
    * Polls QAT so callbacks fire
    * Polls on ring buffer head for status to be COMPLETED
    * Pushes results into the compressed send queue
*/
void* consumer_thread(void *arg)
{
    thread_args_t* args = (thread_args_t*)arg;
    ring_buffer_t *ring = args->ring;
    int thread_id = args->thread_id;
    pin_thread_to_core(args->core_id);
    INFO_PRINT("Consumer thread %d started on core %d\n", thread_id, args->core_id);

    uint64_t send_count = 0;
    uint64_t stuck_count = 0;
    uint64_t compressed_full = 0;
    uint64_t poll_count = 0;
        
    while (ring->running) {
        poll_count++;
        //Poll QAT instance
        CpaStatus poll_status = icp_sal_DcPollInstance(ring->dcInstance, 0);
        if (poll_status != CPA_STATUS_SUCCESS && poll_status != CPA_STATUS_RETRY) {
            if (stuck_count % 10000000 == 0) {
                INFO_PRINT("Consumer: Poll failed with status %d\n", poll_status);
            }
        }
        if(poll_count % 100000000 == 0) {
            DEBUG_PRINT("Consumer: Polled %lu times...\n", poll_count);
        }
        
        uint32_t current_head = ring->consumer.head;
        ring_entry_t *entry = &ring->entries[current_head];
        job_status_t current_status = __atomic_load_n(&entry->status, __ATOMIC_ACQUIRE);
        
        if (current_status == JOB_COMPLETED) {
            //current head is completed
            stuck_count = 0;
            compressed_send_queue_t *queue = &ring->compressed_send_queue;
            uint32_t next_tail = (queue->tail + 1) & queue->mask;


            //compressed send queue is full 
            if (next_tail == queue->head) {
                static uint64_t discard_count = 0;
                discard_count++;

                if(compressed_full % 1000000 == 0) {
                    DEBUG_PRINT("Consumer: Compressed Queue Full, We are Spinning...\n");
                }
                compressed_full++;

                if (discard_count % 1000000 == 0) {
                    DEBUG_PRINT("Consumer: Discarded %lu compressed entries\n",
                                discard_count);
                }

                //after a while change the status to empty and move to next compressed send queue entry
                __atomic_store_n(&entry->status, JOB_EMPTY, __ATOMIC_RELEASE);
                ring->consumer.head = (current_head + 1) & ring->mask;

                continue;
            }
            
            queue->entries[queue->tail].entry = entry;
            __sync_synchronize();
            queue->tail = next_tail;
            
            //mark job as empty after its complete and advance head
            //__atomic_store_n(&entry->status, JOB_EMPTY, __ATOMIC_RELEASE);
            ring->consumer.jobs_sent++;
            ring->consumer.head = (current_head + 1) & ring->mask;
            
            send_count++;
            if (send_count % 10000 == 0) {
                DEBUG_PRINT("Consumer: Sent %lu entries to network thread\n", send_count);
            }
            
        } else if (current_status == JOB_ERROR) {
            //job is error state, change it emoty and advance head
            __atomic_store_n(&entry->status, JOB_EMPTY, __ATOMIC_RELEASE);
            ring->consumer.head = (current_head + 1) & ring->mask; 
            stuck_count = 0;
            //INFO_PRINT("Consumer: JOB %lu STUCK IN ERROR\n", entry->sequence_number);
            // INFO_PRINT("  Entry details: seq=%lu, srcSize=%u, dstSize=%u\n", 
            //         entry->sequence_number, entry->srcDataSize, entry->dstDataSize);
        } else if (current_status == JOB_SUBMITTED) {
            //job is stuck in submitted..
            stuck_count++;
            if(stuck_count % 1000000 == 0) {
                // INFO_PRINT("Consumer: JOB STUCK IN SUBMITTED %lu times\n", stuck_count);
                // INFO_PRINT("  Forcing to ERROR state and skipping...\n");
                // entry->status = JOB_EMPTY;
                // ring->consumer.head = (current_head + 1) & ring->mask;
                // stuck_count = 0;
                INFO_PRINT("Consumer: JOB STUCK IN SUBMITTED %lu times\n", stuck_count);
                INFO_PRINT("  Entry details: seq=%lu, srcSize=%u, dstSize=%u\n", 
                        entry->sequence_number, entry->srcDataSize, entry->dstDataSize);
                // entry->status = JOB_EMPTY;
                // ring->consumer.head = (current_head + 1) & ring->mask;
                // stuck_count = 0;

            } 
        }
    }
    
    INFO_PRINT("Consumer thread exiting\n");
    return NULL;
}

/* NETWORK THREAD
    * if there is data in compressed send queue, send first --> then fallback to uncompressed queue

*/
void* network_sender_thread(void *arg)
{
    thread_args_t* args = (thread_args_t*) arg;
    ring_buffer_t *ring = args->ring;
    pin_thread_to_core(args->core_id);
    INFO_PRINT("Network sender thread started on core %d\n", args->core_id);

    uint64_t last_packets_sent = 0;
    time_t last_report_time = time(NULL);
    uint64_t sent_entries = 0;
    uint64_t uncomp_sent_entries = 0;
    uint64_t no_data_counter = 0;
      
    while (ring->running) {
        bool sent_something = false;
        time_t now = time(NULL);
        if (now - last_report_time >= 5) { 
            uint64_t current_packets = ring->compressed_packets_sent + ring->uncompressed_packets_sent;
            uint64_t packets_per_sec = (current_packets - last_packets_sent) / (now - last_report_time);
          
            //periodic debugging output
            DEBUG_PRINT("Network: %lu packets/sec (comp:%lu uncomp:%lu queued_comp:%u queued_uncomp:%u)\n",
                   packets_per_sec,
                   ring->compressed_packets_sent,
                   ring->uncompressed_packets_sent,
                   (ring->compressed_send_queue.tail - ring->compressed_send_queue.head) & ring->compressed_send_queue.mask,
                   (ring->uncompressed_send_queue.tail - ring->uncompressed_send_queue.head) & ring->uncompressed_send_queue.mask);
          
            last_packets_sent = current_packets;
            last_report_time = now;
        }
      
        // Process COMPRESSED queue
        compressed_send_queue_t *comp_queue = &ring->compressed_send_queue;
        while (comp_queue->head != comp_queue->tail) {
            compressed_send_entry_t *send_msg = &comp_queue->entries[comp_queue->head];
            ring_entry_t *entry = send_msg->entry;
          
            if (ring->socket_fd > 0) {
                struct iovec iov[2];
                iov[0].iov_base = &entry->header;
                iov[0].iov_len = sizeof(compression_header_t);
                iov[1].iov_base = entry->pDstData;
                iov[1].iov_len = entry->producedSize;
                if(entry->header.magic != MAGIC_COMPRESSED) {
                    INFO_PRINT("COMPRESSED PACKET WITH WRONG MAGIC NUMBER!: 0x%08X\n", &entry->header.magic);
                }


                ssize_t sent = writev(ring->socket_fd, iov, 2);
              
                if (sent == sizeof(compression_header_t) + entry->producedSize) {
                    ring->consumer.bytes_sent += entry->producedSize;
                    __sync_fetch_and_add(&ring->compressed_packets_sent, 1);
                    __sync_fetch_and_add(&ring->network_bytes_out, sent);
                    __sync_fetch_and_add(&ring->network_bytes_in, sizeof(compression_header_t) + entry->producedSize);
                    __atomic_store_n(&entry->status, JOB_EMPTY, __ATOMIC_RELEASE);
                    sent_entries++;
                    if(sent_entries % 10000 == 0) {
                        DEBUG_PRINT("Network Send Thread: Sent %lu compressed entries\n", sent_entries);
                    }
                    sent_something = true;
                    comp_queue->head = (comp_queue->head + 1) & comp_queue->mask;
                } else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    __sync_fetch_and_add(&ring->socket_blocked_count, 1);
                    continue;
                } else if (sent > 0) {
                    __sync_fetch_and_add(&ring->network_bytes_out, sent);
                    __sync_fetch_and_add(&ring->network_bytes_in, sizeof(compression_header_t) + entry->producedSize);
                    //INFO_PRINT("PARTIAL SEND REACHED\n");

                } else {
                    perror("compressed send failed");
                    entry->status = JOB_EMPTY;
                    comp_queue->head = (comp_queue->head + 1) & comp_queue->mask;
                }
            }
        }
      
        // Process UNCOMPRESSED queue
        if(!sent_something) {
            uncompressed_send_queue_t *uncomp_queue = &ring->uncompressed_send_queue;
            while (uncomp_queue->head != uncomp_queue->tail) {
                uncompressed_send_entry_t *u_entry = &uncomp_queue->entries[uncomp_queue->head];

                if (__atomic_load_n(&u_entry->ready, __ATOMIC_ACQUIRE) == 0) {
                    break; 
                }
                  
                if (ring->socket_fd > 0) {
                    struct iovec iov[2];
                    iov[0].iov_base = &u_entry->header;
                    iov[0].iov_len = sizeof(compression_header_t);
                    iov[1].iov_base = u_entry->data;
                    iov[1].iov_len = u_entry->length;

                    if(u_entry->header.magic != MAGIC_UNCOMPRESSED) {
                        INFO_PRINT("UNCOMPRESSED PACKET WITH WRONG MAGIC NUMBER!: 0x%08X\n", &u_entry->header.magic);
                    }

                    ssize_t sent = writev(ring->socket_fd, iov, 2);
                  
                    if (sent == u_entry->length + sizeof(compression_header_t)) {
                        ring->consumer.bytes_sent += u_entry->length;
                        __sync_fetch_and_add(&ring->uncompressed_packets_sent, 1);
                        __sync_fetch_and_add(&ring->network_bytes_in, u_entry->length);
                        __sync_fetch_and_add(&ring->network_bytes_out, sent);
                        sent_something = true;
                        uncomp_sent_entries++;
                        if(uncomp_sent_entries % 10000 == 0) {
                            DEBUG_PRINT("Network Send Thread: Sent %lu uncompressed entries\n", sent_entries);
                        }
                        uncomp_queue->head = (uncomp_queue->head + 1) & uncomp_queue->mask;
                    } else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                        // fprintf(stderr, "DEBUG: Got EAGAIN/EWOULDBLOCK (errno=%d)\n", errno);
                        // __sync_fetch_and_add(&ring->socket_blocked_count, 1);
                        // usleep(10);
                        continue;
                    } else if (sent < 0) {
                        fprintf(stderr, "Send error (errno=%d): %s\n", errno, strerror(errno));
                        uncomp_queue->head = (uncomp_queue->head + 1) & uncomp_queue->mask;
                    } else {
                        __sync_fetch_and_add(&ring->network_bytes_out, sent);
                        __sync_fetch_and_add(&ring->network_bytes_in, u_entry->length);
                        //INFO_PRINT("PARTIAL SEND REACHED\n");


                        //uncomp_queue->head = (uncomp_queue->head + 1) & uncomp_queue->mask;
                    }
                }
            }
          
        }


        if (!sent_something) {
            no_data_counter++;
            if (no_data_counter % 100000000 == 0) {
                DEBUG_PRINT("Network: No data to send (count: %lu)\n", no_data_counter);
            }
            //usleep(10);
        } else {
            no_data_counter = 0;
        }
    }
  
    INFO_PRINT("Network sender thread exiting\n");
    return NULL;
}

//periodically prints queue occupancies
void* monitor_thread(void *arg)
{
    thread_args_t* args = (thread_args_t*) arg;
    ring_buffer_t *ring = args->ring;
    pin_thread_to_core(args->core_id);
    INFO_PRINT("Monitor thread started on core %d\n", args->core_id);
    
    while (ring->running) {
        sleep(2); 
        
        uint32_t ring_occupancy = (ring->producer.tail - ring->consumer.head) & ring->mask;
        uint32_t comp_occupancy = (ring->compressed_send_queue.tail - 
                                    ring->compressed_send_queue.head) & 
                                   ring->compressed_send_queue.mask;
        uint32_t uncomp_occupancy = (ring->uncompressed_send_queue.tail - 
                                      ring->uncompressed_send_queue.head) & 
                                     ring->uncompressed_send_queue.mask;
        
        INFO_PRINT("Queue Occupancy: Ring=%u/%d, Comp=%u/%d, Uncomp=%u/%d\n",
               ring_occupancy, RING_SIZE,
               comp_occupancy, SEND_QUEUE_SIZE,
               uncomp_occupancy, SEND_QUEUE_SIZE);
    }
    
    INFO_PRINT("Monitor thread exiting\n");
    return NULL;
}

void cleanup_qat_buffers(ring_entry_t *entry)
{
    if (entry->pSrcData) qaeMemFreeNUMA((void**)&entry->pSrcData);
    if (entry->pDstData) qaeMemFreeNUMA((void**)&entry->pDstData);
    if (entry->pFlatSrcBuffer) qaeMemFreeNUMA((void**)&entry->pFlatSrcBuffer);
    if (entry->pFlatDstBuffer) qaeMemFreeNUMA((void**)&entry->pFlatDstBuffer);
    if (entry->pSrcBuffer) qaeMemFreeNUMA((void**)&entry->pSrcBuffer);
    if (entry->pDstBuffer) qaeMemFreeNUMA((void**)&entry->pDstBuffer);
    if (entry->pSrcBuffer && entry->pSrcBuffer->pPrivateMetaData) 
        qaeMemFreeNUMA((void**)&entry->pSrcBuffer->pPrivateMetaData);
    if (entry->pDstBuffer && entry->pDstBuffer->pPrivateMetaData)
        qaeMemFreeNUMA((void**)&entry->pDstBuffer->pPrivateMetaData);
}

void drain_qat_operations(ring_buffer_t *ring)
{
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
        if (all_empty) break;
        usleep(10000); 
        timeout--;
    }
}

void ring_buffer_cleanup(ring_buffer_t *ring)
{
    if (!ring) return;
    
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
    cleanup_file_reader(&ring->file_reader);

    
    free(ring);
}


int setup_server_socket(int port)
{
    int sockfd;
    struct sockaddr_in server_addr;
    int opt = 1;
    
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed");
        return -1;
    }
    
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, 
                   &opt, sizeof(opt))) {
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
    
    if (listen(sockfd, 1) < 0) {
        perror("listen failed");
        close(sockfd);
        return -1;
    }
    
    INFO_PRINT("Server listening on port %d\n", port);
    
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(sockfd, (struct sockaddr *)&client_addr, &client_len);
    
    if (client_fd < 0) {
        perror("accept failed");
        close(sockfd);
        return -1;
    }
    
    INFO_PRINT("Client connected from %s:%d\n",
           inet_ntoa(client_addr.sin_addr),
           ntohs(client_addr.sin_port));

    int sndbuf = 1024 * 1024 * 1024;  // 1 GB
    int rcvbuf = 1024 * 1024 * 1024;  // 1 GB
    
    int ret = setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
    if (ret < 0) {
        perror("ERROR: setsockopt SO_SNDBUF failed");
    }
    
    ret = setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    if (ret < 0) {
        perror("ERROR: setsockopt SO_RCVBUF failed");
    }
    
    socklen_t optlen = sizeof(sndbuf);
    getsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, &optlen);
    getsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, &optlen);
    
    INFO_PRINT("Actual socket buffers: send=%d bytes (%.2f MB), recv=%d bytes (%.2f MB)\n", 
            sndbuf, sndbuf/(1024.0*1024.0), 
            rcvbuf, rcvbuf/(1024.0*1024.0));
    
    int flags = fcntl(client_fd, F_GETFL, 0);
    fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);
    
    close(sockfd); 
    return client_fd;
}


//print qat_stats
void print_qat_stats(ring_buffer_t *ring, double elapsed_sec)
{
    if (ring->qat_stats.total_compressions > 0) {
        // double avg_time = (double)ring->qat_stats.total_compression_time_us / 
        //                  ring->qat_stats.total_compressions;
        // double compression_ratio = (double)ring->qat_stats.total_bytes_out / 
        //                           ring->qat_stats.total_bytes_in;
        // double elapsed_sec = (ring->qat_stats.last_callback_time.tv_sec - 
        //                             ring->qat_stats.first_submit_time.tv_sec) +
        //                            (ring->qat_stats.last_callback_time.tv_nsec - 
        //                             ring->qat_stats.first_submit_time.tv_nsec) / 1e9;
               
        // double mb_processed = (double)ring->qat_stats.total_bytes_in / (1024 * 1024);
        // double mb_bytes_out = (double)ring->qat_stats.total_bytes_out / (1024 * 1024);
        // double throughput_mbps = mb_processed / elapsed_sec;
        // double output_throughput = mb_bytes_out / elapsed_sec;
        // double ops_per_sec = ring->qat_stats.total_compressions / elapsed_sec;
        
        // printf("QAT Performance Stats:\n");
        // printf("  Total compressions: %lu\n", ring->qat_stats.total_compressions);
        // printf("  Average compression time: %.2f µs\n", avg_time);
        // printf("  Compression ratio: %.2f%%\n", compression_ratio * 100);
        // printf("  Operations per second: %.0f\n", ops_per_sec);
        // printf("  Throughput: %.2f MB/s\n", throughput_mbps);
        // printf("  Output Throughput: %.2f MB/s\n", output_throughput);
        double net_in_gbps = (ring->qat_stats.total_bytes_in * 8.0) / (elapsed_sec * 1e9);
        double net_out_gbps = (ring->qat_stats.total_bytes_out * 8.0) / (elapsed_sec * 1e9);

        INFO_PRINT("QAT Stats:\n");
        INFO_PRINT("    TX QAT Bytes In: %lu --> Throughput(%.2f Gbps)\n", ring->qat_stats.total_bytes_in, net_in_gbps);
        INFO_PRINT("    TX QAT Bytes Out: %lu --> Throughput(%.2f Gbps)\n", ring->qat_stats.total_bytes_out, net_out_gbps);
    }
}

//determine which benchmark to use based on command line arg
const char* map_file_number(const char *input) {
    static const char *base_path = "/home/npunati2/SquashBench/"; //TODO: change to where squash benchmarks are located
    static char full_path[512];
    
    if (strlen(input) == 1 && input[0] >= '1' && input[0] <= '5') {
        int file_num = input[0] - '0';
        const char *filename = NULL;
        
        switch(file_num) {
            case 1: filename = "geo.protodata"; break;
            case 2: filename = "nci"; break;
            case 3: filename = "ptt5"; break;
            case 4: filename = "sum"; break;
            case 5: filename = "xml"; break;
            default: return NULL;
        }
        
        snprintf(full_path, sizeof(full_path), "%s%s", base_path, filename);
        return full_path;
    }
    
    return input;
}

int main(int argc, char *argv[])
{
    int compression_level = 1;
    float compressed_fraction = 0.5; 
    int port = 9999; 
    int numa_node2_base = 64;
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
        } else if(strcmp(argv[i], "-i") == 0 && i + 1 < argc) {
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
    INFO_PRINT("Configuration: input_file=%s, compression_level=%d, compressed_fraction=%.2f\n", 
            input_file, compression_level, compressed_fraction);
    
        
    ring_buffer_t *ring = ring_buffer_init(compression_level, compressed_fraction, input_file);
    if (!ring) {
        printf("Failed to initialize ring buffer\n");
        return 1;
    }
    
    ring->socket_fd = setup_server_socket(port);
    if (ring->socket_fd < 0) {
        printf("Warning: Failed to setup socket, continuing without network\n");
        ring->socket_fd = -1;
    }
    
    pthread_t producer_tids[NUM_PRODUCERS];
    pthread_t consumer_tids[NUM_CONSUMERS];
    pthread_t sender_tids[NUM_SENDERS];
    pthread_t monitor_tid; 

    for (int i = 0; i < NUM_PRODUCERS; i++) { //core 64, 65
        thread_args_t *args = malloc(sizeof(thread_args_t));
        args->ring = ring;
        args->thread_id = i;
        args->core_id = numa_node2_base + i; 
        
        if (pthread_create(&producer_tids[i], NULL, producer_thread, args) != 0) {
            printf("Failed to create producer thread %d\n", i);
            ring->running = false;
            return 1;
        }
    }

    for (int i = 0; i < NUM_CONSUMERS; i++) { //core 66
        thread_args_t *args = malloc(sizeof(thread_args_t));
        args->ring = ring;
        args->thread_id = i;
        args->core_id = numa_node2_base + NUM_PRODUCERS + i;  
        
        if (pthread_create(&consumer_tids[i], NULL, consumer_thread, args) != 0) {
            printf("Failed to create consumer thread %d\n", i);
            ring->running = false;
            return 1;
        }
    }

    for (int i = 0; i < NUM_SENDERS; i++) { //core 67
        thread_args_t *args = malloc(sizeof(thread_args_t));
        args->ring = ring;
        args->thread_id = i;
        args->core_id = numa_node2_base + NUM_PRODUCERS + NUM_CONSUMERS + i;  
        
        if (pthread_create(&sender_tids[i], NULL, network_sender_thread, args) != 0) {
            printf("Failed to create sender thread %d\n", i);
            ring->running = false;
            return 1;
        }
    }

    thread_args_t *monitor_args = malloc(sizeof(thread_args_t));
    monitor_args->ring = ring;
    monitor_args->thread_id = 0;
    monitor_args->core_id = numa_node2_base + NUM_PRODUCERS + NUM_CONSUMERS + NUM_SENDERS;

    if (pthread_create(&monitor_tid, NULL, monitor_thread, monitor_args) != 0) { // core 67
        printf("Failed to create monitor thread\n");
        ring->running = false;
        return 1;
    }

    struct timespec start_ts, end_ts;
    clock_gettime(CLOCK_MONOTONIC, &start_ts);
    INFO_PRINT("Pipeline running with %d producers, %d consumers, %d senders\n",
        NUM_PRODUCERS, NUM_CONSUMERS, NUM_SENDERS);
    INFO_PRINT("All threads pinned to NUMA node 2 (cores %d-%d)\n",
            numa_node2_base, 
            numa_node2_base + NUM_PRODUCERS + NUM_CONSUMERS + NUM_SENDERS - 1);

    sleep(30);  
    
    INFO_PRINT("Shutting down...\n");
    ring->running = false;
    
    for (int i = 0; i < NUM_PRODUCERS; i++) {
        pthread_join(producer_tids[i], NULL);
    }
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        pthread_join(consumer_tids[i], NULL);
    }
    for (int i = 0; i < NUM_SENDERS; i++) {
        pthread_join(sender_tids[i], NULL);
    }
    pthread_join(monitor_tid, NULL);
    
    clock_gettime(CLOCK_MONOTONIC, &end_ts);

    double elapsed_sec = (end_ts.tv_sec - start_ts.tv_sec) + (end_ts.tv_nsec - start_ts.tv_nsec) / 1e9;
    INFO_PRINT("Elapsed Seconds: %f\n", elapsed_sec);
    double app_gbps = (ring->producer.app_bytes_generated * 8.0) / (elapsed_sec * 1e9);
    double net_in_gbps = (ring->network_bytes_in * 8.0) / (elapsed_sec * 1e9);
    double net_out_gbps = (ring->network_bytes_out * 8.0) / (elapsed_sec * 1e9);

    INFO_PRINT("TX Statistics:\n");
    INFO_PRINT("    TX App Bytes Generated: %lu --> Throughput(%.2f Gbps)\n", ring->producer.app_bytes_generated, app_gbps);
    INFO_PRINT("    TX Network Bytes In: %lu --> Throughput(%.2f Gbps)\n", ring->network_bytes_in, net_in_gbps);
    INFO_PRINT("    TX Network Bytes Out: %lu --> Throughput(%.2f Gbps)\n", ring->network_bytes_out, net_out_gbps);
    print_qat_stats(ring, elapsed_sec);
    INFO_PRINT("  Compressed packets: %lu\n", ring->compressed_packets_sent);
    INFO_PRINT("  Uncompressed packets: %lu\n", ring->uncompressed_packets_sent);
    // printf("  Jobs submitted: %lu\n", ring->producer.jobs_submitted);
    // printf("  Jobs sent: %lu\n", ring->consumer.jobs_sent);
    // printf("  Bytes sent: %lu\n", ring->consumer.bytes_sent);
    // printf("  Compressed packets: %lu\n", ring->compressed_packets_sent);
    // printf("  Uncompressed packets: %lu\n", ring->uncompressed_packets_sent);
    // printf("  Socket Blocked Count: %lu\n", ring->socket_blocked_count);
    // print_qat_stats(ring);

    if (ring->socket_fd > 0) {
        close(ring->socket_fd);
    }

    for (int i = 0; i < SEND_QUEUE_SIZE; i++) {
        free(ring->uncompressed_send_queue.entries[i].data);
    }

    ring_buffer_cleanup(ring);
    
    INFO_PRINT("Pipeline shutdown complete\n");
    return 0;
}
