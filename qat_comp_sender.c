// Compile with: gcc -o qat_sender qat_compressed_sender.c -lqat -lusdm -lpthread

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

#define RING_SIZE 8 * 1024
#define CACHE_LINE_SIZE 64
#define MAX_BUFFER_SIZE 65536
#define SAMPLE_SIZE 256
#define SEND_QUEUE_SIZE 16384        

typedef enum {
    JOB_EMPTY = 0,
    JOB_SUBMITTED = 1,
    JOB_COMPLETED = 2,
    JOB_ERROR = 3
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

typedef struct {
    uint8_t *data;
    size_t length;
    bool is_compressed;
    compression_header_t *header;  
} send_queue_entry_t;

typedef struct {
    send_queue_entry_t entries[SEND_QUEUE_SIZE];
    volatile uint32_t head;
    volatile uint32_t tail;
    uint32_t mask;
    char pad[CACHE_LINE_SIZE];
} __attribute__((aligned(CACHE_LINE_SIZE))) send_queue_t;

// Ring buffer entry with QAT-specific fields
typedef struct {
    // Job tracking
    volatile job_status_t status;
    uint64_t sequence_number;
    
    // QAT buffer structures - MUST be allocated with qaeMemAlloc
    CpaBufferList *pSrcBuffer;
    CpaBufferList *pDstBuffer;
    CpaFlatBuffer *pFlatSrcBuffer;
    CpaFlatBuffer *pFlatDstBuffer;
    
    // Actual data pointers (within QAT-allocated memory)
    Cpa8U *pSrcData;
    Cpa8U *pDstData;
    
    // Data sizes
    Cpa32U srcDataSize;
    Cpa32U dstDataSize;
    Cpa32U producedSize;  // Actual compressed size
    
    // QAT results structure
    CpaDcRqResults dcResults;
    
    // Network header
    compression_header_t header;
    
    // Timestamps for performance tracking
    uint64_t submit_time;
    uint64_t complete_time;
} ring_entry_t;

// Main ring buffer structure
typedef struct {
    ring_entry_t entries[RING_SIZE];
    int compression_level;
    float compressed_fraction; 
    
    struct {
        volatile uint32_t tail;
        uint64_t jobs_submitted;
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

    send_queue_t compressed_send_queue;
    send_queue_t uncompressed_send_queue;
    
    volatile uint64_t sequence_counter;
    volatile uint64_t uncompressed_packets_sent;
    volatile uint64_t compressed_packets_sent; 
    volatile uint64_t socket_blocked_count;
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

// Global pointer for callbacks
static ring_buffer_t *g_ring = NULL;

// Callback function called by QAT when compression completes
void qat_dc_callback(void *pCallbackTag, CpaStatus status)
{
    static uint64_t callback_count = 0;
    callback_count++;
    
    ring_entry_t *entry = (ring_entry_t *)pCallbackTag;
    
    if (status == CPA_STATUS_SUCCESS) {
        uint64_t compression_time = clock() - entry->submit_time;
        uint64_t time_us = (compression_time * 1000000) / CLOCKS_PER_SEC;
        
        // Update stats atomically
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
        
        if (callback_count % 10000 == 0) {
            printf("Callback: %lu successful compressions\n", callback_count);
        }
    } else {
        printf("QAT compression failed: %d (callback #%lu)\n", status, callback_count);
        __sync_synchronize();
        entry->status = JOB_ERROR;
    }
    
    entry->complete_time = clock();
}

// Initialize QAT device and session
int qat_init(ring_buffer_t *ring)
{
    CpaStatus status;
    Cpa16U numInstances = 0;
    CpaInstanceHandle *instances = NULL;
    Cpa32U sessionSize = 0;
    Cpa32U contextSize = 0;
    
    // Step 1: Start the QAT user process
    status = icp_sal_userStartMultiProcess("SSL", CPA_FALSE);
    if (status != CPA_STATUS_SUCCESS) {
        printf("Failed to start QAT process: %d\n", status);
        return -1;
    }
    
    // Step 2: Get available DC (Data Compression) instances
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
    
    // Step 3: Use the first available instance
    ring->dcInstance = instances[0];
    free(instances);


    status = cpaDcSetAddressTranslation(ring->dcInstance, qaeVirtToPhysNUMA);
    if (status != CPA_STATUS_SUCCESS) {
        printf("Failed to set address translation: %d\n", status);
        return -1;
    }
    
    // Step 4: Start the DC instance
    Cpa16U numBuffers = 512;  // Number of intermediate buffers
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

    
    // Step 5: Set up compression session parameters
    ring->sessionSetupData.compLevel = qat_level;              // Fastest compression
    ring->sessionSetupData.compType = CPA_DC_DEFLATE;          // Standard DEFLATE
    ring->sessionSetupData.huffType = CPA_DC_HT_STATIC;        // Static Huffman for speed
    ring->sessionSetupData.autoSelectBestHuffmanTree = CPA_FALSE;
    ring->sessionSetupData.sessDirection = CPA_DC_DIR_COMPRESS;
    ring->sessionSetupData.sessState = CPA_DC_STATELESS;       // Each operation independent
    ring->sessionSetupData.checksum = CPA_DC_CRC32;
    ring->sessionSetupData.windowSize = 15;                     // 2^7 = 128 byte window
    
    // Step 6: Get required session memory size
    status = cpaDcGetSessionSize(ring->dcInstance, 
                                 &ring->sessionSetupData,
                                 &sessionSize, 
                                 &contextSize);
    if (status != CPA_STATUS_SUCCESS) {
        printf("Failed to get session size\n");
        return -1; 
    }
    
    // Step 7: Allocate session memory (must be from QAT memory)
    void *pSessionMemory = qaeMemAllocNUMA(sessionSize, 0, 64);
    if (pSessionMemory == NULL) {
        printf("Failed to allocate session memory\n");
        return -1;
    }

    ring->sessionHandle = pSessionMemory;
    // Step 8: Initialize the session
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
    
    printf("QAT initialization successful\n");
    return 0;
}

// Allocate QAT-compatible buffers for a ring entry
int allocate_qat_buffers(ring_entry_t *entry, CpaInstanceHandle dcInstance)
{
    Cpa32U metaSize = 0;
    CpaStatus status;
    
    // Get required metadata size for buffer lists
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
    
    // Allocate flat buffer structure for source
    entry->pFlatSrcBuffer = qaeMemAllocNUMA(sizeof(CpaFlatBuffer), 0, 64);
    if (!entry->pFlatSrcBuffer) return -1;
    
    // Allocate actual source data buffer
    entry->pSrcData = qaeMemAllocNUMA(MAX_BUFFER_SIZE, 0, 64);
    if (!entry->pSrcData) return -1;
    
    // Set up source buffer list
    entry->pSrcBuffer->pBuffers = entry->pFlatSrcBuffer;
    entry->pSrcBuffer->numBuffers = 1;
    
    entry->pFlatSrcBuffer->pData = entry->pSrcData;
    entry->pFlatSrcBuffer->dataLenInBytes = 0;  // Set when data copied
    
    // Allocate destination buffer list structure
    entry->pDstBuffer = qaeMemAllocNUMA(sizeof(CpaBufferList), 0, 64);
    if (!entry->pDstBuffer) return -1;
    
    // Allocate flat buffer structure for destination
    entry->pFlatDstBuffer = qaeMemAllocNUMA(sizeof(CpaFlatBuffer), 0, 64);
    if (!entry->pFlatDstBuffer) return -1;

    // Allocate metadata for destination buffer list
    if (metaSize > 0) {
        entry->pDstBuffer->pPrivateMetaData = qaeMemAllocNUMA(metaSize, 0, 64);
        if (!entry->pDstBuffer->pPrivateMetaData) return -1;
    } else {
        entry->pDstBuffer->pPrivateMetaData = NULL;
    }
    
    // Allocate actual destination data buffer (2x for worst case expansion)
    entry->pDstData = qaeMemAllocNUMA(MAX_BUFFER_SIZE * 2, 0, 64);
    if (!entry->pDstData) return -1;
    
    // Set up destination buffer list
    entry->pDstBuffer->pBuffers = entry->pFlatDstBuffer;
    entry->pDstBuffer->numBuffers = 1;
    
    entry->pFlatDstBuffer->pData = entry->pDstData;
    entry->pFlatDstBuffer->dataLenInBytes = MAX_BUFFER_SIZE * 2;
    
    return 0;
}

ring_buffer_t* ring_buffer_init(int compression_level, float compressed_fraction)
{
    ring_buffer_t *ring = aligned_alloc(CACHE_LINE_SIZE, sizeof(ring_buffer_t));
    if (!ring) return NULL;
    
    memset(ring, 0, sizeof(ring_buffer_t));
    ring->compression_level = compression_level;
    ring->compressed_fraction = compressed_fraction;

    ring->compressed_send_queue.mask = SEND_QUEUE_SIZE - 1;
    ring->uncompressed_send_queue.mask = SEND_QUEUE_SIZE - 1; 

    for (int i = 0; i < SEND_QUEUE_SIZE; i++) {
        ring->compressed_send_queue.entries[i].data = malloc(MAX_BUFFER_SIZE * 2);
        ring->uncompressed_send_queue.entries[i].data = malloc(MAX_BUFFER_SIZE);
    }
    
    if (qat_init(ring) != 0) {
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


void* producer_thread(void *arg)
{
    ring_buffer_t *ring = (ring_buffer_t *)arg;
    CpaStatus status;
    uint64_t sequence = 0;
    uint64_t retry_count = 0;
    uint64_t ring_full_count = 0;
    uint64_t queue_full_count = 0;
    
    char test_data[4096];
    memset(test_data, 'A', sizeof(test_data));
    
    printf("Producer thread started\n");
    
    while (ring->running) {
        float rand_val = (float)rand() / RAND_MAX;
        bool use_compressed = (rand_val < ring->compressed_fraction);
        
        if (use_compressed) {
            uint32_t current_tail = ring->producer.tail;
            uint32_t next_tail = (current_tail + 1) & ring->mask;
            
            if (next_tail == ring->consumer.head) {
                ring_full_count++;
                if (ring_full_count % 1000000000 == 0) {
                    printf("Producer: Ring buffer full (count: %lu)\n", ring_full_count);
                }
                //usleep(10);
                continue;  // Retry
            }
            ring_full_count = 0;
            
            ring_entry_t *entry = &ring->entries[current_tail];
            
            if (!ring->running) break;
            
            memcpy(entry->pSrcData, test_data, sizeof(test_data));
            entry->srcDataSize = sizeof(test_data);
            entry->pFlatSrcBuffer->dataLenInBytes = sizeof(test_data);
            
            entry->sequence_number = __sync_fetch_and_add(&ring->sequence_counter, 1);
            entry->header.magic = 0x51415443;
            entry->header.sequence_number = entry->sequence_number;
            entry->header.uncompressed_size = sizeof(test_data);
            entry->header.algorithm = 0;
            entry->header.compression_level = ring->compression_level;
            
            entry->pFlatDstBuffer->dataLenInBytes = MAX_BUFFER_SIZE * 2;
            
            CpaDcOpData opData = {0};
            opData.flushFlag = CPA_DC_FLUSH_FINAL;
            opData.compressAndVerify = CPA_TRUE;
            
            entry->submit_time = clock();
            
            // Submit to QAT
            status = cpaDcCompressData2(ring->dcInstance, ring->sessionHandle,
                                       entry->pSrcBuffer, entry->pDstBuffer,
                                       &opData, &entry->dcResults, entry);
            
            if (status == CPA_STATUS_SUCCESS) {
                if (!ring->qat_stats.timing_started) {
                    clock_gettime(CLOCK_MONOTONIC, &ring->qat_stats.first_submit_time);
                    __sync_synchronize();
                    ring->qat_stats.timing_started = true;
                }    
                __sync_synchronize();
                entry->status = JOB_SUBMITTED;
                ring->producer.tail = next_tail;
                ring->producer.jobs_submitted++;
                retry_count = 0;  
            } else if (status == CPA_STATUS_RETRY) {
                retry_count++;
                if (retry_count % 1000 == 0) {
                    printf("Producer: QAT retry count: %lu\n", retry_count);
                }
                //usleep(10);
                continue;  
            } else {
                printf("Producer: QAT submission failed with status %d\n", status);
                //usleep(100);
                continue;
            }
            
        } else {
            send_queue_t *queue = &ring->uncompressed_send_queue;
            uint32_t tail = queue->tail;
            uint32_t next_tail = (tail + 1) & queue->mask;
            
            // Check if queue is full
            if (next_tail == queue->head) {
                queue_full_count++;
                if (queue_full_count % 10000 == 0) {
                    printf("Producer: Uncompressed queue full (count: %lu)\n", queue_full_count);
                }
                usleep(10);
                continue;  
            }
            
            send_queue_entry_t *entry = &queue->entries[tail];
            memcpy(entry->data, test_data, sizeof(test_data));
            entry->length = sizeof(test_data);
            entry->is_compressed = false;
            entry->header = NULL;
            
            __sync_synchronize();
            queue->tail = next_tail;
            ring->producer.jobs_submitted++;  
        }
        
        //usleep(10); 
    }
    
    printf("Producer thread exiting (submitted: %lu)\n", ring->producer.jobs_submitted);
    return NULL;
}

void* consumer_thread(void *arg)
{
    ring_buffer_t *ring = (ring_buffer_t *)arg;
    CpaStatus status;
    uint64_t poll_count = 0;
    uint64_t no_progress_count = 0;
    uint32_t last_head = 0;
    uint64_t send_count = 0;
    
    printf("Consumer thread started\n");
    
    while (ring->running) {
        // ALWAYS poll QAT, even if nothing seems ready
        status = icp_sal_DcPollInstance(ring->dcInstance, 0);
        // poll_count++;
        
        // if (ring->consumer.head == last_head) {
        //     no_progress_count++;
        // } else {
        //     no_progress_count = 0;
        //     last_head = ring->consumer.head;
        // }
        
        // // Diagnostic: Check for stuck operations periodically
        // if (poll_count % 10000 == 0) {
        //     int stuck_jobs = 0;
        //     uint32_t oldest_stuck = UINT32_MAX;
            
        //     for (int i = 0; i < RING_SIZE; i++) {
        //         if (ring->entries[i].status == JOB_SUBMITTED) {
        //             stuck_jobs++;
        //             if (oldest_stuck == UINT32_MAX) {
        //                 oldest_stuck = i;
        //             }
        //         }
        //     }
            
        //     if (stuck_jobs > 0) {
        //         printf("Consumer: %d operations stuck in SUBMITTED state (oldest: entry %u)\n", 
        //                stuck_jobs, oldest_stuck);
                
        //         for (int retry = 0; retry < 100; retry++) {
        //             status = icp_sal_DcPollInstance(ring->dcInstance, 0);
        //             if (status != CPA_STATUS_SUCCESS && status != CPA_STATUS_RETRY) {
        //                 printf("Consumer: Poll failed with status %d\n", status);
        //                 break;
        //             }
        //             //usleep(10);
        //         }
                
        //         if (ring->entries[oldest_stuck].status == JOB_SUBMITTED) {
        //             printf("Consumer: Entry %u still stuck after aggressive polling\n", oldest_stuck);
                    
        //             if (no_progress_count > 100000) { 
        //                 printf("Consumer: WARNING - Marking stuck entry %u as ERROR\n", oldest_stuck);
        //                 ring->entries[oldest_stuck].status = JOB_ERROR;
        //                 no_progress_count = 0;
        //             }
        //         }
        //     }
        // }
        
        uint32_t current_head = ring->consumer.head;
        ring_entry_t *entry = &ring->entries[current_head];
        uint64_t still_submitted = 0;
        uint64_t status_other = 0;
        
        if (entry->status == JOB_COMPLETED) {
            send_queue_t *queue = &ring->compressed_send_queue;
            uint32_t tail = queue->tail;
            uint32_t next_tail = (tail + 1) & queue->mask;
            
            if (next_tail == queue->head) {
                printf("Consumer: no space in the compressed send queue....\n");
                continue;
            }
            
            send_queue_entry_t *send_entry = &queue->entries[tail];
            
            if (!send_entry->header) {
                send_entry->header = malloc(sizeof(compression_header_t));
            }
            memcpy(send_entry->header, &entry->header, sizeof(compression_header_t));
            memcpy(send_entry->data, entry->pDstData, entry->producedSize);
            send_entry->length = entry->producedSize;
            send_entry->is_compressed = true;
            
            __sync_synchronize();
            queue->tail = next_tail;
            
            entry->status = JOB_EMPTY;
            ring->consumer.jobs_sent++;
            ring->consumer.head = (current_head + 1) & ring->mask;
            send_count++;
            if(send_count % 10000  == 0) {
                printf("Consumer: Sent %lu entries to network thread\n", send_count);
            }
            
        } else if (entry->status == JOB_ERROR) {
            printf("Consumer: Skipping error entry at %u\n", current_head);
            entry->status = JOB_EMPTY;
            ring->consumer.head = (current_head + 1) & ring->mask;
            
        } else if (entry->status == JOB_SUBMITTED) {
            //usleep(1);
            still_submitted++;
            if(still_submitted % 10000 == 0) {
                printf("Consumer: Job is still in status SUBMITTED\n");
            }
            continue;
        } else {
            status_other++;
            if(status_other % 10000 == 0) {
                printf("Consumer: Job is in status OTHER\n");
            }
            continue;
            //usleep(1);
        }
    }
    
    printf("Consumer thread exiting\n");
    return NULL;
}

void* network_sender_thread(void *arg)
{
    ring_buffer_t *ring = (ring_buffer_t *)arg;
    struct iovec iov[2];
    int no_data_counter = 0;
    uint64_t last_packets_sent = 0;
    time_t last_report_time = time(NULL);
    
    printf("Network sender thread started\n");
    
    while (ring->running) {
        bool sent_something = false;
        
        time_t now = time(NULL);
        if (now - last_report_time >= 5) {  
            uint64_t current_packets = ring->compressed_packets_sent + ring->uncompressed_packets_sent;
            uint64_t packets_per_sec = (current_packets - last_packets_sent) / (now - last_report_time);
            
            printf("Network: %lu packets/sec (comp:%lu uncomp:%lu queued_comp:%u queued_uncomp:%u)\n",
                   packets_per_sec,
                   ring->compressed_packets_sent,
                   ring->uncompressed_packets_sent,
                   (ring->compressed_send_queue.tail - ring->compressed_send_queue.head) & ring->compressed_send_queue.mask,
                   (ring->uncompressed_send_queue.tail - ring->uncompressed_send_queue.head) & ring->uncompressed_send_queue.mask);
            
            last_packets_sent = current_packets;
            last_report_time = now;
        }
        
        send_queue_t *comp_queue = &ring->compressed_send_queue;
        if (comp_queue->head != comp_queue->tail) {
            send_queue_entry_t *entry = &comp_queue->entries[comp_queue->head];
            
            if (ring->socket_fd > 0) {
                // iov[0].iov_base = entry->header;
                // iov[0].iov_len = sizeof(compression_header_t);
                // iov[1].iov_base = entry->data;
                // iov[1].iov_len = entry->length;

                // size_t total_expected = sizeof(compression_header_t) + entry->length;
                // ssize_t sent = writev(ring->socket_fd, iov, 2);
                
                // if (sent == total_expected) {
                //     ring->consumer.bytes_sent += entry->length;
                //     __sync_fetch_and_add(&ring->compressed_packets_sent, 1);
                //     free(entry->header);
                //     entry->header = NULL; 
                //     comp_queue->head = (comp_queue->head + 1) & comp_queue->mask;
                //     sent_something = true;
                //     no_data_counter = 0;
                // } else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                //     __sync_fetch_and_add(&ring->socket_blocked_count, 1);
                //     usleep(100); 
                // } else if (sent < 0) {
                //     if (!ring->running) break;
                //     perror("writev failed");
                //     usleep(1000);
                // }
                free(entry->header);
                entry->header = NULL; 
                comp_queue->head = (comp_queue->head + 1) & comp_queue->mask;
                sent_something = true;
            }
        }
        
        // Priority 2: Uncompressed queue
        if (!sent_something) {
            send_queue_t *uncomp_queue = &ring->uncompressed_send_queue;
            if (uncomp_queue->head != uncomp_queue->tail) {
                send_queue_entry_t *entry = &uncomp_queue->entries[uncomp_queue->head];
                
                if (ring->socket_fd > 0) {
                    ssize_t sent = send(ring->socket_fd, entry->data, entry->length, MSG_NOSIGNAL);
                    
                    if (sent == entry->length) {
                        ring->consumer.bytes_sent += entry->length;
                        __sync_fetch_and_add(&ring->uncompressed_packets_sent, 1);
                        uncomp_queue->head = (uncomp_queue->head + 1) & uncomp_queue->mask;
                        sent_something = true;
                        no_data_counter = 0;
                    } else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                        __sync_fetch_and_add(&ring->socket_blocked_count, 1);
                        //usleep(100);
                    } else if (sent < 0) {
                        if (!ring->running) break;
                        perror("send failed");
                        //usleep(1000);
                    }
                }
            }
        }
        
        // if (!sent_something) {
        //     no_data_counter++;
        //     if (no_data_counter % 10000 == 0) {  // Reduced frequency
        //         printf("Network: No data to send (count: %d)\n", no_data_counter);
        //                         send_queue_t *comp = &ring->compressed_send_queue;
        //         send_queue_t *uncomp = &ring->uncompressed_send_queue;
        //         printf("  Compressed queue: head=%u tail=%u (empty=%d)\n",
        //                comp->head, comp->tail, comp->head == comp->tail);
        //         printf("  Uncompressed queue: head=%u tail=%u (empty=%d)\n",
        //                uncomp->head, uncomp->tail, uncomp->head == uncomp->tail);
        //     }
        //     //usleep(10);
        // } else {
        //     no_data_counter = 0;  // Reset counter when we send something
        // }
    }
    
    printf("Network sender thread exiting\n");
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
    
    printf("Server listening on port %d\n", port);
    
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(sockfd, (struct sockaddr *)&client_addr, &client_len);
    
    if (client_fd < 0) {
        perror("accept failed");
        close(sockfd);
        return -1;
    }
    
    printf("Client connected from %s:%d\n",
           inet_ntoa(client_addr.sin_addr),
           ntohs(client_addr.sin_port));
    
    int flags = fcntl(client_fd, F_GETFL, 0);
    fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);
    
    close(sockfd); 
    return client_fd;
}

void print_qat_stats(ring_buffer_t *ring)
{
    if (ring->qat_stats.total_compressions > 0) {
        double avg_time = (double)ring->qat_stats.total_compression_time_us / 
                         ring->qat_stats.total_compressions;
        double compression_ratio = (double)ring->qat_stats.total_bytes_out / 
                                  ring->qat_stats.total_bytes_in;
        double elapsed_sec = (ring->qat_stats.last_callback_time.tv_sec - 
                                    ring->qat_stats.first_submit_time.tv_sec) +
                                   (ring->qat_stats.last_callback_time.tv_nsec - 
                                    ring->qat_stats.first_submit_time.tv_nsec) / 1e9;
               
        double mb_processed = (double)ring->qat_stats.total_bytes_in / (1024 * 1024);
        double throughput_mbps = mb_processed / elapsed_sec;
        double ops_per_sec = ring->qat_stats.total_compressions / elapsed_sec;
        
        printf("QAT Performance Stats:\n");
        printf("  Total compressions: %lu\n", ring->qat_stats.total_compressions);
        printf("  Average compression time: %.2f µs\n", avg_time);
        printf("  Compression ratio: %.2f%%\n", compression_ratio * 100);
        printf("  Operations per second: %.0f\n", ops_per_sec);
        printf("  Throughput: %.2f MB/s\n", throughput_mbps);
    }
}

int main(int argc, char *argv[])
{
    int compression_level = 1;
    float compressed_fraction = 0.5; 
    int port = 9999; 

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-l") == 0 && i + 1 < argc) {
            compression_level = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-f") == 0 && i + 1 < argc) {
            compressed_fraction = atof(argv[++i]);
            if (compressed_fraction < 0.0 || compressed_fraction > 1.0) {
                printf("Error: fraction must be 0.0-1.0\n");
                return 1;
            }
        }
    }

    printf("Initializing QAT compression pipeline...\n");
    
    ring_buffer_t *ring = ring_buffer_init(compression_level, compressed_fraction);
    if (!ring) {
        printf("Failed to initialize ring buffer\n");
        return 1;
    }
    
    //ring->socket_fd = -1;  // Disabled for demo
    ring->socket_fd = setup_server_socket(port);
    if (ring->socket_fd < 0) {
        printf("Warning: Failed to setup socket, continuing without network\n");
        ring->socket_fd = -1;
    }
    
    pthread_t producer_tid, consumer_tid, sender_tid;
    
    if (pthread_create(&producer_tid, NULL, producer_thread, ring) != 0) {
        printf("Failed to create producer thread\n");
        ring_buffer_cleanup(ring);
        return 1;
    }
    
    if (pthread_create(&consumer_tid, NULL, consumer_thread, ring) != 0) {
        printf("Failed to create consumer thread\n");
        ring->running = false;
        pthread_join(producer_tid, NULL);
        ring_buffer_cleanup(ring);
        return 1;
    }

    if(pthread_create(&sender_tid, NULL, network_sender_thread, ring) != 0) {
        printf("Failed to create sender thread\n");
        ring->running = false; 
        pthread_join(producer_tid, NULL);
        pthread_join(consumer_tid, NULL);
        ring_buffer_cleanup(ring);
        return 1; 
    }
    
    printf("Pipeline running... Press Ctrl+C to stop\n");
    sleep(45);  
    
    printf("\nShutting down...\n");
    ring->running = false;
    
    pthread_join(producer_tid, NULL);
    pthread_join(consumer_tid, NULL);
    pthread_join(sender_tid, NULL); 
    
    printf("\nStatistics:\n");
    printf("  Jobs submitted: %lu\n", ring->producer.jobs_submitted);
    printf("  Jobs sent: %lu\n", ring->consumer.jobs_sent);
    printf("  Bytes sent: %lu\n", ring->consumer.bytes_sent);
    printf("  Compressed packets: %lu\n", ring->compressed_packets_sent);
    printf("  Uncompressed packets: %lu\n", ring->uncompressed_packets_sent);
    printf("  Socket Blocked Count: %lu\n", ring->socket_blocked_count);
    print_qat_stats(ring);

    if (ring->socket_fd > 0) {
        close(ring->socket_fd);
    }

    for (int i = 0; i < SEND_QUEUE_SIZE; i++) {
        free(ring->compressed_send_queue.entries[i].data);
        if (ring->compressed_send_queue.entries[i].header) {
            free(ring->compressed_send_queue.entries[i].header);
        }
        free(ring->uncompressed_send_queue.entries[i].data);
    }

    ring_buffer_cleanup(ring);
    
    printf("Pipeline shutdown complete\n");
    return 0;
}