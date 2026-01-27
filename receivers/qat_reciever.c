#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sched.h>
#include <sys/time.h>
#include <stdint.h>
#include <qat/cpa.h>
#include <qat/cpa_dc.h>
#include <qat/icp_sal_user.h>
#include <qat/icp_sal_poll.h>
#include <qat/qae_mem.h>
#include <stdbool.h>
#include <errno.h>
#include <signal.h>

#define MAGIC_COMPRESSED   0x51415443
#define MAGIC_UNCOMPRESSED 0x51415452
#define MAX_BUFFER_SIZE 131072
#define RING_SIZE 16384
#define CACHE_LINE_SIZE 64

static volatile bool g_shutdown_requested = false;

typedef struct {
    uint32_t magic;
    uint32_t uncompressed_size;
    uint32_t compressed_size;
    uint64_t sequence_number;
    uint8_t algorithm;
    uint8_t compression_level;
    uint16_t checksum;
} __attribute__((packed)) compression_header_t;

typedef enum {
    JOB_EMPTY = 0,
    JOB_READY = 1,      // Received from network, ready to submit to QAT
    JOB_SUBMITTED = 2,  // Submitted to QAT, waiting for callback
    JOB_COMPLETED = 3,  // QAT callback completed
    JOB_ERROR = 4
} job_status_t; 

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
} ring_entry_t;

typedef struct {
    ring_entry_t entries[RING_SIZE];
    
    struct {
        volatile uint32_t tail;
        uint64_t packets_received;
        uint64_t bytes_received;
        // uint64_t compressed_packets;
        // uint64_t uncompressed_packets;
        uint64_t jobs_submitted;
        char pad[CACHE_LINE_SIZE - 48];
    } __attribute__((aligned(CACHE_LINE_SIZE))) producer;
    
    struct {
        volatile uint32_t head;
        uint64_t jobs_completed;
        uint64_t jobs_processed;
        char pad[CACHE_LINE_SIZE - 24];
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
} ring_buffer_t;

static ring_buffer_t *g_ring = NULL;

void signal_handler(int signum) {
    printf("\nReceived signal %d, initiating graceful shutdown...\n", signum);
    g_shutdown_requested = true;
    if (g_ring) {
        g_ring->running = false;
    }
}

void decompress_callback(void *pCallbackTag, CpaStatus status)
{
    ring_entry_t *entry = (ring_entry_t *)pCallbackTag;
    
    if (status == CPA_STATUS_SUCCESS) {
        entry->producedSize = entry->dcResults.produced;
        __sync_synchronize();
        entry->status = JOB_COMPLETED;
        __sync_fetch_and_add(&g_ring->qat_stats.total_bytes_in, entry->srcDataSize);
        __sync_fetch_and_add(&g_ring->qat_stats.total_bytes_out, entry->producedSize);
    } else {
        __sync_synchronize();
        entry->status = JOB_ERROR;
    }
    
    entry->complete_time = clock();
}

int allocate_qat_buffers(ring_entry_t *entry, CpaInstanceHandle dcInstance)
{
    Cpa32U metaSize = 0;
    CpaStatus status;
    
    status = cpaDcBufferListGetMetaSize(dcInstance, 1, &metaSize);
    if (status != CPA_STATUS_SUCCESS) {
        printf("Failed to get metadata size\n");
        return -1;
    }

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

int qat_init(ring_buffer_t *ring, int compression_level)
{
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
    
    if (numInstances > 1) {
        ring->dcInstance = instances[1];
        printf("Using QAT instance 1\n");
    } else {
        ring->dcInstance = instances[0];
        printf("Only 1 instance available, using instance 0\n");
    }
    free(instances);
    
    status = cpaDcSetAddressTranslation(ring->dcInstance, qaeVirtToPhysNUMA);
    if (status != CPA_STATUS_SUCCESS) return -1;
    
    status = cpaDcStartInstance(ring->dcInstance, 512, NULL);
    if (status != CPA_STATUS_SUCCESS) return -1;
    
    CpaDcCompLvl qat_level = CPA_DC_L6;
    
    sessionSetupData.compLevel = qat_level;
    sessionSetupData.compType = CPA_DC_DEFLATE;
    sessionSetupData.huffType = CPA_DC_HT_STATIC;
    sessionSetupData.sessDirection = CPA_DC_DIR_DECOMPRESS;
    sessionSetupData.sessState = CPA_DC_STATELESS;
    sessionSetupData.checksum = CPA_DC_CRC32;

    // sessionSetupData.compLevel = qat_level;              // Compression level
    // sessionSetupData.compType = CPA_DC_LZ4;              // LZ4 compression
    // sessionSetupData.sessDirection = CPA_DC_DIR_DECOMPRESS;
    // sessionSetupData.sessState = CPA_DC_STATELESS;       
    // sessionSetupData.checksum = CPA_DC_NONE; 
    
    status = cpaDcGetSessionSize(ring->dcInstance, &sessionSetupData,
                                 &sessionSize, &contextSize);
    if (status != CPA_STATUS_SUCCESS) return -1;
    
    ring->sessionMemory = qaeMemAllocNUMA(sessionSize, 0, 64);
    if (!ring->sessionMemory) return -1;
    
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

ring_buffer_t* ring_buffer_init(int compression_level)
{
    ring_buffer_t *ring = aligned_alloc(CACHE_LINE_SIZE, sizeof(ring_buffer_t));
    if (!ring) return NULL;
    
    memset(ring, 0, sizeof(ring_buffer_t));
    
    if (qat_init(ring, compression_level) != 0) {
        free(ring);
        return NULL;
    }
    
    // Pre-allocate all buffers in the ring
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
    
    printf("Ring buffer initialized with %d pre-allocated entries\n", RING_SIZE);
    return ring;
}

int recv_exact(int sockfd, void *buffer, size_t len)
{
    size_t total = 0;
    while (total < len && !g_shutdown_requested) {
        ssize_t n = recv(sockfd, (char*)buffer + total, len - total, 0);
        if (n <= 0) {
            if (n == 0) {
                fprintf(stderr, "recv_exact: Connection closed\n");
            } else {
                fprintf(stderr, "recv_exact: Error %s\n", strerror(errno));
            }
            return -1;
        }
        total += n;
    }
    return g_shutdown_requested ? -1 : 0;
}

void* network_thread(void *arg)
{
    int sockfd = *(int*)arg;
    uint8_t *temp_buffer = malloc(MAX_BUFFER_SIZE);
    compression_header_t header;
    
    printf("Network thread started\n");
    
    while (g_ring->running) {
        //don't peek first 4 bytes, read the entire compression header (both compressed + uncompressed packets have it)
        if (recv_exact(sockfd, &header, sizeof(compression_header_t)) < 0) {
            printf("Connection closed or error reading header\n");
            break;
        }

        if (header.magic == MAGIC_COMPRESSED) {
            // compressed path
            uint32_t current_tail = g_ring->producer.tail;
            uint32_t next_tail = (current_tail + 1) & g_ring->mask;
            
            if (next_tail == g_ring->consumer.head) {
                // drain if no space in ring buffer -> this is why qat bytes don't align on RX and TX
                if (recv_exact(sockfd, temp_buffer, header.compressed_size) < 0) {
                    printf("Error draining compressed data\n");
                    break;
                }
                __sync_fetch_and_add(&g_ring->ring_full_count, 1);
                __sync_fetch_and_add(&g_ring->compressed_packets_recv, 1);
                __sync_fetch_and_add(&g_ring->producer.bytes_received, 
                                sizeof(compression_header_t) + header.compressed_size);
                continue;
            }
            
            ring_entry_t *entry = &g_ring->entries[current_tail];
            
            entry->header = header;
            
            if (recv_exact(sockfd, entry->pSrcData, header.compressed_size) < 0) {
                printf("Error reading compressed data\n");
                break;
            }
            
            entry->srcDataSize = header.compressed_size;
            entry->pFlatSrcBuffer->dataLenInBytes = header.compressed_size;
            entry->sequence_number = header.sequence_number;
            entry->status = JOB_READY; 
            
            __sync_synchronize();
            g_ring->producer.tail = next_tail;
            __sync_fetch_and_add(&g_ring->compressed_packets_recv, 1);
            __sync_fetch_and_add(&g_ring->producer.bytes_received, 
                                sizeof(compression_header_t) + header.compressed_size);

        } else if (header.magic == MAGIC_UNCOMPRESSED) {
            // uncompressed path 
            if (recv_exact(sockfd, temp_buffer, header.uncompressed_size) < 0) {
                printf("Error reading uncompressed data\n");
                break;
            }

            __sync_fetch_and_add(&g_ring->uncompressed_packets_recv, 1);
            __sync_fetch_and_add(&g_ring->uncompressed_bytes, header.uncompressed_size);
            __sync_fetch_and_add(&g_ring->producer.bytes_received, 
                                sizeof(compression_header_t) + header.uncompressed_size);
                                
        } else {
            printf("Protocol Error: Unknown magic 0x%08X (expected 0x%08X or 0x%08X)\n", 
                   header.magic, MAGIC_COMPRESSED, MAGIC_UNCOMPRESSED);
            printf("Header details: seq=%lu, comp_size=%u, uncomp_size=%u\n",
                   header.sequence_number, header.compressed_size, header.uncompressed_size);
            break;
        }
    }
    
    free(temp_buffer);
    printf("Network thread exiting\n");
    return NULL;
}


void* decompress_thread(void *arg)
{
    ring_buffer_t *ring = (ring_buffer_t*)arg;
    uint64_t completed_count = 0;
    uint64_t submit_count = 0;
    uint64_t retry_count = 0;
    uint64_t poll_count = 0;
    
    printf("Decompress thread started\n");
    
    while (ring->running && !g_shutdown_requested) {
        poll_count++;
        icp_sal_DcPollInstance(ring->dcInstance, 64);
        
        uint32_t current_head = ring->consumer.head;
        ring_entry_t *entry = &ring->entries[current_head];
        job_status_t current_status = __atomic_load_n(&entry->status, __ATOMIC_ACQUIRE);
        
        if (current_status == JOB_READY) {
            CpaDcOpData opData = {0};
            opData.flushFlag = CPA_DC_FLUSH_FINAL;
            opData.compressAndVerify = CPA_TRUE;
            
            entry->submit_time = clock();
            
            CpaStatus status = cpaDcDecompressData2(ring->dcInstance, ring->sessionHandle,
                                                    entry->pSrcBuffer, entry->pDstBuffer,
                                                    &opData, &entry->dcResults, entry);
            
            if (status == CPA_STATUS_SUCCESS) {
                __sync_synchronize();
                entry->status = JOB_SUBMITTED;
                __sync_synchronize();
                
                __sync_fetch_and_add(&ring->producer.jobs_submitted, 1);
                submit_count++;
                retry_count = 0;
                
            } else if (status == CPA_STATUS_RETRY) {
                retry_count++;
                __builtin_ia32_pause();
            } else {
                printf("Decompress: Submit failed: %d\n", status);
                entry->status = JOB_ERROR;
            }
            
        } else if (current_status == JOB_COMPLETED) {
            __sync_fetch_and_add(&ring->decompressed_packets, 1);
            __sync_fetch_and_add(&ring->decompressed_bytes, entry->producedSize);
            
            __sync_synchronize();
            entry->status = JOB_EMPTY;
            __sync_synchronize();
            
            ring->consumer.head = (current_head + 1) & ring->mask;
            ring->consumer.jobs_completed++;
            ring->consumer.jobs_processed++;
            
            completed_count++;
            
        } else if (current_status == JOB_ERROR) {
            __sync_fetch_and_add(&ring->decompress_errors, 1);
            
            __sync_synchronize();
            entry->status = JOB_EMPTY;
            __sync_synchronize();
            
            ring->consumer.head = (current_head + 1) & ring->mask;
            ring->consumer.jobs_processed++;
            
        } else if (current_status == JOB_SUBMITTED) {
            continue;
            
        } else if (current_status == JOB_EMPTY) {
            continue;
        }
    }
    
    printf("Decompress thread draining remaining jobs...\n");
    for (int i = 0; i < 10000; i++) {
        icp_sal_DcPollInstance(ring->dcInstance, 0);
    }
    
    printf("Decompress thread exiting (processed: %lu, completed: %lu)\n", 
           ring->consumer.jobs_processed, ring->consumer.jobs_completed);
    return NULL;
}

void* stats_thread(void *arg)
{
    ring_buffer_t *ring = (ring_buffer_t*)arg;
    uint64_t last_packets = 0;
    uint64_t last_bytes = 0;
    uint64_t last_decompressed = 0;
    uint64_t last_uncompressed = 0;
    time_t last_time = time(NULL);
    
    while (ring->running && !g_shutdown_requested) {
        sleep(5);
        
        time_t now = time(NULL);
        uint64_t current_packets = ring->producer.packets_received;
        uint64_t current_bytes = ring->producer.bytes_received;
        uint64_t current_decompressed = ring->decompressed_bytes;
        uint64_t current_uncompressed = ring->uncompressed_bytes;
        
        uint64_t pkt_delta = current_packets - last_packets;
        uint64_t byte_delta = current_bytes - last_bytes;
        uint64_t decomp_delta = current_decompressed - last_decompressed;
        uint64_t uncomp_delta = current_uncompressed - last_uncompressed;
        double elapsed = now - last_time;
        
        if (elapsed > 0) {
            double rx_gbps = (byte_delta * 8.0) / (elapsed * 1e9);
            double decomp_gbps = ((decomp_delta + uncomp_delta) * 8.0) / (elapsed * 1e9);
            
            uint32_t queue_depth = (ring->producer.tail - ring->consumer.head) & ring->mask;
            
            printf("RX: %.3f Gbps (wire) | Decompressed: %.3f Gbps | "
                   "%.0f pkt/s | Q:%u | Submitted:%lu Completed:%lu | Err:%lu\n",
                   rx_gbps, decomp_gbps,
                   pkt_delta / elapsed, queue_depth, 
                   ring->producer.jobs_submitted, ring->consumer.jobs_completed,
                   ring->decompress_errors);
        }
        
        last_packets = current_packets;
        last_bytes = current_bytes;
        last_decompressed = current_decompressed;
        last_uncompressed = current_uncompressed;
        last_time = now;
    }
    
    return NULL;
}

void cleanup_qat_buffers(ring_entry_t *entry)
{
    if (entry->pSrcData) qaeMemFreeNUMA((void**)&entry->pSrcData);
    if (entry->pDstData) qaeMemFreeNUMA((void**)&entry->pDstData);
    if (entry->pFlatSrcBuffer) qaeMemFreeNUMA((void**)&entry->pFlatSrcBuffer);
    if (entry->pFlatDstBuffer) qaeMemFreeNUMA((void**)&entry->pFlatDstBuffer);
    if (entry->pSrcBuffer && entry->pSrcBuffer->pPrivateMetaData) 
        qaeMemFreeNUMA((void**)&entry->pSrcBuffer->pPrivateMetaData);
    if (entry->pDstBuffer && entry->pDstBuffer->pPrivateMetaData)
        qaeMemFreeNUMA((void**)&entry->pDstBuffer->pPrivateMetaData);
    if (entry->pSrcBuffer) qaeMemFreeNUMA((void**)&entry->pSrcBuffer);
    if (entry->pDstBuffer) qaeMemFreeNUMA((void**)&entry->pDstBuffer);
}

void ring_buffer_cleanup(ring_buffer_t *ring)
{
    if (!ring) return;
    
    printf("Cleaning up ring buffer...\n");
    
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
    
    icp_sal_userStop();
    free(ring);
    printf("Ring buffer cleaned up\n");
}

int main(int argc, char *argv[])
{
    const char *host = "192.168.100.2";
    int port = 9999;
    int num_workers = 1;
    
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0 && i + 1 < argc) {
            host = argv[++i];
        } else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
            num_workers = atoi(argv[++i]);
        }
    }
    
    // Setup signal handlers for graceful shutdown
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    printf("=== Ring Buffer QAT Receiver ===\n");
    printf("Workers: %d\n", num_workers);
    // printf("Press Ctrl+C for graceful shutdown\n\n");
    
    ring_buffer_t *ring = ring_buffer_init(1);
    if (!ring) {
        printf("Failed to initialize ring buffer\n");
        return 1;
    }
    
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        ring_buffer_cleanup(ring);
        return 1;
    }

    int rcvbuf = 1024 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &server_addr.sin_addr);
    
    printf("Connecting to %s:%d...\n", host, port);
    if (connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        close(sockfd);
        ring_buffer_cleanup(ring);
        return 1;
    }
    
    printf("Connected to %s:%d\n\n", host, port);
    socklen_t len = sizeof(rcvbuf);
    getsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, &len);
    printf("RX buffer = %d bytes\n\n", rcvbuf);
    
    gettimeofday(&ring->start_time, NULL);
    
    pthread_t net_tid, stats_tid;
    pthread_t *worker_tids = malloc(num_workers * sizeof(pthread_t));
    
    pthread_create(&net_tid, NULL, network_thread, &sockfd);
    pthread_create(&stats_tid, NULL, stats_thread, ring);
    
    for (int i = 0; i < num_workers; i++) {
        pthread_create(&worker_tids[i], NULL, decompress_thread, ring);
    }
    
    printf("All threads started, waiting for completion...\n\n");
    
    pthread_join(net_tid, NULL);
    printf("Network thread completed\n");
    
    ring->running = false;
    
    printf("Waiting for worker threads to drain queue...\n");
    for (int i = 0; i < num_workers; i++) {
        pthread_join(worker_tids[i], NULL);
    }
    printf("All worker threads completed\n");
    
    pthread_join(stats_tid, NULL);
    
    struct timeval end_time;
    gettimeofday(&end_time, NULL);
    double elapsed = (end_time.tv_sec - ring->start_time.tv_sec) +
                     (end_time.tv_usec - ring->start_time.tv_usec) / 1e6;
    double network_in_gbps = (ring->producer.bytes_received * 8.0)/ (elapsed * 1e9);
    double qat_tput_in = (ring->qat_stats.total_bytes_in * 8.0)/ (elapsed * 1e9);
    double qat_tput_out = (ring->qat_stats.total_bytes_out * 8.0)/ (elapsed * 1e9);
    
    printf("\n=== FINAL STATISTICS ===\n");
    printf("Duration: %.2f sec\n", elapsed);
    printf("\nRX Statistics:\n");
    printf("    RX Network Bytes In: %lu --> Throughput(%.2f Gbps)\n", ring->producer.bytes_received, network_in_gbps);
    printf("    RX Network Bytes Out: %lu --> Throughput(%.2f Gbps)\n", ring->producer.bytes_received, network_in_gbps);
    printf("    RX QAT Bytes In: %lu --> Throughput(%.2f Gbps)\n", ring->qat_stats.total_bytes_in, qat_tput_in);
    printf("    RX QAT Bytes Out: %lu --> Throughput(%.2f Gbps)\n",ring->qat_stats.total_bytes_out, qat_tput_out);
    printf("    RX Uncompressed Packets Recieved: %lu\n", ring->uncompressed_packets_recv);
    printf("    RX Compressed Packets Recieved: %lu\n",ring->compressed_packets_recv);
    
    close(sockfd);
    ring_buffer_cleanup(ring);
    free(worker_tids);
    
    printf("\nReceiver shutdown complete\n");
    return 0;
}
