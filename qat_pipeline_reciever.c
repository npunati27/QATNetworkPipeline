#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <stdint.h>
#include <qat/cpa.h>
#include <qat/cpa_dc.h>
#include <qat/icp_sal_user.h>
#include <qat/icp_sal_poll.h>
#include <qat/qae_mem.h>
#include <stdbool.h>

#define MAGIC_NUMBER 0x51415443
#define MAX_BUFFER_SIZE 131072
#define QUEUE_SIZE 16384

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
    volatile CpaBoolean complete;
    CpaStatus status;
} async_op_t;

typedef struct {
    compression_header_t header;
    uint8_t compressed_data[2048];
    size_t data_size;
    bool is_compressed;
} queued_packet_t;

typedef struct {
    queued_packet_t packets[QUEUE_SIZE];
    volatile uint32_t head;
    volatile uint32_t tail;
    uint32_t mask;
    volatile bool running;
} packet_queue_t;

typedef struct {
    volatile uint64_t packets_received;
    volatile uint64_t bytes_received;              
    volatile uint64_t compressed_packets;
    volatile uint64_t uncompressed_packets;
    volatile uint64_t decompressed_packets;
    volatile uint64_t decompressed_bytes;          
    volatile uint64_t decompress_errors;
    volatile uint64_t queue_full_count;
    struct timeval start_time;
} stats_t;

typedef struct {
    CpaInstanceHandle dcInstance;
    CpaDcSessionHandle sessionHandle;
    void *sessionMemory;
} qat_ctx_t;

packet_queue_t g_queue;
stats_t g_stats = {0};
qat_ctx_t g_qat_ctx = {0};

void decompress_callback(void *pCallbackTag, CpaStatus status)
{
    async_op_t *op = (async_op_t *)pCallbackTag;
    op->status = status;
    __sync_synchronize();
    op->complete = CPA_TRUE;
}

void queue_init(packet_queue_t *queue)
{
    memset(queue, 0, sizeof(packet_queue_t));
    queue->mask = QUEUE_SIZE - 1;
    queue->running = true;
}

bool queue_push(packet_queue_t *queue, compression_header_t *header, 
                uint8_t *data, size_t size, bool is_compressed)
{
    uint32_t current_tail = queue->tail;
    uint32_t next_tail = (current_tail + 1) & queue->mask;
    
    if (next_tail == queue->head) {
        __sync_fetch_and_add(&g_stats.queue_full_count, 1);
        return false;
    }
    
    queued_packet_t *pkt = &queue->packets[current_tail];
    
    if (is_compressed && size <= sizeof(pkt->compressed_data)) {
        pkt->header = *header;
        memcpy(pkt->compressed_data, data, size);
        pkt->data_size = size;
        pkt->is_compressed = true;
    } else {
        pkt->is_compressed = false;
        pkt->data_size = size;
    }
    
    __sync_synchronize();
    queue->tail = next_tail;
    
    return true;
}

bool queue_pop(packet_queue_t *queue, queued_packet_t *out_pkt)
{
    uint32_t current_head = queue->head;
    
    if (current_head == queue->tail) {
        return false;
    }
    
    *out_pkt = queue->packets[current_head];
    
    __sync_synchronize();
    queue->head = (current_head + 1) & queue->mask;
    
    return true;
}

int qat_init(qat_ctx_t *ctx, int compression_level)
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
        ctx->dcInstance = instances[1];
        printf("Using QAT instance 1\n");
    } else {
        ctx->dcInstance = instances[0];
        printf("Only 1 instance available, using instance 0\n");
    }
    free(instances);
    
    status = cpaDcSetAddressTranslation(ctx->dcInstance, qaeVirtToPhysNUMA);
    if (status != CPA_STATUS_SUCCESS) return -1;
    
    status = cpaDcStartInstance(ctx->dcInstance, 512, NULL);
    if (status != CPA_STATUS_SUCCESS) return -1;
    
    CpaDcCompLvl qat_level = CPA_DC_L1;
    
    sessionSetupData.compLevel = qat_level;
    sessionSetupData.compType = CPA_DC_DEFLATE;
    sessionSetupData.huffType = CPA_DC_HT_STATIC;
    sessionSetupData.sessDirection = CPA_DC_DIR_DECOMPRESS;
    sessionSetupData.sessState = CPA_DC_STATELESS;
    sessionSetupData.checksum = CPA_DC_CRC32;
    
    status = cpaDcGetSessionSize(ctx->dcInstance, &sessionSetupData,
                                 &sessionSize, &contextSize);
    if (status != CPA_STATUS_SUCCESS) return -1;
    
    ctx->sessionMemory = qaeMemAllocNUMA(sessionSize, 0, 64);
    if (!ctx->sessionMemory) return -1;
    
    ctx->sessionHandle = ctx->sessionMemory;
    
    status = cpaDcInitSession(ctx->dcInstance, ctx->sessionHandle,
                             &sessionSetupData, NULL, decompress_callback);
    if (status != CPA_STATUS_SUCCESS) {
        qaeMemFreeNUMA(&ctx->sessionMemory);
        return -1;
    }
    
    printf("QAT initialized with async callback\n");
    return 0;
}

int recv_exact(int sockfd, void *buffer, size_t len)
{
    size_t total = 0;
    while (total < len) {
        ssize_t n = recv(sockfd, (char*)buffer + total, len - total, 0);
        if (n <= 0) return -1;
        total += n;
    }
    return 0;
}

void* network_thread(void *arg)
{
    int sockfd = *(int*)arg;
    uint8_t temp_buffer[MAX_BUFFER_SIZE];
    
    printf("Network thread started\n");
    
    while (g_queue.running) {
        uint32_t magic;
        ssize_t n = recv(sockfd, &magic, sizeof(uint32_t), MSG_PEEK);
        if (n != sizeof(uint32_t)) break;
        
        if (magic == MAGIC_NUMBER) {
            compression_header_t header;
            if (recv_exact(sockfd, &header, sizeof(header)) < 0) break;
            if (recv_exact(sockfd, temp_buffer, header.compressed_size) < 0) break;
            
            __sync_fetch_and_add(&g_stats.packets_received, 1);
            __sync_fetch_and_add(&g_stats.bytes_received, sizeof(header) + header.compressed_size);
            __sync_fetch_and_add(&g_stats.compressed_packets, 1);
            
            queue_push(&g_queue, &header, temp_buffer, header.compressed_size, true);
            
        } else {
            ssize_t n = recv(sockfd, temp_buffer, sizeof(temp_buffer), 0);
            if (n <= 0) break;
            
            __sync_fetch_and_add(&g_stats.packets_received, 1);
            __sync_fetch_and_add(&g_stats.bytes_received, n);
            __sync_fetch_and_add(&g_stats.uncompressed_packets, 1);
        }
    }
    
    printf("Network thread exiting\n");
    return NULL;
}

void* polling_thread(void *arg)
{
    qat_ctx_t *ctx = (qat_ctx_t*)arg;
    
    printf("Polling thread started\n");
    
    while (g_queue.running) {
        icp_sal_DcPollInstance(ctx->dcInstance, 0);
    }
    
    // Keep polling for a bit after shutdown to drain
    printf("Draining QAT...\n");
    for (int i = 0; i < 100000; i++) {
        icp_sal_DcPollInstance(ctx->dcInstance, 0);
    }
    
    printf("Polling thread exiting\n");
    return NULL;
}

void* decompress_thread(void *arg)
{
    qat_ctx_t *ctx = (qat_ctx_t*)arg;
    queued_packet_t pkt;
    
    CpaBufferList *pSrcBuffer = qaeMemAllocNUMA(sizeof(CpaBufferList), 0, 64);
    CpaBufferList *pDstBuffer = qaeMemAllocNUMA(sizeof(CpaBufferList), 0, 64);
    CpaFlatBuffer *pFlatSrc = qaeMemAllocNUMA(sizeof(CpaFlatBuffer), 0, 64);
    CpaFlatBuffer *pFlatDst = qaeMemAllocNUMA(sizeof(CpaFlatBuffer), 0, 64);
    Cpa8U *srcData = qaeMemAllocNUMA(MAX_BUFFER_SIZE, 0, 64);
    Cpa8U *dstData = qaeMemAllocNUMA(MAX_BUFFER_SIZE, 0, 64);
    
    Cpa32U metaSize = 0;
    cpaDcBufferListGetMetaSize(ctx->dcInstance, 1, &metaSize);
    Cpa8U *srcMetaData = NULL;
    Cpa8U *dstMetaData = NULL;
    if (metaSize > 0) {
        srcMetaData = qaeMemAllocNUMA(metaSize, 0, 64);
        dstMetaData = qaeMemAllocNUMA(metaSize, 0, 64);
    }
    
    printf("Decompress thread started\n");
    
    while (g_queue.running) {
        if (!queue_pop(&g_queue, &pkt)) {
            //usleep(10);
            continue;
        }
        
        if (!pkt.is_compressed) continue;
        
        memcpy(srcData, pkt.compressed_data, pkt.data_size);
        
        pFlatSrc->pData = srcData;
        pFlatSrc->dataLenInBytes = pkt.data_size;
        pSrcBuffer->pBuffers = pFlatSrc;
        pSrcBuffer->numBuffers = 1;
        pSrcBuffer->pPrivateMetaData = srcMetaData;
        
        pFlatDst->pData = dstData;
        pFlatDst->dataLenInBytes = MAX_BUFFER_SIZE;
        pDstBuffer->pBuffers = pFlatDst;
        pDstBuffer->numBuffers = 1;
        pDstBuffer->pPrivateMetaData = dstMetaData;
        
        CpaDcOpData opData = {0};
        opData.flushFlag = CPA_DC_FLUSH_FINAL;
        opData.compressAndVerify = CPA_TRUE;
        
        CpaDcRqResults dcResults = {0};
        async_op_t async_op = { .complete = CPA_FALSE, .status = CPA_STATUS_SUCCESS };
        
        CpaStatus status = cpaDcDecompressData2(ctx->dcInstance, ctx->sessionHandle,
                                                pSrcBuffer, pDstBuffer, &opData,
                                                &dcResults, &async_op);
        
        if (status == CPA_STATUS_SUCCESS) {
            int timeout = 100000;
            while (!async_op.complete && timeout > 0) {
                usleep(1);
                timeout--;
            }
            
            if (async_op.complete) {
                if (async_op.status == CPA_STATUS_SUCCESS) {
                    __sync_fetch_and_add(&g_stats.decompressed_packets, 1);
                    __sync_fetch_and_add(&g_stats.decompressed_bytes, dcResults.produced);
                } else {
                    __sync_fetch_and_add(&g_stats.decompress_errors, 1);
                }
            } else {
                __sync_fetch_and_add(&g_stats.decompress_errors, 1);
            }
        } else if (status == CPA_STATUS_RETRY) {
            usleep(1);
        } else {
            __sync_fetch_and_add(&g_stats.decompress_errors, 1);
        }
    }
    
    qaeMemFreeNUMA((void**)&srcData);
    qaeMemFreeNUMA((void**)&dstData);
    qaeMemFreeNUMA((void**)&pFlatSrc);
    qaeMemFreeNUMA((void**)&pFlatDst);
    if (srcMetaData) qaeMemFreeNUMA((void**)&srcMetaData);
    if (dstMetaData) qaeMemFreeNUMA((void**)&dstMetaData);
    qaeMemFreeNUMA((void**)&pSrcBuffer);
    qaeMemFreeNUMA((void**)&pDstBuffer);
    
    printf("Decompress thread exiting\n");
    return NULL;
}

void* stats_thread(void *arg)
{
    uint64_t last_packets = 0;
    uint64_t last_bytes = 0;
    uint64_t last_decompressed = 0;
    time_t last_time = time(NULL);
    
    while (g_queue.running) {
        sleep(5);
        
        time_t now = time(NULL);
        uint64_t current_packets = g_stats.packets_received;
        uint64_t current_bytes = g_stats.bytes_received;
        uint64_t current_decompressed = g_stats.decompressed_bytes;
        
        uint64_t pkt_delta = current_packets - last_packets;
        uint64_t byte_delta = current_bytes - last_bytes;
        uint64_t decomp_delta = current_decompressed - last_decompressed;
        double elapsed = now - last_time;
        
        double rx_mbps = (byte_delta * 8.0) / (elapsed * 1000000.0);
        double decomp_mbps = (decomp_delta * 8.0) / (elapsed * 1000000.0);
        
        uint32_t queue_depth = (g_queue.tail - g_queue.head) & g_queue.mask;
        
        printf("RX: %.2f Mbps (wire) | Decompressed: %.2f Mbps (%.2f MB/s) | "
               "%.0f pkt/s | Q:%u | Total Decomp:%lu | Err:%lu\n",
               rx_mbps, decomp_mbps, decomp_delta / (elapsed * 1024.0 * 1024.0),
               pkt_delta / elapsed, queue_depth, 
               g_stats.decompressed_packets, g_stats.decompress_errors);
        
        last_packets = current_packets;
        last_bytes = current_bytes;
        last_decompressed = current_decompressed;
        last_time = now;
    }
    
    return NULL;
}

int main(int argc, char *argv[])
{
    const char *host = "192.168.100.3";
    int port = 9999;
    int num_workers = 4;
    
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0 && i + 1 < argc) {
            host = argv[++i];
        } else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
            num_workers = atoi(argv[++i]);
        }
    }
    
    printf("=== Async QAT Receiver ===\n");
    printf("Workers: %d\n\n", num_workers);
    
    queue_init(&g_queue);
    
    if (qat_init(&g_qat_ctx, 1) < 0) return 1;
    
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return 1;
    }

    int rcvbuf = 64 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &server_addr.sin_addr);
    
    if (connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        return 1;
    }
    
    printf("Connected to %s:%d\n\n", host, port);
    
    gettimeofday(&g_stats.start_time, NULL);
    
    pthread_t net_tid, stats_tid, poll_tid;
    pthread_t *worker_tids = malloc(num_workers * sizeof(pthread_t));
    
    pthread_create(&poll_tid, NULL, polling_thread, &g_qat_ctx);
    pthread_create(&net_tid, NULL, network_thread, &sockfd);
    pthread_create(&stats_tid, NULL, stats_thread, NULL);
    
    for (int i = 0; i < num_workers; i++) {
        pthread_create(&worker_tids[i], NULL, decompress_thread, &g_qat_ctx);
    }
    
    pthread_join(net_tid, NULL);
    g_queue.running = false;
    
    // Wait for workers to finish
    for (int i = 0; i < num_workers; i++) {
        pthread_join(worker_tids[i], NULL);
    }
    
    // Wait for polling thread to drain
    pthread_join(poll_tid, NULL);
    pthread_join(stats_tid, NULL);
    
    struct timeval end_time;
    gettimeofday(&end_time, NULL);
    double elapsed = (end_time.tv_sec - g_stats.start_time.tv_sec) +
                     (end_time.tv_usec - g_stats.start_time.tv_usec) / 1e6;
    
    printf("\n=== FINAL STATISTICS ===\n");
    printf("Duration: %.2f sec\n", elapsed);
    printf("\nNetwork (Wire):\n");
    printf("  Packets received:  %lu\n", g_stats.packets_received);
    printf("  Bytes received:    %.2f MB\n", g_stats.bytes_received / (1024.0 * 1024.0));
    printf("  Throughput:        %.2f Mbps\n", 
           (g_stats.bytes_received * 8.0) / (elapsed * 1000000.0));
    
    printf("\nDecompression:\n");
    printf("  Packets decompressed: %lu (%.1f%%)\n", 
           g_stats.decompressed_packets,
           g_stats.compressed_packets > 0 ? 
           (g_stats.decompressed_packets * 100.0) / g_stats.compressed_packets : 0);
    printf("  Decompressed bytes:   %.2f GB\n", 
           g_stats.decompressed_bytes / (1024.0 * 1024.0 * 1024.0));
    printf("  Decompressed throughput: %.2f Mbps (%.2f MB/s)\n",
           (g_stats.decompressed_bytes * 8.0) / (elapsed * 1000000.0),
           g_stats.decompressed_bytes / (elapsed * 1024.0 * 1024.0));
    printf("  Errors:              %lu\n", g_stats.decompress_errors);
    
    printf("\nCompression Ratio:\n");
    if (g_stats.decompressed_bytes > 0) {
        printf("  %.2f MB (wire) → %.2f GB (decompressed)\n",
               g_stats.bytes_received / (1024.0 * 1024.0),
               g_stats.decompressed_bytes / (1024.0 * 1024.0 * 1024.0));
        printf("  Ratio: %.2f%% (%.1f:1 expansion)\n",
               (g_stats.bytes_received * 100.0) / g_stats.decompressed_bytes,
               (double)g_stats.decompressed_bytes / g_stats.bytes_received);
    }
    
    close(sockfd);
    cpaDcRemoveSession(g_qat_ctx.dcInstance, g_qat_ctx.sessionHandle);
    qaeMemFreeNUMA(&g_qat_ctx.sessionMemory);
    cpaDcStopInstance(g_qat_ctx.dcInstance);
    icp_sal_userStop();
    
    free(worker_tids);
    
    return 0;
}