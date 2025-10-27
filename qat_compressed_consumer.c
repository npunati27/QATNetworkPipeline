#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <stdint.h>
#include <qat/cpa.h>
#include <qat/cpa_dc.h>
#include <qat/icp_sal_user.h>
#include <qat/icp_sal_poll.h>
#include <qat/qae_mem.h>

// header struct
typedef struct {
    uint32_t magic;
    uint32_t uncompressed_size;
    uint32_t compressed_size;
    uint64_t sequence_number;
    uint8_t algorithm;
    uint8_t compression_level;
    uint16_t checksum;
} __attribute__((packed)) compression_header_t;

#define MAGIC_NUMBER 0x51415443  
#define MAX_BUFFER_SIZE 65536

// tracking statistics
typedef struct {
    uint64_t packets_received;
    uint64_t compressed_packets;
    uint64_t uncompressed_packets;
    
    // wire bytes recieved
    uint64_t bytes_received_on_wire;           
    uint64_t compressed_bytes_on_wire;         
    uint64_t uncompressed_bytes_on_wire;       
    
    // nformation/goodput bytes recieved
    uint64_t effective_data_bytes;             
    uint64_t bytes_from_compressed_packets;    
    uint64_t bytes_from_uncompressed_packets;  
    
    uint64_t decompress_errors;
    
    struct timeval start_time;
    struct timeval end_time;
} stats_t;

// decompression context
typedef struct {
    CpaInstanceHandle dcInstance;
    CpaDcSessionHandle sessionHandle;
    void *sessionMemory;
    volatile CpaBoolean callbackComplete;
    CpaStatus callbackStatus;
} qat_decompress_ctx_t;

// decompression callback
void decompress_callback(void *pCallbackTag, CpaStatus status)
{
    qat_decompress_ctx_t *ctx = (qat_decompress_ctx_t *)pCallbackTag;
    ctx->callbackStatus = status;
    ctx->callbackComplete = CPA_TRUE;
}

// init qat for decompression
int qat_decompress_init(qat_decompress_ctx_t *ctx, int compression_level)
{
    CpaStatus status;
    Cpa16U numInstances = 0;
    CpaInstanceHandle *instances = NULL;
    CpaDcSessionSetupData sessionSetupData = {0};
    Cpa32U sessionSize = 0;
    Cpa32U contextSize = 0;
    
    // start qat user processes
    status = icp_sal_userStartMultiProcess("SSL", CPA_FALSE);
    if (status != CPA_STATUS_SUCCESS) {
        printf("Failed to start QAT process: %d\n", status);
        return -1;
    }
    
    // get compression instances
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
    
    // use first instance
    ctx->dcInstance = instances[0];
    free(instances);
    
    // set up address translation
    status = cpaDcSetAddressTranslation(ctx->dcInstance, qaeVirtToPhysNUMA);
    if (status != CPA_STATUS_SUCCESS) {
        printf("Failed to set address translation: %d\n", status);
        return -1;
    }
    
    // start compression instance
    status = cpaDcStartInstance(ctx->dcInstance, 512, NULL);
    if (status != CPA_STATUS_SUCCESS) {
        printf("Failed to start DC instance: %d\n", status);
        return -1;
    }

    // select compression level based on command line arg
    CpaDcCompLvl qat_level;
    switch(compression_level) {
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
    
    // set up decompression session
    sessionSetupData.compLevel = qat_level;
    sessionSetupData.compType = CPA_DC_DEFLATE;
    sessionSetupData.huffType = CPA_DC_HT_STATIC;
    sessionSetupData.sessDirection = CPA_DC_DIR_DECOMPRESS;
    sessionSetupData.sessState = CPA_DC_STATELESS;
    sessionSetupData.checksum = CPA_DC_CRC32;
    
    // get session size
    status = cpaDcGetSessionSize(ctx->dcInstance, &sessionSetupData,
                                 &sessionSize, &contextSize);
    if (status != CPA_STATUS_SUCCESS) {
        printf("Failed to get session size\n");
        return -1;
    }
    
    // allocate session memory
    ctx->sessionMemory = qaeMemAllocNUMA(sessionSize, 0, 64);
    if (!ctx->sessionMemory) {
        printf("Failed to allocate session memory\n");
        return -1;
    }
    
    // init session
    ctx->sessionHandle = ctx->sessionMemory;
    status = cpaDcInitSession(ctx->dcInstance, ctx->sessionHandle,
                             &sessionSetupData, NULL, decompress_callback);
    if (status != CPA_STATUS_SUCCESS) {
        printf("Failed to init session: %d\n", status);
        qaeMemFreeNUMA(&ctx->sessionMemory);
        return -1;
    }
    
    printf("QAT decompression initialized successfully\n");
    return 0;
}

// connect to server
int connect_to_server(const char *host, int port)
{
    int sockfd;
    struct sockaddr_in server_addr;
    
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed");
        return -1;
    }
    
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, host, &server_addr.sin_addr) <= 0) {
        perror("invalid address");
        close(sockfd);
        return -1;
    }
    
    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("connection failed");
        close(sockfd);
        return -1;
    }
    
    printf("Connected to %s:%d\n\n", host, port);
    return sockfd;
}

// Rrecieve bytes
int recv_exact(int sockfd, void *buffer, size_t len)
{
    size_t total_received = 0;
    while (total_received < len) {
        ssize_t n = recv(sockfd, (char*)buffer + total_received, 
                        len - total_received, 0);
        if (n <= 0) {
            if (n == 0) {
                printf("Connection closed by server\n");
            } else {
                perror("recv failed");
            }
            return -1;
        }
        total_received += n;
    }
    return 0;
}

//check if packet is compressed
int peek_magic(int sockfd, uint32_t *magic)
{
    ssize_t n = recv(sockfd, magic, sizeof(uint32_t), MSG_PEEK);
    if (n != sizeof(uint32_t)) {
        return -1;
    }
    return 0;
}

//process regular packet
int process_uncompressed_packet(int sockfd, stats_t *stats)
{
    uint8_t buffer[4096];
    
    ssize_t n = recv(sockfd, buffer, sizeof(buffer), 0);
    if (n <= 0) {
        if (n == 0) printf("Connection closed\n");
        else perror("recv failed");
        return -1;
    }
    
    //update data tracking
    stats->uncompressed_packets++;
    stats->bytes_received_on_wire += n;
    stats->uncompressed_bytes_on_wire += n;
    stats->effective_data_bytes += n;                    
    stats->bytes_from_uncompressed_packets += n;
    
    return 0;
}

// process compressed packet
int process_packet(qat_decompress_ctx_t *ctx, int sockfd, stats_t *stats)
{
    compression_header_t header;
    CpaBufferList *pSrcBuffer = NULL;
    CpaBufferList *pDstBuffer = NULL;
    CpaFlatBuffer *pFlatSrc = NULL;
    CpaFlatBuffer *pFlatDst = NULL;
    Cpa8U *srcData = NULL;
    Cpa8U *dstData = NULL;
    Cpa8U *srcMetaData = NULL;
    Cpa8U *dstMetaData = NULL;
    CpaDcRqResults dcResults = {0};
    CpaDcOpData opData = {0};
    CpaStatus status;
    Cpa32U metaSize = 0;
    
    // recieve and valdiate header
    if (recv_exact(sockfd, &header, sizeof(header)) < 0) {
        return -1;
    }
    
    if (header.magic != MAGIC_NUMBER) {
        printf("Invalid magic number: 0x%08x\n", header.magic);
        return -1;
    }
    
    // track wire level bytes
    stats->bytes_received_on_wire += sizeof(header) + header.compressed_size;
    stats->compressed_bytes_on_wire += header.compressed_size;
    
    cpaDcBufferListGetMetaSize(ctx->dcInstance, 1, &metaSize);
    
    // ddetermine destination buffer size with margin
    Cpa32U safe_dst_size = header.uncompressed_size;
    
    Cpa32U safety_margin = (header.uncompressed_size / 10) > 1024 ? 
                           (header.uncompressed_size / 10) : 1024;
    safe_dst_size += safety_margin;
    if (safe_dst_size < MAX_BUFFER_SIZE) {
        safe_dst_size = MAX_BUFFER_SIZE;
    }
    
    printf("[COMPRESSED PACKET] Seq: %lu, Compressed: %u bytes -> Uncompressed: %u bytes (Ratio: %.2f:1)\n",
           header.sequence_number,
           header.compressed_size,
           header.uncompressed_size,
           (float)header.uncompressed_size / (float)header.compressed_size);
    
    // allocate qat buffer
    pSrcBuffer = qaeMemAllocNUMA(sizeof(CpaBufferList), 0, 64);
    pDstBuffer = qaeMemAllocNUMA(sizeof(CpaBufferList), 0, 64);
    pFlatSrc = qaeMemAllocNUMA(sizeof(CpaFlatBuffer), 0, 64);
    pFlatDst = qaeMemAllocNUMA(sizeof(CpaFlatBuffer), 0, 64);
    srcData = qaeMemAllocNUMA(header.compressed_size, 0, 64);
    dstData = qaeMemAllocNUMA(safe_dst_size, 0, 64);  // Use safe size
    
    if (metaSize > 0) {
        srcMetaData = qaeMemAllocNUMA(metaSize, 0, 64);
        dstMetaData = qaeMemAllocNUMA(metaSize, 0, 64);
    }
    
    if (!pSrcBuffer || !pDstBuffer || !pFlatSrc || !pFlatDst || 
        !srcData || !dstData || (metaSize > 0 && (!srcMetaData || !dstMetaData))) {
        printf("Memory allocation failed\n");
        goto cleanup;
    }
    
    // recieve compressed data
    if (recv_exact(sockfd, srcData, header.compressed_size) < 0) {
        goto cleanup;
    }
    
    // set up source info
    pFlatSrc->pData = srcData;
    pFlatSrc->dataLenInBytes = header.compressed_size;
    pSrcBuffer->pBuffers = pFlatSrc;
    pSrcBuffer->numBuffers = 1;
    pSrcBuffer->pPrivateMetaData = srcMetaData;
    
    // set up dst info
    pFlatDst->pData = dstData;
    pFlatDst->dataLenInBytes = safe_dst_size; 
    pDstBuffer->pBuffers = pFlatDst;
    pDstBuffer->numBuffers = 1;
    pDstBuffer->pPrivateMetaData = dstMetaData;
    
    // set up operation for decompression
    opData.flushFlag = CPA_DC_FLUSH_FINAL;
    opData.compressAndVerify = CPA_TRUE;
    
    ctx->callbackComplete = CPA_FALSE;
    ctx->callbackStatus = CPA_STATUS_SUCCESS;
    
    // call decompress
    status = cpaDcDecompressData2(ctx->dcInstance, ctx->sessionHandle,
                                  pSrcBuffer, pDstBuffer, &opData,
                                  &dcResults, ctx);
    
    if (status != CPA_STATUS_SUCCESS) {
        printf("Decompression submit failed: %d\n", status);
        stats->decompress_errors++;
        goto cleanup;
    }
    
    // poll for completion
    int timeout = 1000;
    while (!ctx->callbackComplete && timeout > 0) {
        icp_sal_DcPollInstance(ctx->dcInstance, 0);
        usleep(100);
        timeout--;
    }
    
    if (!ctx->callbackComplete) {
        printf("Decompression timeout\n");
        stats->decompress_errors++;
        goto cleanup;
    }
    
    if (ctx->callbackStatus != CPA_STATUS_SUCCESS) {
        printf("Decompression failed: %d (produced: %u bytes, expected: %u bytes)\n", 
               ctx->callbackStatus, dcResults.produced, header.uncompressed_size);
        stats->decompress_errors++;
        goto cleanup;
    }
    
    // verify results size
    if (dcResults.produced > safe_dst_size) {
        printf("CRITICAL: Size overflow: produced %u, buffer was %u\n", 
               dcResults.produced, safe_dst_size);
        stats->decompress_errors++;
    } else if (dcResults.produced != header.uncompressed_size) {
        printf("WARNING: Size mismatch: expected %u, got %u (diff: %d)\n", 
               header.uncompressed_size, dcResults.produced,
               (int)dcResults.produced - (int)header.uncompressed_size);
        // account for goodput data
        stats->effective_data_bytes += dcResults.produced;
        stats->bytes_from_compressed_packets += dcResults.produced;
        stats->packets_received++;
    } else {
        // decompression results was expected, add information to get goodput
        printf("Decompression successful: %u bytes\n", dcResults.produced);
        stats->effective_data_bytes += dcResults.produced;
        stats->bytes_from_compressed_packets += dcResults.produced;
        stats->packets_received++;
    }
    
cleanup:
    if (srcData) qaeMemFreeNUMA((void**)&srcData);
    if (dstData) qaeMemFreeNUMA((void**)&dstData);
    if (pFlatSrc) qaeMemFreeNUMA((void**)&pFlatSrc);
    if (pFlatDst) qaeMemFreeNUMA((void**)&pFlatDst);
    if (srcMetaData) qaeMemFreeNUMA((void**)&srcMetaData);
    if (dstMetaData) qaeMemFreeNUMA((void**)&dstMetaData);
    if (pSrcBuffer) qaeMemFreeNUMA((void**)&pSrcBuffer);
    if (pDstBuffer) qaeMemFreeNUMA((void**)&pDstBuffer);
    
    return 0;
}

int main(int argc, char *argv[])
{
    const char *host = "127.0.0.1";
    int port = 9999;
    qat_decompress_ctx_t ctx = {0};
    stats_t stats = {0};
    int sockfd;
    int compression_level = 1; 

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-l") == 0 && i + 1 < argc) {
            compression_level = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
            host = argv[++i];
        } else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        }
    }
    
    printf("=== QAT Decompression Consumer (QATlib) ===\n\n");
    
    if (qat_decompress_init(&ctx, compression_level) < 0) {
        return 1;
    }
    
    sockfd = connect_to_server(host, port);
    if (sockfd < 0) {
        cpaDcStopInstance(ctx.dcInstance);
        icp_sal_userStop();
        return 1;
    }
    
    printf("Receiving Mixed data...\n\n");
    
    // start timing
    gettimeofday(&stats.start_time, NULL);
    
    while (1) {
        uint32_t magic;
        
        /// peek packet to see if its compressed
        if (peek_magic(sockfd, &magic) < 0) {
            break;
        }
        
        if (magic == MAGIC_NUMBER) {
            // process compressed packet
            if (process_packet(&ctx, sockfd, &stats) < 0) {
                break;
            }
            stats.compressed_packets++;
        } else {
            // process uncompressed packet
            if (process_uncompressed_packet(sockfd, &stats) < 0) {
                break;
            }
        }
    }
    
    // end timing
    gettimeofday(&stats.end_time, NULL);
    double elapsed = (stats.end_time.tv_sec - stats.start_time.tv_sec) +
                     (stats.end_time.tv_usec - stats.start_time.tv_usec) / 1e6;
    
    // statistics printing 
    printf("\n FINAL STATISTICS n");
    printf("Test Duration: %.2f seconds\n\n", elapsed);
    
    printf("Packet Counts:\n");
    uint64_t total_packets = stats.compressed_packets + stats.uncompressed_packets;
    printf("  Total packets:         %lu\n", total_packets);
    printf("  Compressed packets:    %lu (%.1f%%)\n", 
           stats.compressed_packets,
           stats.compressed_packets * 100.0 / total_packets);
    printf("  Uncompressed packets:  %lu (%.1f%%)\n",
           stats.uncompressed_packets,
           stats.uncompressed_packets * 100.0 / total_packets);
    printf("  Decompression errors:  %lu\n\n", stats.decompress_errors);
    
    printf("Network-Level (Wire) Bytes:\n");
    printf("  Total received:        %lu bytes (%.2f MB)\n", 
           stats.bytes_received_on_wire, 
           stats.bytes_received_on_wire / (1024.0 * 1024.0));
    printf("  Compressed payload:    %lu bytes (%.2f MB)\n",
           stats.compressed_bytes_on_wire,
           stats.compressed_bytes_on_wire / (1024.0 * 1024.0));
    printf("  Uncompressed payload:  %lu bytes (%.2f MB)\n\n",
           stats.uncompressed_bytes_on_wire,
           stats.uncompressed_bytes_on_wire / (1024.0 * 1024.0));
    
    printf("Effective Data (Uncompressed):\n");
    printf("  Total data received:   %lu bytes (%.2f MB)\n",
           stats.effective_data_bytes,
           stats.effective_data_bytes / (1024.0 * 1024.0));
    printf("    From compressed:     %lu bytes (%.2f MB)\n",
           stats.bytes_from_compressed_packets,
           stats.bytes_from_compressed_packets / (1024.0 * 1024.0));
    printf("    From uncompressed:   %lu bytes (%.2f MB)\n\n",
           stats.bytes_from_uncompressed_packets,
           stats.bytes_from_uncompressed_packets / (1024.0 * 1024.0));
    
    // cleanup session and socket
    close(sockfd);
    if (ctx.sessionHandle) {
        cpaDcRemoveSession(ctx.dcInstance, ctx.sessionHandle);
    }
    if (ctx.sessionMemory) {
        qaeMemFreeNUMA(&ctx.sessionMemory);
    }
    cpaDcStopInstance(ctx.dcInstance);
    icp_sal_userStop();
    
    printf("\nConsumer shutdown complete\n");
    return 0;
}