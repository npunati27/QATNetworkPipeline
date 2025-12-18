// Fast receiver - just discards data to test network throughput
// Compile: gcc -o fast_receiver fast_receiver.c

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <stdint.h>
#include <errno.h>

typedef struct {
    uint64_t bytes_received;
    uint64_t packets_received;
    struct timeval start_time;
} stats_t;

int connect_to_server(const char *host, int port)
{
    int sockfd;
    struct sockaddr_in server_addr;
    
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed");
        return -1;
    }
    
    // Increase receive buffer
    int rcvbuf = 64 * 1024 * 1024;  // 64 MB
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf)) < 0) {
        perror("Failed to set SO_RCVBUF");
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
    
    printf("Connected to %s:%d\n", host, port);
    return sockfd;
}

int main(int argc, char *argv[])
{
    const char *host = "192.168.100.3";
    int port = 9999;
    int sockfd;
    stats_t stats = {0};
    uint8_t buffer[1024 * 1024];  // 1 MB buffer
    
    // Parse args
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0 && i + 1 < argc) {
            host = argv[++i];
        } else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        }
    }
    
    printf("=== Fast Discard Receiver ===\n");
    printf("This receiver just discards all data as fast as possible\n\n");
    
    sockfd = connect_to_server(host, port);
    if (sockfd < 0) {
        return 1;
    }
    
    gettimeofday(&stats.start_time, NULL);
    
    uint64_t last_report_bytes = 0;
    time_t last_report_time = time(NULL);
    
    printf("Receiving data... (press Ctrl+C to stop)\n\n");
    
    while (1) {
        // Just read as much as possible and discard
        ssize_t n = recv(sockfd, buffer, sizeof(buffer), 0);
        
        if (n <= 0) {
            if (n == 0) {
                printf("\nConnection closed by sender\n");
            } else {
                perror("\nrecv failed");
            }
            break;
        }
        
        stats.bytes_received += n;
        stats.packets_received++;
        
        // Report every 5 seconds
        time_t now = time(NULL);
        if (now - last_report_time >= 5) {
            uint64_t bytes_in_period = stats.bytes_received - last_report_bytes;
            double mbps = (bytes_in_period * 8.0) / (1000000.0 * (now - last_report_time));
            double MB_per_sec = bytes_in_period / (1024.0 * 1024.0 * (now - last_report_time));
            
            printf("Throughput: %.2f Mbps (%.2f MB/s) | Total: %.2f MB | Packets: %lu\n",
                   mbps,
                   MB_per_sec,
                   stats.bytes_received / (1024.0 * 1024.0),
                   stats.packets_received);
            
            last_report_bytes = stats.bytes_received;
            last_report_time = now;
        }
    }
    
    // Final stats
    struct timeval end_time;
    gettimeofday(&end_time, NULL);
    double elapsed = (end_time.tv_sec - stats.start_time.tv_sec) +
                     (end_time.tv_usec - stats.start_time.tv_usec) / 1e6;
    
    printf("\n=== FINAL STATISTICS ===\n");
    printf("Duration:       %.2f seconds\n", elapsed);
    printf("Total received: %.2f MB\n", stats.bytes_received / (1024.0 * 1024.0));
    printf("Recv calls:     %lu\n", stats.packets_received);
    printf("Avg throughput: %.2f Mbps (%.2f MB/s)\n",
           (stats.bytes_received * 8.0) / (1000000.0 * elapsed),
           stats.bytes_received / (1024.0 * 1024.0 * elapsed));
    
    close(sockfd);
    return 0;
}
