#include "proxy_parse.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <errno.h>

#define MAX_CLIENTS 400     /* Maximum number of concurrent client requests that can be handled */

/* Structure representing a single node in the LRU (Least Recently Used) Cache linked list */
typedef struct cache_element cache_element;

struct cache_element {
    char *data;             /* Pointer to the cached HTTP response payload */
    int len;                /* Length of the cached response payload (in bytes) */
    char *url;              /* Key: full request URL used for cache lookup */
    time_t lru_time_track;  /* Timestamp of the last access (used for LRU eviction policy) */
    cache_element *next;    /* Pointer to the next node in the cache linked list */
};

/* ========================================================================= */
/*                       Cache Management Prototypes                         */
/* ========================================================================= */

/**
 * @brief Searches the LRU cache for a cached entry matching the given URL.
 * 
 * @param url The request URL to look up.
 * @return Pointer to the matching cache_element if found; NULL on a cache miss.
 */
cache_element* find(char *url);

/**
 * @brief Adds a new response payload to the LRU cache.
 * 
 * @param data Pointer to the HTTP response payload to cache.
 * @param size Length of the response payload in bytes.
 * @param url The request URL associated with this response.
 * @return 0 on success, or -1 on failure.
 */
int add_cache_element(char *data, int size, char *url);

/**
 * @brief Evicts the Least Recently Used (LRU) element from the cache to free space.
 */
void remove_cache_element();

/* ========================================================================= */
/*                       Server State & Synchronization                      */
/* ========================================================================= */

int port_number = 8080;             /* Default port on which the proxy server listens */
int proxy_socketId;                 /* Socket descriptor for the listening proxy server */

pthread_t tid[MAX_CLIENTS];         /* Thread ID array to track worker threads for clients */

/**
 * Semaphore to limit concurrent client connections to MAX_CLIENTS.
 * When the active client count reaches MAX_CLIENTS, subsequent client threads
 * will wait (sem_wait) until an existing connection finishes and signals (sem_post).
 */
sem_t seamaphore;

/**
 * Mutex lock used to ensure thread-safe access to the shared LRU cache.
 * Since multiple client threads can read, insert, or evict cache entries concurrently,
 * acquiring this lock prevents race conditions and ensures cache consistency.
 */
pthread_mutex_t lock;

cache_element* head;                /* Head pointer to the LRU cache linked list */
int cache_size;                     /* Current total memory size consumed by the cache (in bytes) */

/* ========================================================================= */
/*                               Main Function                               */
/* ========================================================================= */

int main(int argc, char *argv[]) {
    int client_socketId, client_len;             /* Client socket descriptor and address struct length */
    struct sockaddr_in server_addr, client_addr; /* Server and client IPv4 address structures */

    /* Verify command-line arguments */
    if (argc == 2) {
        port_number = atoi(argv[1]);
    } else {
        printf("Too few arguments\n");
        exit(1);
    }

    printf("Setting Proxy Server Port : %d\n", port_number);

    /*
     * Create proxy server's TCP listening socket using IPv4:
     * - AF_INET: IPv4 Internet protocols
     * - SOCK_STREAM: Sequenced, reliable, two-way connection-based byte streams (TCP)
     * - 0: OS chooses default protocol (IPPROTO_TCP)
     */
    proxy_socketId = socket(AF_INET, SOCK_STREAM, 0);
    if (proxy_socketId < 0) {
        perror("Failed to create socket.\n");
        exit(1);
    }

    /*
     * Enable SO_REUSEADDR socket option to reuse the local address/port immediately
     * after restart, avoiding "Address already in use" errors during TIME_WAIT state.
     */
    int reuse = 1;
    if (setsockopt(proxy_socketId, SOL_SOCKET, SO_REUSEADDR, (const char *)&reuse, sizeof(reuse)) < 0) {
        perror("setsockopt(SO_REUSEADDR) failed\n");
    }

    /*
     * Initialize concurrency synchronization primitives:
     * - seamaphore: initialized to MAX_CLIENTS (0 flag denotes shared between threads)
     * - lock: initialized with default attributes (NULL)
     */
    sem_init(&seamaphore, 0, MAX_CLIENTS);
    pthread_mutex_init(&lock, NULL);

    // Since C uses garbage values, initialize struct memory to 0
    // bzero comes from #include <strings.h>
    bzero((char *)&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_number); // Assigning port to the Proxy
    server_addr.sin_addr.s_addr = INADDR_ANY;   // Any available address assigned
    // Proxy:
    // "Don't restrict me to one particular local IP.
    // Listen on this port on all available local interfaces."
    
    // Binding the socket
    if (bind(proxy_socketId, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Port is not free\n");
        exit(1);
    }
    printf("Binding on port: %d\n", port_number);

    // Proxy socket listening to the requests
    int listen_status = listen(proxy_socketId, MAX_CLIENTS);
    // This changes the socket's role. This socket is a server socket. Start waiting for incoming TCP connection requests.
    if (listen_status < 0) {
        perror("Error while Listening !\n");
        exit(1);
    }

    int i = 0;                             // Iterator for thread_id (tid) and Accepted Client_Socket for each thread
    int Connected_socketId[MAX_CLIENTS];   // This array stores socket descriptors of connected clients

    // Infinite Loop for accepting connections
    while (1) {
        bzero((char *)&client_addr, sizeof(client_addr)); // Clears struct client_addr
        client_len = sizeof(client_addr);

        // Accepting the connections
        client_socketId = accept(proxy_socketId, (struct sockaddr *)&client_addr, (socklen_t *)&client_len); // Accepts connection
        if (client_socketId < 0) {
            fprintf(stderr, "Error in Accepting connection !\n");
            exit(1);
        } else {
            Connected_socketId[i] = client_socketId; // Storing accepted client into array
        }

        // Getting IP address and port number of client
        struct sockaddr_in *client_pt = (struct sockaddr_in *)&client_addr;
        struct in_addr ip_addr = client_pt->sin_addr;
        char str[INET_ADDRSTRLEN]; // INET_ADDRSTRLEN: Default ip address size
        inet_ntop(AF_INET, &ip_addr, str, INET_ADDRSTRLEN);
        printf("Client is connected with port number: %d and ip address: %s \n", ntohs(client_addr.sin_port), str);
        // printf("Socket values of index %d in main function is %d\n", i, client_socketId);
        pthread_create(&tid[i], NULL, thread_fn, (void *)&Connected_socketId[i]); // Creating a thread for each client accepted
        i++;
    }

    close(proxy_socketId); // Close socket
    return 0;
}