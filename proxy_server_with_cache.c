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

/* ========================================================================= */
/*                           Macro Definitions                               */
/* ========================================================================= */

#define MAX_BYTES 4096              /* Buffer size (4 KB) for network transmission */
#define MAX_CLIENTS 400             /* Maximum number of concurrent client requests */
#define MAX_SIZE 200 * (1 << 20)    /* Maximum total capacity of LRU cache (200 MB) */

/* ========================================================================= */
/*                       Data Structures & Types                             */
/* ========================================================================= */

/**
 * @brief Represents a single element in the LRU (Least Recently Used) cache linked list.
 */
typedef struct cache_element cache_element;

struct cache_element {
    char *data;             /* Pointer to the cached HTTP response payload */
    int len;                /* Length of the cached response payload in bytes */
    char *url;              /* Key: full request URL used for cache lookup */
    time_t lru_time_track;  /* Timestamp of last access (used for LRU eviction) */
    cache_element *next;    /* Pointer to the next node in the cache linked list */
};

/* ========================================================================= */
/*                       Function Prototypes                                 */
/* ========================================================================= */

/**
 * @brief Worker thread function to handle an individual client request.
 * 
 * @param socketNew Pointer to the client socket descriptor (int *).
 * @return NULL upon thread completion.
 */
void *thread_fn(void *socketNew);

/**
 * @brief Searches the LRU cache for a cached entry matching the given URL.
 * 
 * @param url The request URL to look up in the cache.
 * @return Pointer to matching cache_element if found; NULL on a cache miss.
 */
cache_element *find(char *url);

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
void remove_cache_element(void);

/**
 * @brief Validates the HTTP version string (supports HTTP/1.0 and HTTP/1.1).
 * 
 * @param msg The HTTP version string to validate (e.g., "HTTP/1.0").
 * @return 1 if the version is valid/supported; -1 otherwise.
 */
int checkHTTPversion(char *msg);

/**
 * @brief Sends standard HTTP error response to the client based on status code.
 * 
 * @param socket Socket descriptor connected to the client.
 * @param status_code HTTP status code (e.g., 400, 500).
 * @return Number of bytes sent, or -1 on failure.
 */
int sendErrorMessage(int socket, int status_code);

/**
 * @brief Handles HTTP GET requests: connects to the remote server, forwards request,
 *        relays response back to client, and stores response in cache.
 * 
 * @param clientSocket Socket descriptor of the connected client.
 * @param request Parsed request structure containing method, host, path, headers.
 * @param tempReq Raw client request string.
 * @return Number of bytes sent, or -1 on error.
 */
int handle_request(int clientSocket, ParsedRequest *request, char *tempReq);

/**
 * @brief Establishes a TCP connection to the remote origin server on the given port.
 * 
 * @param host_addr Host name or IP address string of the remote server.
 * @param port_num Port number of the remote server (default 80 for HTTP).
 * @return Socket file descriptor on success, or -1 on error.
 */
int connectRemoteServer(char *host_addr, int port_num);

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

cache_element *head;                /* Head pointer to the LRU cache linked list */
int cache_size;                     /* Current total memory size consumed by cache (in bytes) */

/* ========================================================================= */
/*                       Remote Request Handling                             */
/* ========================================================================= */

/**
 * @brief Handles HTTP GET requests: connects to the remote origin server,
 *        forwards the reconstructed request, streams the server response back
 *        to the client socket, and caches the complete response payload.
 *
 * @param clientSocket Socket descriptor of the connected client.
 * @param request Parsed request structure containing method, host, path, and headers.
 * @param tempReq Raw client request URL string (used as cache key).
 * @return 0 on success, or -1 on failure.
 */
int handle_request(int clientSocket, ParsedRequest *request, char *tempReq) {
    char *buf = (char *)malloc(sizeof(char) * MAX_BYTES);
    if (buf == NULL) {
        perror("Failed to allocate memory for request buffer\n");
        return -1;
    }

    // Reconstruct HTTP request line: "GET <path> <version>\r\n"
    strcpy(buf, "GET ");
    strcat(buf, request->path);
    strcat(buf, " ");
    strcat(buf, request->version);
    strcat(buf, "\r\n");

    size_t len = strlen(buf);

    // Set "Connection: close" to ensure remote server closes connection after responding
    if (ParsedHeader_set(request, "Connection", "close") < 0) {
        printf("Failed to set 'Connection: close' header\n");
    }

    /*
     * HTTP/1.1 requires a Host header on every request (allowing virtual hosting on same IP).
     * Defensively guarantee a Host header is present by setting it from request->host if missing.
     */
    if (ParsedHeader_get(request, "Host") == NULL) {
        if (ParsedHeader_set(request, "Host", request->host) < 0) {
            printf("Failed to set 'Host' header\n");
        }
    }

    // Unparse headers and append them to the request buffer
    if (ParsedRequest_unparse_headers(request, buf + len, (size_t)MAX_BYTES - len) < 0) {
        printf("Unparse headers failed\n");
    }

    // Default remote server port to 80 (standard HTTP) unless specified in request
    int server_port = 80;
    if (request->port != NULL) {
        server_port = atoi(request->port);
    }

    // Establish TCP connection to origin server
    int remoteSocketID = connectRemoteServer(request->host, server_port);
    if (remoteSocketID < 0) {
        free(buf);
        return -1;
    }

    // Forward the reconstructed HTTP request to the remote origin server
    int bytes_send = send(remoteSocketID, buf, strlen(buf), 0);
    bzero(buf, MAX_BYTES);

    // Receive origin server response and stream it to the client
    bytes_send = recv(remoteSocketID, buf, MAX_BYTES - 1, 0);
    char *temp_buffer = (char *)malloc(sizeof(char) * MAX_BYTES);
    if (temp_buffer == NULL) {
        perror("Failed to allocate memory for temp_buffer\n");
        free(buf);
        close(remoteSocketID);
        return -1;
    }

    int temp_buffer_size = MAX_BYTES;
    int temp_buffer_index = 0;

    while (bytes_send > 0) {
        // Relay received chunk to client socket
        bytes_send = send(clientSocket, buf, bytes_send, 0);
        if (bytes_send < 0) {
            perror("Error in sending data to client socket.\n");
            break;
        }

        // Accumulate received chunk into temp_buffer for LRU caching
        for (int i = 0; i < bytes_send / (int)sizeof(char); i++) {
            temp_buffer[temp_buffer_index] = buf[i];
            temp_buffer_index++;
        }

        // Dynamically expand buffer capacity for subsequent response chunks
        temp_buffer_size += MAX_BYTES;
        char *realloc_ptr = (char *)realloc(temp_buffer, temp_buffer_size);
        if (realloc_ptr == NULL) {
            perror("Failed to reallocate memory for temp_buffer\n");
            break;
        }
        temp_buffer = realloc_ptr;

        bzero(buf, MAX_BYTES);
        bytes_send = recv(remoteSocketID, buf, MAX_BYTES - 1, 0);
    }

    temp_buffer[temp_buffer_index] = '\0';
    free(buf);

    // Cache the complete response payload for future requests
    add_cache_element(temp_buffer, strlen(temp_buffer), tempReq);
    printf("Done\n");
    free(temp_buffer);

    close(remoteSocketID);
    return 0;
}









/* ========================================================================= */
/*                       Client Request Worker Thread                        */
/* ========================================================================= */

/**
 * @brief Worker thread routine to process incoming client HTTP requests.
 *
 * Workflow:
 * 1. Acquire semaphore slot to enforce MAX_CLIENTS concurrency limit.
 * 2. Receive HTTP request from client socket until "\r\n\r\n" is encountered.
 * 3. Check LRU cache for an existing response:
 *    - CACHE HIT: Transmit cached response chunks directly to client.
 *    - CACHE MISS: Parse request, validate method/version, forward via handle_request().
 * 4. Clean up allocated buffers, close sockets, and release semaphore slot.
 *
 * @param socketNew Pointer to the client socket descriptor integer.
 * @return NULL upon completion.
 */
void *thread_fn(void *socketNew) {
    // Acquire a semaphore slot; blocks if active connections reach MAX_CLIENTS
    sem_wait(&seamaphore);
    
    int p;
    sem_getvalue(&seamaphore, &p);
    printf("Semaphore value: %d\n", p);

    int *t = (int *)(socketNew);
    int socket = *t;                // Socket descriptor of the connected client
    int bytes_send_client;          // Number of bytes received from client / sent by handler
    int len;                        // Length of current data in buffer

    // Allocate 4KB buffer for reading client request
    char *buffer = (char *)calloc(MAX_BYTES, sizeof(char));
    if (buffer == NULL) {
        perror("Failed to allocate memory for client buffer\n");
        shutdown(socket, SHUT_RDWR);
        close(socket);
        sem_post(&seamaphore);
        return NULL;
    }

    bzero(buffer, MAX_BYTES);       // Zero out buffer memory

    // Receive initial chunk of the client HTTP request
    bytes_send_client = recv(socket, buffer, MAX_BYTES, 0);

    // HTTP headers terminate with "\r\n\r\n". Continue receiving until full headers arrive.
    while (bytes_send_client > 0) {
        len = strlen(buffer);
        if (strstr(buffer, "\r\n\r\n") == NULL) {
            bytes_send_client = recv(socket, buffer + len, MAX_BYTES - len, 0);
        } else {
            break;
        }
    }

    // Allocate heap memory for a copy of the request string (+1 for null-terminator '\0')
    char *tempReq = (char *)malloc(strlen(buffer) * sizeof(char) + 1);
    if (tempReq == NULL) {
        perror("Failed to allocate memory for tempReq\n");
        free(buffer);
        shutdown(socket, SHUT_RDWR);
        close(socket);
        sem_post(&seamaphore);
        return NULL;
    }

    for (size_t i = 0; i < strlen(buffer); i++) {
        tempReq[i] = buffer[i];
    }
    tempReq[strlen(buffer)] = '\0';

    // Check if the requested URL exists in the LRU cache
    struct cache_element *temp = find(tempReq);

    if (temp != NULL) {
        // -----------------------------------------------------------------
        // CACHE HIT: Send the cached response directly to client in chunks
        // -----------------------------------------------------------------
        int size = temp->len / sizeof(char);
        int pos = 0;
        char response[MAX_BYTES];

        while (pos < size) {
            bzero(response, MAX_BYTES);
            for (int i = 0; i < MAX_BYTES && pos < size; i++) {
                response[i] = temp->data[pos];
                pos++;
            }
            send(socket, response, MAX_BYTES, 0);
        }
        printf("Data retrieved from the Cache\n\n");
        printf("%s\n\n", response);
    } else if (bytes_send_client > 0) {
        // -----------------------------------------------------------------
        // CACHE MISS: Parse request and forward to the origin server
        // -----------------------------------------------------------------
        len = strlen(buffer);
        ParsedRequest *request = ParsedRequest_create();

        // Parse raw HTTP request into structured ParsedRequest object
        if (ParsedRequest_parse(request, buffer, len) < 0) {
            printf("Parsing failed\n");
            sendErrorMessage(socket, 400); // 400 Bad Request
        } else {
            bzero(buffer, MAX_BYTES);
            if (!strcmp(request->method, "GET")) {
                // Only process GET requests with valid host, path, and HTTP version
                if (request->host && request->path && (checkHTTPversion(request->version) == 1)) {
                    bytes_send_client = handle_request(socket, request, tempReq); // Forward GET request
                    if (bytes_send_client == -1) {
                        sendErrorMessage(socket, 500); // 500 Internal Server Error (origin server failure)
                    }
                } else {
                    sendErrorMessage(socket, 500);     // 500 Internal Server Error (invalid request fields)
                }
            } else {
                printf("This proxy currently supports only HTTP GET method\n");
            }
        }
        // Free dynamically allocated ParsedRequest structure
        ParsedRequest_destroy(request);
    } else if (bytes_send_client < 0) {
        perror("Error in receiving from client.\n");
    } else if (bytes_send_client == 0) {
        printf("Client disconnected!\n");
    }

    // Shut down bidirectional socket communication and close descriptor
    shutdown(socket, SHUT_RDWR);
    close(socket);

    // Free dynamically allocated request buffers
    free(buffer);
    free(tempReq);

    // Release semaphore slot so next waiting client thread can proceed
    sem_post(&seamaphore);

    sem_getvalue(&seamaphore, &p);
    printf("Semaphore post value: %d\n", p);

    return NULL;
}

/* ========================================================================= */
/*                               Main Function                               */
/* ========================================================================= */

int main(int argc, char *argv[]) {
    int client_socketId;                         /* Client socket descriptor */
    int client_len;                              /* Length of client address structure */
    struct sockaddr_in server_addr, client_addr; /* Server and client IPv4 address structures */

    /* Verify command-line arguments: port number is required */
    if (argc == 2) {
        port_number = atoi(argv[1]);
    } else {
        printf("Too few arguments\n");
        printf("Usage: %s <port_number>\n", argv[0]);
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

    /*
     * Configure proxy server address structure:
     * - AF_INET: IPv4 address family
     * - htons(port_number): Convert port number from host byte order to network byte order (Big-Endian)
     * - INADDR_ANY: Bind to all available local network interfaces
     */
    bzero((char *)&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_number);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    /* Bind the socket to the configured port and IP address */
    if (bind(proxy_socketId, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Port is not free\n");
        exit(1);
    }
    printf("Binding on port: %d\n", port_number);

    /*
     * Transition socket to listening mode:
     * Backlog queue size set to MAX_CLIENTS for incoming TCP connection requests.
     */
    int listen_status = listen(proxy_socketId, MAX_CLIENTS);
    if (listen_status < 0) {
        perror("Error while Listening !\n");
        exit(1);
    }

    int i = 0;                             /* Iterator for thread_id (tid) and client socket tracking */
    int Connected_socketId[MAX_CLIENTS];   /* Array storing socket descriptors of connected clients */

    /* Infinite loop to accept incoming client connections */
    while (1) {
        bzero((char *)&client_addr, sizeof(client_addr));
        client_len = sizeof(client_addr);

        /* Accept incoming connection from client */
        client_socketId = accept(proxy_socketId, (struct sockaddr *)&client_addr, (socklen_t *)&client_len);
        if (client_socketId < 0) {
            fprintf(stderr, "Error in Accepting connection !\n");
            exit(1);
        } else {
            Connected_socketId[i] = client_socketId;
        }

        /* Extract and display client IP address and port */
        struct sockaddr_in *client_pt = (struct sockaddr_in *)&client_addr;
        struct in_addr ip_addr = client_pt->sin_addr;
        char str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &ip_addr, str, INET_ADDRSTRLEN);
        printf("Client is connected with port number: %d and ip address: %s \n", ntohs(client_addr.sin_port), str);

        /* Spawn a worker thread to handle client request concurrently */
        pthread_create(&tid[i], NULL, thread_fn, (void *)&Connected_socketId[i]);
        i++;
    }

    close(proxy_socketId); /* Close listening socket */
    return 0;
}
