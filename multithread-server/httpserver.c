#include "asgn4_helper_funcs.h"
#include "connection.h"
#include "debug.h"
#include "request.h"
#include "response.h"
#include "queue.h"

#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <pthread.h>
#include <getopt.h>
#include <sys/file.h>
#include <assert.h>

#include <sys/stat.h>

#define DEFAULT_THREAD_COUNT 4;
#define RID                  "Request-Id"

//Global
queue_t *workQ = NULL;
const char *LOCK_FILE = "lockfile.lock";

//Function Declarations
void handle_connection(void);
void handle_get(conn_t *);
void handle_put(conn_t *);
void handle_unsupported(conn_t *);
void acquire_shared(int fd);
void acquire_exclusive(int fd);
int acquire_templock(void);
void release(int fd);
void init(int threads);

//Function Declarations
char *get_rid(conn_t *conn) {
    char *id = conn_get_header(conn, RID);
    //if RequestID is empty, default to 0
    if (id == NULL) {
        id = "0";
    }
    return id;
}

void auditLog(char *name, char *uri, char *id, int code) {
    // <Opers>,<URI>,<Status-Code>,<RequestID header value>\n
    fprintf(stderr, "%s,/%s,%d,%s\n", name, uri, code, id);
}

void usage(FILE *stream, char *exec) {
    fprintf(stream, "Usage: %s [-t threads] <port>\n", exec);
}

void acquire_exclusive(int fd) {
    int res = flock(fd, LOCK_EX);
    assert(res == 0);
    //debug("acquire_exclusive on %d", fd);
}

void acquire_shared(int fd) {
    int res = flock(fd, LOCK_SH);
    assert(res == 0);
    //debug("acquire_shared on %d", fd);
}

int acquire_templock(void) {
    int fd = open(LOCK_FILE, 0600);
    //debug("opened %d", fd);
    acquire_exclusive(fd);
    return fd;
}

void release(int fd) {
    //debug("release");
    int res = flock(fd, LOCK_UN);
    assert(res == 0);
}

int main(int argc, char **argv) {
    int option = 0;
    int threads = DEFAULT_THREAD_COUNT;
    pthread_t *threadIDs;

    if (argc < 2) {
        warnx("Wrong arguments: %s portNum", argv[0]);
        usage(stderr, argv[0]);
        return EXIT_FAILURE;
    }

    //parse the command line
    while ((option = getopt(argc, argv, "t:h")) != -1) {
        switch (option) {
        case 't':
            threads = strtol(optarg, NULL, 10);
            if (threads <= 0) {
                errx(EXIT_FAILURE, "Bad number of threads\n");
            }
            break;
        case 'h':
            usage(stdout, argv[0]);
            return EXIT_SUCCESS;
            break;
        default:
            usage(stderr, argv[0]);
            return EXIT_FAILURE;
            break;
        }
    }

    if (optind >= argc) {
        warnx("wrong arguments: %s port_num", argv[0]);
        usage(stderr, argv[0]);
        return EXIT_FAILURE;
    }

    char *endptr = NULL;
    size_t port = (size_t) strtoull(argv[optind], &endptr, 10);
    if (endptr && *endptr != '\0') {
        warnx("invalid port number: %s", argv[1]);
        return EXIT_FAILURE;
    }

    signal(SIGPIPE, SIG_IGN);
    Listener_Socket sock;
    if (listener_init(&sock, port) < 0) {
        warnx("Cannot open listener sock: %s", argv[0]);
        return EXIT_FAILURE;
    }

    threadIDs = malloc(sizeof(pthread_t) * threads);

    //Create new queue and tempfile
    workQ = queue_new(threads);
    creat(LOCK_FILE, 0600);

    for (int i = 0; i < threads; ++i) {
        int rc = pthread_create(threadIDs + i, NULL, (void *(*) (void *) ) handle_connection, NULL);
        if (rc != 0) {
            warnx("Cannot create %d pthreads", threads);
            return EXIT_FAILURE;
        }
    }

    while (1) {
        uintptr_t connfd = listener_accept(&sock);
        //debug("accepted %lu\n", connfd);
        queue_push(workQ, (void *) connfd);
    }

    queue_delete(&workQ);

    return EXIT_SUCCESS;
}

void handle_connection(void) {
    while (1) {
        uintptr_t connfd = 0;
        conn_t *conn = NULL;
        queue_pop(workQ, (void **) &connfd);
        //debug("popped off %lu", connfd);

        conn = conn_new(connfd);

        const Response_t *response = conn_parse(conn);
        if (response != NULL) {
            conn_send_response(conn, response);
        } else {
            const Request_t *request = conn_get_request(conn);
            if (request == &REQUEST_GET) {
                handle_get(conn);
            } else if (request == &REQUEST_PUT) {
                handle_put(conn);
            } else {
                handle_unsupported(conn);
            }
        }

        conn_delete(&conn);
        close(connfd);
    }
}

void handle_get(conn_t *conn) {
    //Create temp lock
    int tempLock = acquire_templock();

    //Get URI from Request line
    char *uri = conn_get_uri(conn);

    //Create Response type
    const Response_t *response = NULL;

    //Open file for reading
    int fdGet = open(uri, O_RDONLY);
    //If file DNE, return error
    if (fdGet < 0) {
        //Error Checking
        response = &RESPONSE_NOT_FOUND;
        goto out;
    }

    //Lock file for reading
    acquire_shared(fdGet);

    //Release Templock and Close it
    release(tempLock);
    close(tempLock);

    //Get file size
    struct stat sb;
    fstat(fdGet, &sb);
    off_t fileSize = sb.st_size;

    //Check if the file is directory
    if (stat(uri, &sb) == 0 && S_ISDIR(sb.st_mode)) {
        response = &RESPONSE_FORBIDDEN;
        goto out;
    }

    //send to file
    conn_send_file(conn, fdGet, fileSize);
    auditLog("GET", uri, get_rid(conn), response_get_code(&RESPONSE_OK));

out:
    //Send a Response if Response is not NULL
    if (response != NULL) {
        conn_send_response(conn, response);
        auditLog("GET", uri, get_rid(conn), response_get_code(response));
    }

    //Lastly, release any remaining Locks
    if (fdGet > 0) {
        release(fdGet);
        close(fdGet);
    } else {
        release(tempLock);
        close(tempLock);
    }

    return;
}

void handle_put(conn_t *conn) {
    //Create temp lock
    int tempLock = acquire_templock();

    char *uri = conn_get_uri(conn);
    //debug("PUT %s", uri);

    //Create Response type
    const Response_t *response = NULL;

    //First, check if file already exists
    bool fileExists = access(uri, F_OK) == 0;

    //Now, Open file or Create if file DNE
    int fdPut = open(uri, O_CREAT | O_WRONLY, 0600);
    if (fdPut < 0) {
        response = &RESPONSE_INTERNAL_SERVER_ERROR;
        goto out;
    }

    //Lock File for Writing
    acquire_exclusive(fdPut);

    //Release Templock and Close it
    release(tempLock);
    close(tempLock);

    //Now, truncate the file
    int rc = ftruncate(fdPut, 0);
    assert(rc == 0);

    //Check if the file is directory
    struct stat sb;
    if (stat(uri, &sb) == 0 && S_ISDIR(sb.st_mode)) {
        response = &RESPONSE_FORBIDDEN;
        goto out;
    }

    //Recieve the file
    conn_recv_file(conn, fdPut);

    if (response == NULL && fileExists == true) {
        conn_send_response(conn, &RESPONSE_OK);
        auditLog("PUT", uri, get_rid(conn), response_get_code(&RESPONSE_OK));
    }

    if (response == NULL && fileExists == false) {
        conn_send_response(conn, &RESPONSE_CREATED);
        auditLog("PUT", uri, get_rid(conn), response_get_code(&RESPONSE_CREATED));
    }

out:

    //Send a Response if Response is not NULL
    if (response != NULL) {
        conn_send_response(conn, response);
        auditLog("PUT", uri, get_rid(conn), response_get_code(response));
    }

    //Lastly, release any remaining Locks
    if (fdPut > 0) {
        release(fdPut);
        close(fdPut);
    } else {
        release(tempLock);
        close(tempLock);
    }

    return;
}

void handle_unsupported(conn_t *conn) {
    //debug("Unsupported request");
    conn_send_response(conn, &RESPONSE_NOT_IMPLEMENTED);
    return;
}
