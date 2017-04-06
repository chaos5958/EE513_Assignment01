/* Reference * 1. http://csapp.cs.cmu.edu/2e/ics2/code/conc/echoservers.c
 * (code related to use select function)
 */

#include "csapp.h"
#include "network.h" 
#include <semaphore.h>
#include <stdbool.h>
#include <sys/time.h>
#include "protocol.h"
#include <sys/epoll.h>

#define MAX_EVENT_NUM 10000
#define LISTEN_QUEUE_NUM 100
#define MAX_PORT_LEN 6
#define LOG_MSG_LEN 100
#define WORKER_THREAD_NUM 4 
#define ACCEPT_NUM 10000 

typedef struct { /* represents a pool of connected descriptors */ //line:conc:echoservers:beginpool
    int maxfd;        /* largest descriptor in read_set */   
    fd_set read_set;  /* set of all active descriptors */
    fd_set ready_set; /* subset of descriptors ready for reading  */
    int nready;       /* number of ready descriptors from select */   
    int maxi;         /* highwater index into client array */
    int clientfd[FD_SETSIZE];    /* set of active descriptors */
    rio_t clientrio[FD_SETSIZE]; /* set of active read buffers */
} pool; 

void init_pool(int listenfd, pool *p);
void add_client(int connfd, pool *p);
void check_clients(pool *p, int*, int);
int enqueue_task(int, int);
int dequeue_task(int);
void *distribute_work(void *args);

int byte_cnt = 0; /* counts total bytes received by server */
pthread_cond_t *empty_arr; /* condition variable to wake up worker threads when input comes */
pthread_cond_t *full_arr; /* condition variable to wake up super threads when queues have an empty entry */ 
pthread_mutex_t *mutex_arr; /* mutext for prevent racing condition between the acceptor thread and the worker thread (put/get into/from the queue) */
int *count_arr; /* managing how many number of tasks in the worker thread queue */
int **accept_queue; /* worker threads's queue */

/*************************************************************
 * FUNCTION NAME: enqueue_task                                         
 * PARAMETER: 1)index: worker thread's index 2)connf: accepted file descriptor to be enqueued                                              
 * PURPOSE: enqueue a task into a worker thread queue 
 ************************************************************/
int enqueue_task(int index, int connfd)
{
    int trylock_result;

    pthread_mutex_lock(&mutex_arr[index]);

    while(count_arr[index] == ACCEPT_NUM)
    {
        pthread_cond_wait(&full_arr[index], &mutex_arr[index]);
    }
    count_arr[index]++; 
    accept_queue[index][count_arr[index]] = connfd; 
    pthread_cond_signal(&empty_arr[index]);
    pthread_mutex_unlock(&mutex_arr[index]);

    return 0;
}

/*************************************************************
 * FUNCTION NAME: dequeue_task                                         
 * PARAMETER: 1)index: worker thread's index                                              
 * PURPOSE: dequeue a task from a worker thread queue
 ************************************************************/
int dequeue_task(int index)
{
    int dequeue_fd;

    pthread_mutex_lock(&mutex_arr[index]);
    while (count_arr[index] == -1) 
    {
        pthread_cond_wait(&empty_arr[index], &mutex_arr[index]); 
    }
    dequeue_fd = accept_queue[index][count_arr[index]];
    count_arr[index]--;
    pthread_cond_signal(&full_arr[index]);
    pthread_mutex_unlock(&mutex_arr[index]);

    return dequeue_fd; 
}

int main(int argc, char **argv)
{
    int listenfd, connfd, port, num_thread, i, enqueue_result; 
    int *ids;
    int worker_index = 0;
    int count = 0;
    pthread_t *pthread_arr;
    static pool pool; 
    struct timeval timeout;
     

    int epoll_fd;
    int ssn;
    int ec;
    
    struct epoll_event event;
    struct epoll_event *events;

    if (argc != 1 && argc != 2) {
        fprintf(stderr, "usage: %s <thread num: defualt 4>\n", argv[0]);
        exit(0);
    }

    //set number of worker threads
    if (argc == 2)
        num_thread = atoi(argv[2]);
    else
        num_thread = WORKER_THREAD_NUM; 

    //initializing data used in the acceptor thread
    empty_arr = (pthread_cond_t *)malloc(num_thread * sizeof(pthread_cond_t));
    full_arr = (pthread_cond_t *)malloc(num_thread * sizeof(pthread_cond_t));
    mutex_arr = (pthread_mutex_t *)malloc(num_thread * sizeof(pthread_mutex_t));
    count_arr = (int *)malloc(num_thread * sizeof(int));
    pthread_arr = (pthread_t *)malloc(num_thread * sizeof(pthread_t));
    ids = (int *)malloc(num_thread * sizeof(int));
    accept_queue = (int **)malloc(num_thread * sizeof(int *));

    for (i = 0; i < num_thread; i++) {
        ids[i] = i;
        pthread_cond_init(&empty_arr[i], NULL);
        pthread_cond_init(&full_arr[i], NULL);
        pthread_mutex_init(&mutex_arr[i], NULL);
        accept_queue[i] = (int *)malloc(ACCEPT_NUM * sizeof(int));
        count_arr[i] = -1;
    }

    //creating worker threadds
    for (i = 0; i < num_thread; i++) {
        int thread_id = pthread_create(&pthread_arr[i], NULL, &distribute_work, (void *)&ids[i]);
        if(thread_id == -1)
        {
            perror("thread creation failed");
            abort();
        }
    }


    //listen fd as non-blocking 
    listenfd = Open_listenfd(LB_CLIENT_PORT);
    ssn = set_socket_nonblocking(listenfd);
    if(ssn == -1)
        abort();

    //Connect to handlers 
    

    //epoll creation
    //TODO: client->LB Handler->client seperation 
    //TODO: Handler connection
    epoll_fd = epoll_create1(0);
    if(epoll_fd == -1)
    {
        perror("epoll_create1(0)");
        abort();
    }
    
    event.data.fd = listenfd;
    event.events = EPOLLIN | EPOLLET;
    ec = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listenfd, &event);
    if(ec == -1)
    {
        perror("epoll_ctl");
        abort();
    }

    events = calloc(MAX_EVENT_NUM, sizeof event);
    
    while(1)
    {
        int event_num, i;
        event_num = epoll_wait(epoll_fd, events, MAX_EVENT_NUM, -1);
        printf("epoll event_num: %d\n", event_num);

        for (i = 0; i < event_num; i++) {
            //Handle epoll error
            if((events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP) || (!(events[i].events & EPOLLIN)))
            {
                fprintf(stderr, "epoll error\n");
                close(events[i].data.fd);
                continue;
            }
            //Handle a listen socket
            else if (listenfd == events[i].data.fd)
            {
                while(1)
                {
                    int connfd;
                    socklen_t clientlen = sizeof(struct sockaddr_in);
                    struct sockaddr_in clientaddr;
                    char s[INET_ADDRSTRLEN];

                    connfd = accept(listenfd, (struct sockaddr*)&clientaddr, &clientlen); 
                    if(connfd == -1)
                    {
                        if((errno == EAGAIN) || (errno == EWOULDBLOCK))
                            break;
                        else
                        {
                            perror("accept");
                            break;
                        }
                    }

                    inet_ntop(clientaddr.sin_family, &clientaddr.sin_addr, s, sizeof s);
                    fprintf(stdout, "LB>%s:%05u\n", s, clientaddr.sin_port);

                    ssn = set_socket_nonblocking(connfd);
                    if(ssn == -1)
                        abort();

                    event.data.fd = connfd;
                    event.events = EPOLLIN | EPOLLET;
                    ec  = epoll_ctl (epoll_fd, EPOLL_CTL_ADD, connfd, &event);
                    if(ec == -1)
                    {
                        perror("epoll_ctl");
                        abort();
                    }
 
                }
                continue;
            }
            else
            {
                /*distribute accepted file descriptors to the worker threads*/
                enqueue_result = enqueue_task(worker_index, connfd); 

                /* increase worker thread index to balance load */
                worker_index  = (worker_index + 1) % num_thread;
            }
        }
    }

    for (i = 0; i < num_thread; i++) {
       pthread_join(pthread_arr[i], NULL); 
    }

    return 0;
}

void *distribute_work(void *args)
{
    int connfd;
    int worker_idx;
    char recv_buf[MAX_BUF_SIZE];

    worker_idx = *(int *)(args); 

    printf("thread %d work starts\n", worker_idx);
    while(1)
    {
        connfd = dequeue_task(worker_idx);
        
        memset(recv_buf, 0, sizeof(recv_buf));
        memcpy(recv_buf, &worker_idx, sizeof(worker_idx)); 
    }
}


//TODO: client->LB Handler->client seperation 
//TODO: Handler connection

#if 0
/*************************************************************
 * FUNCTION NAME: child_proxy                                         
 * PARAMETER: 1)args: worker thread index                                              
 * PURPOSE: worker thread does following jobs 1) receive an http request from an client 2) parse it and extract an server address and an port number 3) send the request to a server 4) receive the response from a server and send it to an server 
 
 ************************************************************/
void *child_proxy(void *args)
{
    int numbytes = 0, numbytes_serv = 0;
    int cnt = 0;
    int connfd = 0, px_sockfd = 0;
    int rv;
    int worker_index;
    char recv_buf[MAX_BUF_SIZE];
    HTTP_REQUEST* http_request = NULL;
    struct addrinfo hints, *servinfo, *p;
    struct sockaddr_storage client_addr;
    bool is_working = false;

    worker_index = *(int *)(args);

    /*worker thread jobs*/
    while(1)
    {
find_work:
        //worker thread dequeue a task from the waiting queue
        if(!is_working)
        {
            //initialize the working thread
            memset(recv_buf, 0, sizeof(recv_buf));
            if(http_request != NULL)
            {
                free_http_req(http_request);
                http_request = NULL;
            }

            if(connfd != 0)
            {
                close(connfd);
                connfd = 0;
            }
            if(px_sockfd != 0)
            {
                close(px_sockfd);
                px_sockfd = 0;
            }
            cnt = 0;
            numbytes = 0;
            numbytes_serv = 0;

            //get the work from the waiting queue 
            connfd = dequeue_task(worker_index);
            is_working = true;
        }
        //worker thread has a task to handle
        else
        {
            //receive from the client
            cnt = recv(connfd, recv_buf + numbytes, MAX_BUF_SIZE, 0);
            if(cnt > 0)
                numbytes += cnt;
            else if(cnt == -1)
            {
                perror("worker thread recv from the client");
                is_working = false;
            }
            else if(cnt == 0)
                is_working = false;


            //client's http-request ends
            if(strstr(recv_buf, "\r\n\r\n") != NULL)
            {
                http_request = parse_http_request(recv_buf);

                if(http_request == NULL)
                {
                    is_working = false;
                    goto find_work;
                }

                //connect to a server
                if((rv = getaddrinfo(http_request->host_addr, http_request->host_port, &hints, &servinfo))  != 0)
                {
                    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
                    goto find_work;
                }

                for(p = servinfo; p !=  NULL; p = p->ai_next)
                {
                    if((px_sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
                    {
                        perror("proxy| client: socket");
                        continue;
                    }

                    if (connect(px_sockfd, p->ai_addr, p->ai_addrlen) == -1){
                        close (px_sockfd);
                        perror("pxoxy| clinet: connect");

                        if (p == p->ai_next)
                            break;
                        continue;
                    }
                    break;
                }

                if(p == NULL){
                    fprintf(stderr, "proxy| clinet: failed to connect\n");
                    is_working = false;
                    goto find_work;
                }
                
                //send the http-request to a server
                if((send (px_sockfd, recv_buf, sizeof(recv_buf), 0) == -1))
                {
                    perror ("proxy| send");
                    is_working = false;
                    goto find_work;
                }

                //receive the http-response from a server
                while(1)
                {
                    cnt = recv(px_sockfd, recv_buf, sizeof(recv_buf), 0);
                    if(cnt > 0)
                    {
                        
                        if((cnt = send(connfd, recv_buf, cnt, MSG_NOSIGNAL)) == -1)
                        {
                            perror("worker thread send to the client");
                            is_working = false;
                            goto find_work;
                        }
                    }
                    else if(cnt < 0)
                    {   
                        perror("worker thread recv from server");
                        break;
                    }
                    else if(cnt == 0)
                        break;

                }

                is_working = false;
            }
        }
    }
}
#endif
