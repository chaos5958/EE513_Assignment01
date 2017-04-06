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
#include <unistd.h>

#define MAX_EVENT_NUM 10000
#define QUEUE_SIZE 10000
#define WORKER_THREAD_NUM 4 

//Client information 
#define CLIENT_INFO_MAX 100000
#define CLIENT_INFO_ARR_SIZE 10
#define CLIENT_INFO_NUM (CLIENT_INFO_MAX / CLIENT_INFO_ARR_SIZE)
int enqueue_task(int, int);
int dequeue_task(int);
void *worker_func(void *args);

int byte_cnt = 0; /* counts total bytes received by server */
pthread_cond_t *empty_arr; /* condition variable to wake up worker threads when input comes */
pthread_cond_t *full_arr; /* condition variable to wake up super threads when queues have an empty entry */ 
pthread_mutex_t *mutex_arr; /* mutext for prevent racing condition between the acceptor thread and the worker thread (put/get into/from the queue) */
pthread_mutex_t shared_mutex; /* mutex between worker threads */
int *count_arr; /* managing how many number of tasks in the worker thread queue */
int **accept_queue; /* worker threads's queue */
int handler_fd[HANDLER_NUM];
int client_num = CLIENT_INFO_NUM;
host_info_t handelrinfo_arr[HANDLER_NUM];
client_info_t **clientinfo_arr;

/*************************************************************
 * FUNCTION NAME: enqueue_task                                         
 * PARAMETER: 1)index: worker thread's index 2)connf: accepted file descriptor to be enqueued                                              
 * PURPOSE: enqueue a task into a worker thread queue 
 ************************************************************/
int enqueue_task(int index, int connfd)
{
    int trylock_result;

    pthread_mutex_lock(&mutex_arr[index]);

    while(count_arr[index] == QUEUE_SIZE)
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
    int param_opt;
    bool is_thread = 0;
    int listenfd, connfd, port, num_thread, i, enqueue_result; 
    int *ids;
    int worker_index = 0;
    int count = 0;
    pthread_t *pthread_arr;

    int epoll_fd;
    int ssn;
    int ec;
    struct epoll_event event;
    struct epoll_event *events;

    //Process input arguments
    while((param_opt = getopt(argc, argv, "t:")) != -1)
    {
        switch(param_opt)
        {
            case 't':
                is_thread = true;
                num_thread = atoi(optarg);

            default:
            {
                fprintf(stderr, "usage: %s -t [thread_num] (t: optinal)\n", argv[0]);
                exit(1);
            }
        }
    }

    if(is_thread == 0)
        num_thread = WORKER_THREAD_NUM;

    //initializing data 
    empty_arr = (pthread_cond_t *)malloc(num_thread * sizeof(pthread_cond_t));
    full_arr = (pthread_cond_t *)malloc(num_thread * sizeof(pthread_cond_t));
    mutex_arr = (pthread_mutex_t *)malloc(num_thread * sizeof(pthread_mutex_t));
    count_arr = (int *)malloc(num_thread * sizeof(int));
    pthread_arr = (pthread_t *)malloc(num_thread * sizeof(pthread_t));
    ids = (int *)malloc(num_thread * sizeof(int));
    accept_queue = (int **)malloc(num_thread * sizeof(int *));
    clientinfo_arr = (client_info_t **)malloc(CLIENT_INFO_ARR_SIZE * sizeof(client_info_t *));
    clientinfo_arr[0] = (client_info_t *)malloc(CLIENT_INFO_NUM * sizeof(client_info_t));

    for (i = 0; i < num_thread; i++) {
        ids[i] = i;
        pthread_cond_init(&empty_arr[i], NULL);
        pthread_cond_init(&full_arr[i], NULL);
        pthread_mutex_init(&mutex_arr[i], NULL);
        accept_queue[i] = (int *)malloc(QUEUE_SIZE * sizeof(int));
        count_arr[i] = -1;
    }
    pthread_mutex_init(&shared_mutex, NULL);

    for (i = 0; i < CLIENT_INFO_NUM; i++) {
        clientinfo_arr[0][i].is_live = false;
        pthread_mutex_init(&clientinfo_arr[0][i].mutex, NULL);
    }

    //creating worker threadds
    for (i = 0; i < num_thread; i++) {
        int thread_id = pthread_create(&pthread_arr[i], NULL, &worker_func, (void *)&ids[i]);
        if(thread_id == -1)
        {
            perror("thread creation failed");
            abort();
        }
    }

    //listen fd as non-blocking 
    listenfd = Open_listenfd(LB_PORT);
    ssn = set_socket_nonblocking(listenfd);
    if(ssn == -1)
        abort();

    //Connect to handlers 
    getHandlersInfo(handelrinfo_arr);    
    for (i = 0; i < HANDLER_NUM; i++) {
        //TODO
        //handler_fd[i] = Open_clientfd(handelrinfo_arr[i].ip, handelrinfo_arr[i].port);
    }

    //epoll creation
    epoll_fd = epoll_create1(0);
    if(epoll_fd == -1)
    {
        perror("epoll_create1(0)");
        abort();
    }
    
    //Register a listnen fd and STDIN
    event.data.fd = listenfd;
    event.events = EPOLLIN | EPOLLET;
    ec = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listenfd, &event);

    event.data.fd = STDIN_FILENO;
    ec = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, STDIN_FILENO, &event);
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
        //printf("epoll event_num: %d\n", event_num);

        for (i = 0; i < event_num; i++) {
            //Handle epoll error
            if((events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP))
            {
                fprintf(stderr, "epoll error\n");
                close(events[i].data.fd);
                continue;
            }
            else if(events[i].events & EPOLLRDHUP)
            {
                close(events[i].data.fd);
                printf("client_close\n");
                continue;
            }
            //Handle a listen socket
            else if (events[i].data.fd == listenfd)
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
                    event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
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
                //printf("enqueue connfd %d to worker %d\n", events[i].data.fd, worker_index);
                /*distribute accepted file descriptors to the worker threads*/
                enqueue_result = enqueue_task(worker_index, events[i].data.fd); 

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

void *worker_func(void *args)
{
    int connfd;
    int worker_idx;
    int handler_idx = 0;
    int i, j, cnt, count;
    bool ih;
    data_hdr_t *hdr;
    char msg[sizeof(data_hdr_t) + KEY_MAX + VALUE_MAX];
    char cli_buf[CLI_BUF_SIZE];
    
    hdr = (data_hdr_t *)msg;
    worker_idx = *(int *)(args); 

    printf("thread %d work starts\n", worker_idx);
    while(1)
    {
        connfd = dequeue_task(worker_idx);

        printf("[thread %d] connfd %d is dequeued\n", worker_idx, connfd);

        //Handle a LB CLI command
        if(connfd == STDIN_FILENO)
        {
            memset(cli_buf, 0, sizeof(cli_buf));
            fgets(cli_buf, sizeof(cli_buf), stdin);

            if(strcmp(cli_buf, "list\n") == 0)
            { 
                 for (i = 0; i < HANDLER_NUM; i++)
                    fprintf(stdout, "%d/%s/%d\n", i, handelrinfo_arr[i].ip, handelrinfo_arr[i].port); 
            }
            else
            {
                fprintf(stderr, "invalid LB cli\n");
                continue;
            }
        }
        //Handle a client request or a handler response
        else
        {
            //Recieve message
            memset(msg, 0, sizeof(msg));
            cnt = 0, count = 0;

            if((cnt += read(connfd, msg, sizeof(data_hdr_t))) != sizeof(data_hdr_t))
            {
                fprintf(stderr, "loadbalancer read: fail to read header\n");
                continue;
            }

            if((cnt += read(connfd, msg + cnt, hdr->key_len)) != sizeof(data_hdr_t) + hdr->key_len)
            {
                fprintf(stderr, "loadbalancer read: fail to read key\n");
                continue;
            }
            
            if(hdr->value_len > 0)
            {
                if((cnt += read(connfd, msg + cnt, hdr->value_len)) != sizeof(data_hdr_t) + hdr->key_len + hdr->value_len)
                {
                    fprintf(stderr, "loadbalancer read: fail to read value");
                    continue;
                }
            } 

            /* Client request */
            if(hdr->code == 0)
            {
                //Register (client_id, fd) to use later in handle a reponse from a handler
                int lock;
                bool is_success = false;
                for (i = 0; i < CLIENT_INFO_ARR_SIZE; i++) {
                    //Expand an array which holds client information
                    while(clientinfo_arr[i] == NULL)
                    {
                        pthread_mutex_lock(&shared_mutex);
                        clientinfo_arr[i] = (client_info_t *)malloc(CLIENT_INFO_NUM * sizeof(client_info_t));
                        int k;
                        for (k = 0; k < CLIENT_INFO_NUM; k++) {
                            pthread_mutex_init(&clientinfo_arr[i][k].mutex, NULL);
                            clientinfo_arr[i][k].is_live; 
                        }
                        pthread_mutex_unlock(&shared_mutex);
                    }
                    
                    for (j = 0; j < CLIENT_INFO_NUM; j++) {
                        if(!clientinfo_arr[i][j].is_live) 
                        {
                            lock = pthread_mutex_trylock(&clientinfo_arr[i][j].mutex);
                            if(lock == 0)
                            {
                                clientinfo_arr[i][j].id = hdr->client_id;
                                clientinfo_arr[i][j].fd = connfd;
                                clientinfo_arr[i][j].is_live = true;
                                is_success = true;
                                goto double_for_exit;
                            }
                            else
                                continue;

                        }    
                    }

                }

double_for_exit:

                if(!is_success)
                {
                    fprintf(stderr, "client information array is full\n"); 
                }

                if(send(handler_fd[handler_idx], msg, cnt, 0) == -1)
                {
                    perror("[LB] send to client error");
                    continue;
                }

                handler_idx = (handler_idx + 1) % HANDLER_NUM;
            }
            /* Handler response */
            else
            {
                //Find a client which id is 'id'
                int lock;
                bool is_success = false;
                int client_fd = -1;
                for (i = 0; i < CLIENT_INFO_ARR_SIZE; i++) {
                    if(clientinfo_arr[i] == NULL)
                    {
                        fprintf(stderr, "client fd disappear\n");
                        continue;
                    }
                    for (j = 0; j < CLIENT_INFO_NUM; j++) {
                        if(clientinfo_arr[i][j].is_live && (hdr->client_id == clientinfo_arr[i][j].id)) 
                        {
                            client_fd = clientinfo_arr[i][j].fd; 
                            clientinfo_arr[i][j].is_live = false;
                            break;
                        }    
                    }
                }

                //Send a message to a client
                if(send(client_fd, msg, cnt, MSG_NOSIGNAL) == -1)
                {
                    perror("[LB] send to client error");
                    continue;
                }
            }
        }
    }
}
