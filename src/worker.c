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
#define WORKER_INFO_MAX 100000
#define WORKER_INFO_ARR_SIZE 10
#define WORKER_INFO_NUM (WORKER_INFO_MAX / WORKER_INFO_ARR_SIZE)

int enqueue_task(int, int);
int dequeue_task(int);
void *worker_func(void *args);
void initialize_worker(int, int *, pthread_t *);
int handleCLI();
int receiveMsg(char *msg, int connfd);
int handleHandlerReq(char *msg, int);
int handleshow(char *);
int handlelist(char *);
int receiveHandlerMsg(char *, int);
int findKey(char *);
int handlePUT(char *);
int handleGET(char *);
int handleDEL(char *);

int byte_cnt = 0; /* counts total bytes received by server */
pthread_cond_t *empty_arr; /* condition variable to wake up worker threads when input comes */
pthread_cond_t *full_arr; /* condition variable to wake up super threads when queues have an empty entry */ 
pthread_mutex_t *mutex_arr; /* mutext for prevent racing condition between the acceptor thread and the worker thread (put/get into/from the queue) */
pthread_mutex_t shared_mutex; /* mutex between worker threads */
int *count_arr; /* managing how many number of tasks in the worker thread queue */
int **accept_queue; /* worker threads's queue */
int handler_fd[HANDLER_NUM - 1];
int client_num = WORKER_INFO_NUM;
host_info_t handlerinfo_arr[HANDLER_NUM];
host_info_t workerinfo_arr[WORKER_NUM];
worker_data_t **workerdata_arr;


int main(int argc, char **argv)
{
    int param_opt;
    bool is_thread = 0, is_worker = 0;
    int listenfd, connfd, port, num_thread, i, enqueue_result; 
    int *ids;
    int worker_index = 0;
    int count = 0;
    pthread_t *pthread_arr;
    int epoll_fd;
    int ssn, ec;
    struct epoll_event event;
    struct epoll_event *events;
    int worker_idx;
    int conn_handler_num = 0; 
    socklen_t clientlen = sizeof(struct sockaddr_in);
    struct sockaddr_in clientaddr;

    //Process input arguments
    while((param_opt = getopt(argc, argv, "t:n:")) != -1)
    {
        switch(param_opt)
        {
            case 't':
                is_thread = true;
                num_thread = atoi(optarg);
                break;
            
            case 'n':
                is_worker = true;
                worker_idx = atoi(optarg);
                break;

            default:
            {
                fprintf(stderr, "usage: %s -n [worker_idx] -t [thread_num] (t: optinal)\n", argv[0]);
                exit(1);
            }
        }
    }

    if(is_thread == 0)
        num_thread = WORKER_THREAD_NUM;

    if(is_worker == 0)
    {
        fprintf(stderr, "usage: %s -t [thread_num] (t: optinal)\n", argv[0]);
        exit(1);
    }

    //Initialize several environment variables (most related to threads) 
    initialize_worker(num_thread, ids, pthread_arr);
    getHandlersInfo(handlerinfo_arr);    
    getWorkersInfo(workerinfo_arr);

    //Create a socket, then, bind and listen to a port 
    listenfd = Open_listenfd(workerinfo_arr[worker_idx].port);

    //Set listen socket as non-blocking
    ssn = set_socket_nonblocking(listenfd);
    if(ssn == -1)
        abort();

    //Create an epoll instance
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
        printf("epoll event_num: %d\n", event_num);

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
                    fprintf(stdout, "WB>%s:%05u\n", s, clientaddr.sin_port);

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
            //Handle other connect sockets 
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
    int i, j, cnt, count, result;
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
            result = handleCLI();

            if(result == -1)
                continue;

        }
        //Handle a worker response
        else
        {
            result = receiveMsg(msg, connfd);

            if(result == -1)
                continue;

            /* Client request */
            if(hdr->code == 0)
            {
                result = handleHandlerReq(msg, connfd);
                
                if(result == -1)
                    continue;
            }
            else
            {
                fprintf(stderr, "wrong request comes to a worker\n");
                continue;
            }
        }
    }
}

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

void initialize_worker(int num_thread, int *ids, pthread_t *pthread_arr)
{
    int i, j;

    empty_arr = (pthread_cond_t *)malloc(num_thread * sizeof(pthread_cond_t));
    full_arr = (pthread_cond_t *)malloc(num_thread * sizeof(pthread_cond_t));
    mutex_arr = (pthread_mutex_t *)malloc(num_thread * sizeof(pthread_mutex_t));
    count_arr = (int *)malloc(num_thread * sizeof(int));
    pthread_arr = (pthread_t *)malloc(num_thread * sizeof(pthread_t));
    ids = (int *)malloc(num_thread * sizeof(int));
    accept_queue = (int **)malloc(num_thread * sizeof(int *));
    workerdata_arr = (worker_data_t **)malloc(WORKER_INFO_ARR_SIZE * sizeof(worker_data_t *));

    workerdata_arr[0] = (worker_data_t *)malloc(WORKER_INFO_NUM * sizeof(worker_data_t));
    for (j = 0; j < WORKER_INFO_NUM; j++) {
        workerdata_arr[0][j].is_live = false;
    }


    for (i = 0; i < num_thread; i++) {
        ids[i] = i;
        pthread_cond_init(&empty_arr[i], NULL);
        pthread_cond_init(&full_arr[i], NULL);
        pthread_mutex_init(&mutex_arr[i], NULL);
        accept_queue[i] = (int *)malloc(QUEUE_SIZE * sizeof(int));
        count_arr[i] = -1;
    }
    pthread_mutex_init(&shared_mutex, NULL);

    //creating worker threadds
    for (i = 0; i < num_thread; i++) {
        int thread_id = pthread_create(&pthread_arr[i], NULL, &worker_func, (void *)&ids[i]);
        if(thread_id == -1)
        {
            perror("thread creation failed");
            abort();
        }
    }
}

int handleCLI()
{
    char cli_buffer[CLI_BUF_SIZE];
    int result = -1;
    char *ptr;

    memset(cli_buffer, 0, sizeof(cli_buffer));

    fgets(cli_buffer, sizeof(cli_buffer), stdin);
    ptr = strtok(cli_buffer, " ");
    checkLineSeparator(cli_buffer);
    //printf("ptr: %s\n", ptr);

    if(strcmp(ptr, "list") == 0)
        result = handlelist(ptr); 
    else if(strcmp(ptr, "show") == 0)
        result = handleshow(ptr);
    
    if(result == -1)
    {
       fprintf(stderr, "invalid cli\n");
       return -1;
    }

    return 0;
}


int handlelist(char *ptr)
{
    int i, j, k;

    if((ptr = strtok(NULL, " ")) != NULL)
        return -1;

    pthread_mutex_lock(&shared_mutex);
    for (j = 0; j < WORKER_INFO_ARR_SIZE; j++) {
        if(workerdata_arr[j] != NULL){
            for (k = 0; k < WORKER_INFO_NUM; k++) {
                if(workerdata_arr[j][k].is_live)
                {
                    printf("%zu/%s/%s\n", jenkins_one_at_a_time_hash(workerdata_arr[j][k].key, strlen(workerdata_arr[j][k].key)), workerdata_arr[j][k].key, workerdata_arr[j][k].value);
                }
            }
        }
    }
    pthread_mutex_unlock(&shared_mutex);

    return 0;
}

int handleshow(char *ptr)
{
    int i, j, k;
    int worker_idx;
    char *arg_ptr;

    if((ptr = strtok(NULL, " ")) == NULL)
        return -1;
    arg_ptr = ptr; 
    
    if((ptr = strtok(NULL, " ")) != NULL)
        return -1;

    checkLineSeparator(arg_ptr);
    worker_idx = atoi(arg_ptr);

    pthread_mutex_lock(&shared_mutex);
    for (j = 0; j < WORKER_INFO_ARR_SIZE; j++) {
        if(workerdata_arr[j] != NULL)
        {
            for (k = 0; k < WORKER_INFO_NUM; k++) {
                if(workerdata_arr[j][k].is_live && (strcmp(arg_ptr, workerdata_arr[j][k].key) == 0))
                {
                    printf("%zu/%s/%s\n", jenkins_one_at_a_time_hash(workerdata_arr[j][k].key,strlen(workerdata_arr[j][k].key)), workerdata_arr[j][k].key, workerdata_arr[j][k].value);
                }
            }
        }
    }
    pthread_mutex_unlock(&shared_mutex);

    return 0;
}

int receiveMsg(char * msg, int connfd)
{
    int cnt = 0;
    int count = 0;
    data_hdr_t *hdr = (data_hdr_t *)msg;

    memset(msg, 0, sizeof(msg));

    if((cnt += read(connfd, msg, sizeof(data_hdr_t))) != sizeof(data_hdr_t))
    {
        fprintf(stderr, "WK read: fail to read header\n");
        return -1;
    }

    if((cnt += read(connfd, msg + cnt, hdr->key_len)) != sizeof(data_hdr_t) + hdr->key_len)
    {
        fprintf(stderr, "WK read: fail to read key\n");
        return -1;
    }

    if(hdr->value_len > 0)
    {
        if((cnt += read(connfd, msg + cnt, hdr->value_len)) != sizeof(data_hdr_t) + hdr->key_len + hdr->value_len)
        {
            fprintf(stderr, "WK read: fail to read value");
            return -1;
        }
    }

    return 0;
}

int receiveHandlerMsg(char * msg, int connfd)
{
    int cnt = 0;
    int count = 0;

    memset(msg, 0, sizeof(msg));

    if((cnt += read(connfd, msg, sizeof(sync_packet_t))) != sizeof(sync_packet_t))
    {
        fprintf(stderr, "WK read: fail to read header\n");
        return -1;
    }

    return 0;
}


int handleHandlerReq(char *msg, int connfd)
{
    int result;
    bool is_success = false;
    data_hdr_t *hdr = (data_hdr_t *)msg;
    
    if(hdr->cmd == PUT)
    {
        result = handlePUT(msg);     
        if(result == -1)
        {
            hdr->cmd = PUT_ACK;
            hdr->code = ALREADY_EXIST;
        }
        else
        {
            hdr->cmd = PUT_ACK;
            hdr->code = SUCCESS;
        }
    }
    else if(hdr->cmd == GET)
    {
        result = handleGET(msg);     
        if(result == -1)
        {
            hdr->cmd = GET_ACK;
            hdr->code = NOT_EXIST;
        }
        else
        {
            hdr->cmd = GET_ACK;
            hdr->code = SUCCESS;
        }
    }
    else if(hdr->cmd == DEL)
    {
        result = handleDEL(msg);     
        hdr = (data_hdr_t *)msg;
        if(result == -1)
        {
            hdr->cmd = DEL_ACK;
            hdr->code = NOT_EXIST;
        }
        else
        {
            hdr->cmd = DEL_ACK;
            hdr->code = SUCCESS;
        }

    }

    printf("worker send %d\n", sizeof(data_hdr_t) + hdr->key_len + hdr->value_len);
    if(send(connfd, msg, sizeof(data_hdr_t) + hdr->key_len + hdr->value_len, 0) == -1)
    {
        perror("worker send to handler");
        return -1;
    }

    close(connfd);

    return 0;
}

int handlePUT(char *msg)
{
    int i, j, k;
    data_hdr_t *hdr = (data_hdr_t *)msg;

    char *key = msg + sizeof(data_hdr_t);
    char *value = msg + sizeof(data_hdr_t) + hdr->key_len;

    pthread_mutex_lock(&shared_mutex);


    printf("handle put\n");
    uint32_t hash_value = jenkins_one_at_a_time_hash(key, hdr->key_len); 
    int result = findKey(key);
    if(result != -1)
    {
        pthread_mutex_unlock(&shared_mutex);
        return -1;
    }

    printf("hande put 2\n");
    //Update a key information array
    for (i = 0; i < WORKER_INFO_ARR_SIZE; i++) {
        //Expand an array which holds client information
        if(workerdata_arr[i] == NULL)
        {
            workerdata_arr[i] = (worker_data_t *)malloc(WORKER_INFO_NUM * sizeof(worker_data_t));
            for (k = 0; k < WORKER_INFO_NUM; k++) {
                workerdata_arr[i][k].is_live= false; 
            }
        }

        for (j = 0; j < WORKER_INFO_NUM; j++) {
            if(!workerdata_arr[i][j].is_live) 
            {
                memset(workerdata_arr[i][j].key, 0, sizeof(workerdata_arr[i][j].key));
                memset(workerdata_arr[i][j].value, 0, sizeof(workerdata_arr[i][j].value));
                memcpy(workerdata_arr[i][j].key, key, hdr->key_len);
                memcpy(workerdata_arr[i][j].value, value, hdr->value_len);
                workerdata_arr[i][j].is_live = true;

                pthread_mutex_unlock(&shared_mutex);
                return 0;
            }    
        }

    }
    pthread_mutex_unlock(&shared_mutex);
    printf("handle put3\n");
    return -1;
}

int handleDEL(char *msg)
{
    int i, j;
    data_hdr_t *hdr = (data_hdr_t *)msg;
    char *key = msg + sizeof(data_hdr_t);
    char *value = msg + sizeof(data_hdr_t) + hdr->key_len;


    pthread_mutex_lock(&shared_mutex);

    uint32_t hash_value = jenkins_one_at_a_time_hash(key, hdr->key_len); 
    int result = findKey(key);
    if(result == -1)
    {
        pthread_mutex_unlock(&shared_mutex);
        return -1;
    }
    else
    {
        i = result / WORKER_INFO_NUM;
        j = result % WORKER_INFO_NUM;

        workerdata_arr[i][j].is_live = false;
    }

    pthread_mutex_unlock(&shared_mutex);
    return 0;
}

int handleGET(char *msg)
{
    int i, j;
    data_hdr_t *hdr = (data_hdr_t *)msg;
    char *key = msg + sizeof(data_hdr_t);
    uint32_t hash_value = jenkins_one_at_a_time_hash(key, hdr->key_len); 
    int worker_idx = hash_value % 5;
    pthread_mutex_lock(&shared_mutex);
    for (i = 0; i < WORKER_INFO_ARR_SIZE; i++) {
        if(workerdata_arr[i] == NULL)
        {
            pthread_mutex_unlock(&shared_mutex);
            return -1;
        }

        for (j = 0; j < WORKER_INFO_NUM; j++) {
            if((workerdata_arr[i][j].is_live) && (strcmp(key, workerdata_arr[i][j].key) == 0))
            {
                msg = realloc(msg, sizeof(data_hdr_t) + strlen(workerdata_arr[i][j].key) + strlen(workerdata_arr[i][j].key));
                hdr = (data_hdr_t *)msg;

                hdr->key_len = strlen(workerdata_arr[i][j].key);
                hdr->value_len = strlen(workerdata_arr[i][j].value);
  
                memcpy(msg + sizeof(data_hdr_t), workerdata_arr[i][j].key, hdr->key_len);
                memcpy(msg + sizeof(data_hdr_t) + hdr->key_len, workerdata_arr[i][j].value, hdr->value_len);
                printf("handleGET %s, %s\n", workerdata_arr[i][j].key, workerdata_arr[i][j].value);

                pthread_mutex_unlock(&shared_mutex);
                return 0;
            }    
        }
    }
    pthread_mutex_unlock(&shared_mutex);
    return -1;
}

int findKey(char *key)
{
    int i, j;

    for (i = 0; i < WORKER_INFO_ARR_SIZE; i++) {
        if(workerdata_arr[i] == NULL)
            return -1;

        for (j = 0; j < WORKER_INFO_NUM; j++) {
            if((workerdata_arr[i][j].is_live) && (strcmp(key, workerdata_arr[i][j].key) == 0))
            {
                return i * WORKER_INFO_NUM + j;
            }    
        }
    }

    return -1;
}

