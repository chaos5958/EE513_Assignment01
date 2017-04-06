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
#define KEY_INFO_MAX 100000
#define KEY_INFO_ARR_SIZE 10
#define KEY_INFO_NUM (KEY_INFO_MAX / KEY_INFO_ARR_SIZE)

int enqueue_task(int, int);
int dequeue_task(int);
void *worker_func(void *args);
void initialize_handler(int, int *, pthread_t *);
int handleCLI();
int receiveMsg(char *msg, int connfd);
int handleLB(char *msg);
int handleWorker(char *msg);
int handleshow(char *);
int handlelist(char *);
int receiveHandlerMsg(char *, int);
bool is_handlerfd(int);
int updateKey(uint32_t hash_value, int status);
int findKey(uint32_t hash_value);

int byte_cnt = 0; /* counts total bytes received by server */
pthread_cond_t *empty_arr; /* condition variable to wake up worker threads when input comes */
pthread_cond_t *full_arr; /* condition variable to wake up super threads when queues have an empty entry */ 
pthread_mutex_t *mutex_arr; /* mutext for prevent racing condition between the acceptor thread and the worker thread (put/get into/from the queue) */
pthread_mutex_t shared_mutex; /* mutex between worker threads */
int *count_arr; /* managing how many number of tasks in the worker thread queue */
int **accept_queue; /* worker threads's queue */
int handler_fd[HANDLER_NUM - 1];
int client_num = KEY_INFO_NUM;
int lb_fd;
host_info_t handlerinfo_arr[HANDLER_NUM];
host_info_t workerinfo_arr[WORKER_NUM];
key_info_t ***keyinfo_arr;
int epoll_fd;

int main(int argc, char **argv)
{
    int param_opt;
    bool is_thread = 0, is_handler = 0;
    int listenfd, connfd, port, num_thread, i, enqueue_result; 
    int *ids;
    int worker_index = 0;
    int count = 0;
    pthread_t *pthread_arr;
    int ssn, ec;
    struct epoll_event event;
    struct epoll_event *events;
    int handler_idx;
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
                is_handler = true;
                handler_idx = atoi(optarg);
                break;

            default:
            {
                fprintf(stderr, "usage: %s -n [handler_idx] -t [thread_num] (t: optinal)\n", argv[0]);
                exit(1);
            }
        }
    }

    if(is_thread == 0)
        num_thread = WORKER_THREAD_NUM;

    if(is_handler == 0)
    {
        fprintf(stderr, "usage: %s -t [thread_num] (t: optinal)\n", argv[0]);
        exit(1);
    }

    //Initialize several environment variables (most related to threads) 
    initialize_handler(num_thread, ids, pthread_arr);
    getHandlersInfo(handlerinfo_arr);    
    getWorkersInfo(workerinfo_arr);
    printf("port: %d\n", workerinfo_arr[0].port);

    //Create a socket, then, bind and listen to a port 
    listenfd = Open_listenfd(handlerinfo_arr[handler_idx].port);

    //Connect to other handlers 
    for (i = 0; i < handler_idx; i++) {
        
        while((handler_fd[conn_handler_num] = Open_clientfd(handlerinfo_arr[i].ip, handlerinfo_arr[i].port)) == -1); 
        conn_handler_num++; ;
    }

    

    //Accept other handlers 
    while(conn_handler_num < HANDLER_NUM - 1)
    {
        handler_fd[conn_handler_num] = accept(listenfd, (struct sockaddr*)&clientaddr, &clientlen); 

        if(handler_fd[conn_handler_num] == -1)
        {
            perror("handler accepts other handelrs");
            abort();
        }

        conn_handler_num++;
    } 

    printf("handler connection success\n");

    lb_fd  = accept(listenfd, (struct sockaddr*)&clientaddr, &clientlen); 

    if(lb_fd == -1)
    {
        perror("handler accepts a load balancer");
        abort();
    }

    printf("handler-lb connection success\n");

    //Create an epoll instance
    epoll_fd = epoll_create1(0);
    if(epoll_fd == -1)
    {
        perror("epoll_create1(0)");
        abort();
    }

    //Set listen socket as non-blocking
    ssn = set_socket_nonblocking(listenfd);
    if(ssn == -1)
        abort();


    //Register a listnen fd and STDIN
    event.data.fd = listenfd;
    event.events = EPOLLIN | EPOLLET;
    ec = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listenfd, &event);

    event.data.fd = lb_fd;
    event.events = EPOLLIN | EPOLLET;
    ec = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, lb_fd, &event);

    for (i = 0; i < HANDLER_NUM - 1; i++) {
        event.data.fd = handler_fd[i];
        event.events = EPOLLIN | EPOLLET;
        ec = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, handler_fd[i], &event);
    }

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
                    fprintf(stdout, "HD>%s:%05u\n", s, clientaddr.sin_port);

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
        //Handle a handler response
        else if(is_handlerfd(connfd))
        {
            sync_packet_t *recv_sync = (sync_packet_t *)msg;
            result = receiveHandlerMsg(msg, connfd);

            if(result == -1)
                continue;

            result = updateKey(recv_sync->key_hash, recv_sync->action);

            if(result == -1)
                continue;
        }
        //Handle a worker response
        else
        {
            printf("debug start\n");
            result = receiveMsg(msg, connfd);

            if(result == -1)
                continue;

            /* Client request */
            if(hdr->code == 0)
            {
                printf("debug_client_req\n");
                result = handleLB(msg);
                
                if(result == -1)
                    continue;
            }
            /* Handler response */
            else
            {
                printf("debug_worker\n");
                result = handleWorker(msg);

                if(result == -1)
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

void initialize_handler(int num_thread, int *ids, pthread_t *pthread_arr)
{
    int i, j;

    empty_arr = (pthread_cond_t *)malloc(num_thread * sizeof(pthread_cond_t));
    full_arr = (pthread_cond_t *)malloc(num_thread * sizeof(pthread_cond_t));
    mutex_arr = (pthread_mutex_t *)malloc(num_thread * sizeof(pthread_mutex_t));
    count_arr = (int *)malloc(num_thread * sizeof(int));
    pthread_arr = (pthread_t *)malloc(num_thread * sizeof(pthread_t));
    ids = (int *)malloc(num_thread * sizeof(int));
    accept_queue = (int **)malloc(num_thread * sizeof(int *));
    keyinfo_arr = (key_info_t ***)malloc(WORKER_NUM * sizeof(key_info_t **));

    for (i = 0; i < WORKER_NUM; i++) {
        keyinfo_arr[i] = (key_info_t **)malloc(KEY_INFO_ARR_SIZE * sizeof(key_info_t*));
        keyinfo_arr[i][0] = (key_info_t *)malloc(KEY_INFO_NUM * sizeof(key_info_t));
        for (j = 0; j < KEY_INFO_NUM; j++) {
            keyinfo_arr[i][0][j].cnt = 0;
            keyinfo_arr[i][0][j].worker_idx = -1;
        }
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
    for (i = 0; i < WORKER_NUM; i++) {
        for (j = 0; j < KEY_INFO_ARR_SIZE; j++) {
            if(keyinfo_arr[i][j] != NULL){
                for (k = 0; k < KEY_INFO_NUM; k++) {
                    if(keyinfo_arr[i][j][k].worker_idx != -1)
                    {
                        printf("%d/%zu\n", keyinfo_arr[i][j][k].worker_idx, keyinfo_arr[i][j][k].key_hash);
                    }
                }
            }
        }
    }
    printf("%d\n", keyinfo_arr[3][0][0].worker_idx);

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
    for (j = 0; j < KEY_INFO_ARR_SIZE; j++) {
        if(keyinfo_arr[worker_idx][j] != NULL)
        {
            for (k = 0; k < KEY_INFO_NUM; k++) {
                if(keyinfo_arr[worker_idx][j][k].cnt > 0)
                {
                    printf("%d/%zu\n", keyinfo_arr[worker_idx][j][k].worker_idx, keyinfo_arr[worker_idx][j][k].key_hash);
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
        fprintf(stderr, "handler read: fail to read header\n");
        return -1;
    }

    if((cnt += read(connfd, msg + cnt, hdr->key_len)) != (sizeof(data_hdr_t) + hdr->key_len))
    {
        fprintf(stderr, "handler read: fail to read key\n");
        return -1;
    }

    if(hdr->value_len > 0)
    {
        if((cnt += read(connfd, msg + cnt, hdr->value_len)) != (sizeof(data_hdr_t) + hdr->key_len + hdr->value_len))
        {
            fprintf(stderr, "handler read: fail to read value");
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
        fprintf(stderr, "handler read: fail to read header\n");
        return -1;
    }

    return 0;
}


int handleLB(char *msg)
{
    int i, j, lock;
    bool is_success = false;
    struct epoll_event event;
    data_hdr_t *hdr = (data_hdr_t *)msg;

    uint32_t hash_value = jenkins_one_at_a_time_hash(msg + sizeof(data_hdr_t), hdr->key_len);

    int connfd = Open_clientfd(workerinfo_arr[hash_value % WORKER_NUM].ip, workerinfo_arr[hash_value % WORKER_NUM].port); 

    event.data.fd = connfd;
    event.events = EPOLLIN | EPOLLET;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, connfd, &event);

    //TODO: multithread OK for epoll_ctl?

    if(send(connfd, msg, sizeof(data_hdr_t) + hdr->key_len + hdr->value_len, 0) == -1)
    {
        perror("Handler sends to workers");
        return -1;
    }


    //printf("hash value: %zu, %d\n", hash_value, hash_value % WORKER_NUM);
    //printf("ip:%s, port: %d\n", workerinfo_arr[hash_value % WORKER_NUM].ip, workerinfo_arr[hash_value % WORKER_NUM].port);
    
    return 0;
}

int handleWorker(char *msg)
{
    //Find a client which id is 'id'
    int i, j, k, lock, client_fd = -1, hash_value;
    data_hdr_t *hdr = (data_hdr_t *)msg;
    bool is_success = false;
    sync_packet_t *sync_packet = (sync_packet_t *)malloc(sizeof(sync_packet_t));

    //Send a message to a client
    if(send(lb_fd, msg, sizeof(data_hdr_t) + hdr->key_len + hdr->value_len, 0) == -1)
    {
        perror("[LB] send to client error");
        return -1;
    }

    hash_value = jenkins_one_at_a_time_hash(msg + sizeof(data_hdr_t), hdr->key_len);
    printf("hdr->keylen %d, hash %d\n", hdr->key_len, hash_value);

    //Synchronize handlers
    if(hdr->cmd == PUT_ACK && hdr->code == SUCCESS)
    {
        printf("debug_put1\n");
        updateKey(hash_value, PUT); 

        sync_packet->key_hash = hash_value;
        sync_packet->action= PUT;
        
        for (i = 0; i < HANDLER_NUM - 1; i++) {
            if(send(handler_fd[i], sync_packet, sizeof(sync_packet_t), 0) == -1)
            {
                perror("Handler sends to otherr handlers");
                return -1;
            }
        }
    }
    else if(hdr->cmd == DEL_ACK && hdr->code == SUCCESS)
    {
        printf("debug_put2\n");
        updateKey(hash_value, DEL);

        sync_packet->key_hash = hash_value;
        sync_packet->action = DEL;

        for (i = 0; i < HANDLER_NUM - 1; i++) {
            if(send(handler_fd[i], sync_packet, sizeof(sync_packet_t), 0) == -1)
            {
                perror("Handler sends to otherr handlers");
                return -1;
            }
        }
    }

    return 0;
}

int updateKey(uint32_t hash_value, int status)
{
    printf("updateKey\n");
    int i, j, k;
    int i_, j_, value;
    uint32_t worker_idx = hash_value % WORKER_NUM; if(status != PUT && status != DEL) { fprintf(stderr, "wrong status\n"); return -1; } pthread_mutex_lock(&shared_mutex); value = findKey(hash_value);
    if(value != -1)
    {
        printf("findkey\n");
        i_ = value / KEY_INFO_NUM;
        j_ = value % KEY_INFO_NUM;

        if(status == PUT)
            keyinfo_arr[worker_idx][i_][j_].cnt++;
        else if(status == DEL)
            keyinfo_arr[worker_idx][i_][j_].cnt--;

        if(keyinfo_arr[worker_idx][i_][j_].cnt == 0)
            keyinfo_arr[worker_idx][i_][j_].worker_idx = -1;
        pthread_mutex_unlock(&shared_mutex);
        return 0;
    }

    //Update a key information array
    for (i = 0; i < KEY_INFO_ARR_SIZE; i++) {
        //Expand an array which holds client information
        printf("debug 1\n");
        printf("worker_idx %d\n", worker_idx);
        if(keyinfo_arr[worker_idx][i] == NULL)
        {
            keyinfo_arr[worker_idx][i] = (key_info_t *)malloc(KEY_INFO_NUM * sizeof(key_info_t));
            for (k = 0; k < KEY_INFO_NUM; k++) {
                keyinfo_arr[worker_idx][i][k].worker_idx = -1; 
                keyinfo_arr[worker_idx][i][k].cnt = 0;
            }
        }
        printf("debug 2\n");
        for (j = 0; j < KEY_INFO_NUM; j++) {
            if(keyinfo_arr[worker_idx][i][j].worker_idx == -1) 
            {
                keyinfo_arr[worker_idx][i][j].key_hash = hash_value;
                keyinfo_arr[worker_idx][i][j].worker_idx = hash_value % WORKER_NUM;
                printf("%zu hash\n", hash_value);
                printf("i: %d, j: %d, worker_idx: %zu\n", i, j, worker_idx);
                if(status == PUT)
                {
                    keyinfo_arr[worker_idx][i][j].cnt = 1;
                    printf("PUT receive\n");
                }
                else if(status == DEL)
                {
                    keyinfo_arr[worker_idx][i][j].cnt = -1;
                    printf("DEL receive\n");
                }

                pthread_mutex_unlock(&shared_mutex);
                return 0;
            }    
        }

    }
    pthread_mutex_unlock(&shared_mutex);
    return -1;
}

int findKey(uint32_t hash_value)
{
    int i, j;
    int worker_idx = hash_value % 5;

    for (i = 0; i < KEY_INFO_ARR_SIZE; i++) {
        if(keyinfo_arr[worker_idx][i] == NULL)
            return -1;

        for (j = 0; j < KEY_INFO_NUM; j++) {
            if((keyinfo_arr[worker_idx][i][j].worker_idx != -1) && (keyinfo_arr[worker_idx][i][j].key_hash == hash_value)) 
            {
                return i * KEY_INFO_NUM + j;
            }    
        }
    }

    return -1;
}

bool is_handlerfd(int fd)
{
    int i;
    for (i = 0; i < HANDLER_NUM - 1; i++) {
        if(fd == handler_fd[i])
            return true;
    }

    return false;
}
