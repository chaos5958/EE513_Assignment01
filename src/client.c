#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "protocol.h" 
#include "csapp.h"
#include "network.h"

#define MAXDATASIZE 100
#define CLI_BUF_SIZE 50

enum method{
    CONNECT_CLI,
    PUT_CLI,
    GET_CLI,
    DEL_CLI,
    SET_CLI,
};

int handleCONNECT(char *, int *);
int handleGET(char *,int, uint32_t, uint32_t);
int handlePUT(char *,int, uint32_t, uint32_t);
int handleDEL(char *,int, uint32_t, uint32_t);
int handleSET(char *,int *);
int receiveMsg(char *, int);

void *get_in_addr(struct sockaddr *sa)
{
    if(sa->sa_family == AF_INET){
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

int main(int argc, char *argv[])
{
    /* Variable Description
       1. sockfd: sock for connecing, sending to server
       2. buf: buf for read stream from stdin
       3. hints, servinfo, p, rv => from Beejs
       4. PORT_NUM: port number got from input argument parsing
       5. HOST: host address got from input argument parsing
       6. param_opt: return value of getopt() for input argument parsing
       7. is_p: check PORT_NUM 
       8. is_h: check HOST
       9. iteration: use in whileloop for checking first loop 
       */

    int sockfd;
    char buf[MAXDATASIZE]; 
    struct addrinfo hints, *servinfo, *p;
    int rv;
    char s[INET6_ADDRSTRLEN];

    char *PORT_NUM = NULL;
    char *HOST = NULL;

    int iteration=0;
    char cli_buffer[CLI_BUF_SIZE];
    char msg[sizeof(data_hdr_t) + KEY_MAX + VALUE_MAX]; 
    data_hdr_t *hdr = (data_hdr_t *)msg;

    char *ptr;
    enum method mtd;
    int client_id = -1;
    int transaction_id = 0, cnt;
    int connfd;
    int result; 

    while(1)
    {
        //CLI parse
        if(client_id == -1)
            printf("Client>");
        else
            printf("Client#%d>", client_id);

        memset(cli_buffer, 0, sizeof(cli_buffer));
        hdr->client_id = client_id;
        hdr->transaction_id = transaction_id;

        fgets(cli_buffer, sizeof(cli_buffer), stdin);
        ptr = strtok(cli_buffer, " ");
        printf("ptr: %s\n", ptr);

        if(strcmp(ptr, "connect") == 0)
        {
            result = handleCONNECT(ptr, &connfd);
            
            if(result == -1)
                continue;
        }
        else if(strcmp(ptr, "put") == 0)
        {
            result = handlePUT(ptr, connfd, client_id, transaction_id);
            transaction_id++;

            if(result == -1)
                continue;

            result = receiveMsg(msg, connfd);

            if(result == -1)
                continue;
        }
        else if(strcmp(ptr, "get") == 0)
        {
            result = handleGET(ptr, connfd, client_id, transaction_id);
            transaction_id++;
            
            if(result == -1)
                continue;

            result = receiveMsg(msg, connfd);

            if(result == -1)
                continue;

        }
        else if(strcmp(ptr, "del") == 0)
        {
            result = handleDEL(ptr, connfd, client_id, transaction_id);
            transaction_id++;

            if(result == -1)
                continue;

            result = receiveMsg(msg, connfd);

            if(result == -1)
                continue;
        }
        else if(strcmp(ptr, "set") == 0)
        {
            result = handleSET(ptr, &client_id);

            if(result == -1)
                continue;
        }
        else
        {
            fprintf(stderr, "invalid cli\n");
            continue;
        }
    }
    return 0;
}


int handlePUT(char *ptr, int connfd, uint32_t client_id, uint32_t transaction_id)
{
    char msg[sizeof(data_hdr_t) + KEY_MAX + VALUE_MAX];
    data_hdr_t *hdr = (data_hdr_t *)msg;

    hdr->client_id = client_id;
    hdr->transaction_id = transaction_id;
    hdr->cmd = 1;
    hdr->code = 0;

    if((ptr = strtok(NULL, " ")) == NULL)
    {
        fprintf(stderr, "invalid cli put\n");
        return -1;
    }
    checkLineSeparator(ptr);
    if(strlen(ptr) > KEY_MAX)
    {
        fprintf(stderr, "invalid cli put - key length\n");
        return -1;
    }
    hdr->key_len = strlen(ptr);
    memcpy(msg + sizeof(data_hdr_t), ptr, hdr->key_len);

    if((ptr = strtok(NULL, " ")) == NULL)
    {
        fprintf(stderr, "invalid cli put\n");
        return -1;
    }
    checkLineSeparator(ptr);
    if(strlen(ptr) > VALUE_MAX)
    {
        fprintf(stderr, "invalid cli put - value length\n");
        return -1;
    }
    hdr->value_len = strlen(ptr);
    memcpy(msg + sizeof(data_hdr_t) + hdr->key_len, ptr,  hdr->value_len); 

    if(strtok(NULL, " ") != NULL)
    {
        fprintf(stderr, "invalid cli put\n");
        return -1;
    }

    if(send(connfd, msg, sizeof(data_hdr_t) + hdr->key_len + hdr->value_len, 0) == -1)
    {
        perror("client send");
        return -1;
    }

    return 0;
}

int handleGET(char *ptr, int connfd, uint32_t client_id, uint32_t transaction_id)
{
    char msg[sizeof(data_hdr_t) + KEY_MAX + VALUE_MAX];
    data_hdr_t *hdr = (data_hdr_t *)msg;

    hdr->client_id = client_id;
    hdr->transaction_id = transaction_id;
    hdr->cmd = 3; 
    hdr->code = 0;

    if((ptr = strtok(NULL, " ")) == NULL)
    {
        fprintf(stderr, "invalid cli get\n");
        return -1;
    }
    checkLineSeparator(ptr);
    if(strlen(ptr) > hdr->key_len)
    {
        fprintf(stderr, "invalid cli get - key length\n");
        return -1;
    }
    hdr->key_len = strlen(ptr);
    memcpy(msg + sizeof(data_hdr_t), ptr, hdr->key_len);
    hdr->value_len = 0;

    if(strtok(NULL, " ") != NULL)
    {
        fprintf(stderr, "invalid cli get\n");
        return -1;
    }

    if(send(connfd, msg, sizeof(data_hdr_t) + hdr->key_len + hdr->value_len, 0) == -1)
    {
        perror("client send");
        return -1;
    }
    
    return 0;
}

int handleCONNECT(char *ptr, int *connfd){
    char msg[sizeof(data_hdr_t) + KEY_MAX + VALUE_MAX];
    data_hdr_t *hdr = (data_hdr_t *)msg;
    char ip[INET_ADDRSTRLEN];

    if((ptr = strtok(NULL, " ")) == NULL)
    {
        fprintf(stderr, "invalid cli - connect\n");
        return -1;
    }
    memcpy(ip, ptr, sizeof(ip));
    checkLineSeparator(ip);

    if(strtok(NULL, " ") != NULL)
    {
        fprintf(stderr, "invalid cli - connect\n");
        return -1;
    }

    *connfd = Open_clientfd(ip, LB_PORT);

    return -1;
}

int handleDEL(char *ptr, int connfd, uint32_t client_id, uint32_t transaction_id)
{
    char msg[sizeof(data_hdr_t) + KEY_MAX + VALUE_MAX];
    data_hdr_t *hdr = (data_hdr_t *)msg;
    
    hdr->client_id = client_id;
    hdr->transaction_id = transaction_id;
    hdr->cmd = 5; 
    hdr->code = 0;

    if((ptr = strtok(NULL, " ")) == NULL)
    {
        fprintf(stderr, "invalid cli del\n");
        return -1;
    }
    checkLineSeparator(ptr);
    if(strlen(ptr) > hdr->key_len)
    {
        fprintf(stderr, "invalid cli del - key length\n");
        return -1;
    }
    hdr->key_len = strlen(ptr);
    memcpy(msg + sizeof(data_hdr_t), ptr, hdr->key_len);
    hdr->value_len = 0;

    if(strtok(NULL, " ") != NULL)
    {
        fprintf(stderr, "invalid cli del\n");
        return -1;
    }

    if(send(connfd, msg, sizeof(data_hdr_t) + hdr->key_len + hdr->value_len, 0) == -1)
    {
        perror("client send");
        return -1;
    }
    
    return 0;
}

int handleSET(char *ptr, int *client_id){
    char msg[sizeof(data_hdr_t) + KEY_MAX + VALUE_MAX];
    data_hdr_t *hdr = (data_hdr_t *)msg;
    char *ptr_;

    if((ptr = strtok(NULL, " ")) == NULL)
    {
        fprintf(stderr, "invalid cli - connect\n");
        return -1;
    }
    ptr_ = ptr;

    if(strtok(NULL, " ") != NULL)
    {
        fprintf(stderr, "invalid cli - connect\n");
        return -1;
    }

    checkLineSeparator(ptr_);
    *client_id = atoi(ptr_);
    printf("atoi client_id %d\n", *client_id);


    return 0;
}

int receiveMsg(char * msg, int connfd)
{
    int cnt = 0;
    int count = 0;
    data_hdr_t *hdr = (data_hdr_t *)msg;

    memset(msg, 0, sizeof(msg));
    printf("connfd : %d\n", connfd);

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

    printf("hdr->code %d\n", hdr->code);
    printf("hdr->cmd %d\n", hdr->cmd);
    if(hdr->code == SUCCESS)
    {
        if(hdr->cmd == PUT_ACK || hdr->cmd == DEL_ACK)
            printf("Success\n");
        else if(hdr->cmd == GET_ACK)
            printf("%s\n", msg + sizeof(data_hdr_t) + hdr->key_len);
    }
    else if(hdr->code == ALREADY_EXIST)
        printf("Fail (reason: already exist)\n");
    else if(hdr->code == NOT_EXIST)
        printf("Fail (reason: not exist)\n");

    return 0;
}










