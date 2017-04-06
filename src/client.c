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

void checkLineSeparator(char *buf);

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
    char ip[INET_ADDRSTRLEN];
    char msg[sizeof(data_hdr_t) + KEY_MAX + VALUE_MAX]; 
    data_hdr_t *hdr = (data_hdr_t *)msg;

    char *ptr;
    enum method mtd;
    int client_id = -1;
    int transaction_id = 0, cnt;
    int connfd;

    while(1)
    {
        //CLI parse
        if(client_id == -1)
            printf("Client>");
        else
            printf("Client#%d>", client_id);

        memset(cli_buffer, 0, sizeof(cli_buffer));

        fgets(cli_buffer, sizeof(cli_buffer), stdin);
        ptr = strtok(cli_buffer, " ");
        printf("ptr: %s\n", ptr);

        if(strcmp(ptr, "connect") == 0)
        {
            mtd = CONNECT_CLI; 
        }
        else if(strcmp(ptr, "put") == 0)
        {
            mtd = PUT_CLI;
        }
        else if(strcmp(ptr, "get") == 0)
        {
            mtd = GET_CLI;
        }
        else if(strcmp(ptr, "del") == 0)
        {
            mtd = DEL_CLI;
        }
        else if(strcmp(ptr, "set") == 0)
        {
            mtd = SET_CLI;
        }
        else
        {
            fprintf(stderr, "invalid cli\n");
            continue;
        }

        memset(ip, 0, sizeof ip); 
        hdr->client_id = client_id;
        hdr->transaction_id = transaction_id;
        cnt = 0;

        //TODO: client receive 
        //TODO: client key, value size restriction
        switch(mtd)
        {
            case CONNECT_CLI:
            ptr = strtok(NULL, " ");
            memcpy(ip, ptr, sizeof(ip));
            checkLineSeparator(ip);

            connfd = Open_clientfd(ip, LB_PORT);
            break;

            //TODO put 1 segmentation fault
            case PUT_CLI:
            hdr->cmd = 1;
            hdr->code = 0;

            ptr = strtok(NULL, " ");
            checkLineSeparator(ptr);
            hdr->key_len = strlen(ptr);
            memcpy(msg + sizeof(data_hdr_t), ptr, hdr->key_len);

            ptr = strtok(NULL, " ");
            checkLineSeparator(ptr);
            hdr->value_len = strlen(ptr);
            memcpy(msg + sizeof(data_hdr_t) + hdr->key_len, ptr,  hdr->key_len); 

            if(strtok(NULL, " ") != NULL)
            {
                fprintf(stderr, "invalid cli\n");
                continue;
            }

            if(send(connfd, msg, sizeof(data_hdr_t) + hdr->key_len + hdr->value_len, 0) == -1)
            {
                perror("client send");
                continue;
            }
            break;

            case GET_CLI:
            hdr->cmd = 3; 
            hdr->code = 0;

            ptr = strtok(NULL, " ");
            checkLineSeparator(ptr);
            hdr->key_len = strlen(ptr);
            memcpy(msg + sizeof(data_hdr_t), ptr, hdr->key_len);
            hdr->value_len = 0;

            if(strtok(NULL, " ") != NULL)
            {
                fprintf(stderr, "invalid cli\n");
                continue;
            }

            if(send(connfd, msg, sizeof(data_hdr_t) + hdr->key_len + hdr->value_len, 0) == -1)
            {
                perror("client send");
                continue;
            }
            break;

            case DEL_CLI:
            hdr->cmd = 5;
            hdr->code = 0;

            ptr = strtok(NULL, " ");
            checkLineSeparator(ptr);
            hdr->key_len = strlen(ptr);
            memcpy(msg + sizeof(data_hdr_t), ptr, hdr->key_len);
            hdr->value_len = 0;

            if(strtok(NULL, " ") != NULL)
            {
                fprintf(stderr, "invalid cli\n");
                continue;
            }

            if(send(connfd, msg, sizeof(data_hdr_t) + hdr->key_len + hdr->value_len, 0) == -1)
            {
                perror("client send");
                continue;
            }
            break;

            case SET_CLI:
            ptr = strtok(NULL, " ");
            client_id = atoi(ptr);
            
            if(strtok(NULL, " ") != NULL)
            {
                fprintf(stderr, "invalid cli\n");
                continue;
            }

            break;
        }

        printf("success\n");
    }
}


 
            





