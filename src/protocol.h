#include <arpa/inet.h>
#include <stdbool.h>
#ifndef __PROTOCOL_H__
#define __PROTOCOL_H__


#define PUT 0x01
#define PUT_ACK 0x02
#define GET 0x03
#define GET_ACK 0x04
#define DEL 0x05
#define DEL_ACK 0x06

#define NONE 0x00
#define SUCCESS 0X01    
#define NOT_EXIST 0x02
#define ALREADY_EXIST 0x03

#define HANDLER_NUM 3
#define WORKER_NUM 5

#define HANDLER_01_IP "127.0.0.1"
#define HANDLER_02_IP "127.0.0.1"
#define HANDLER_03_IP "127.0.0.1"
#define HANDLER_01_PORT 5231
#define HANDLER_02_PORT 5232
#define HANDLER_03_PORT 5233

#define WORKER_01_IP "127.0.0.1"
#define WORKER_02_IP "127.0.0.1"
#define WORKER_03_IP "127.0.0.1"
#define WORKER_04_IP "127.0.0.1"
#define WORKER_05_IP "127.0.0.1"
#define WORKER_01_PORT 5331
#define WORKER_02_PORT 5332
#define WORKER_03_PORT 5333
#define WORKER_04_PORT 5334
#define WORKER_05_PORT 5335

#define LB_PORT 5131

#define MAX_BUF_SIZE 8
#define KEY_MAX 4
#define VALUE_MAX 16

#define CLI_BUF_SIZE 50 

typedef struct _host_info_t{
    char ip[INET_ADDRSTRLEN];
    int port;
    int request_num;
} host_info_t;

typedef struct _client_info_t{
    int id;
    int fd;
    bool is_live;
    pthread_mutex_t mutex; 
} client_info_t;

typedef struct _key_info_t{
    uint32_t key_hash;
    int worker_idx;
    int cnt;
} key_info_t;

typedef struct _data_hdr_t{
    uint32_t client_id;
    uint32_t transaction_id;
    uint16_t cmd;
    uint16_t code;
    uint16_t key_len;
    uint16_t value_len;
} data_hdr_t;

typedef struct _sync_packet_t{
    uint32_t key_hash;
    int action;
} sync_packet_t;

static void getHandlersInfo(host_info_t hinfo[])
{
    memcpy(hinfo[0].ip, HANDLER_01_IP, sizeof(hinfo[0].ip));
    hinfo[0].port = HANDLER_01_PORT; 
    hinfo[0].request_num = 0;

    memcpy(hinfo[1].ip, HANDLER_02_IP, sizeof(hinfo[1].ip));
    hinfo[1].port = HANDLER_02_PORT; 
    hinfo[1].request_num = 0;

    memcpy(hinfo[2].ip, HANDLER_03_IP, sizeof(hinfo[2].ip));
    hinfo[2].port = HANDLER_03_PORT; 
    hinfo[2].request_num = 0;

}

static void getWorkersInfo(host_info_t hinfo[])
{
    memcpy(hinfo[0].ip, WORKER_01_IP, sizeof(hinfo[0].ip));
    hinfo[0].port = WORKER_01_PORT; 
    hinfo[0].request_num = 0;

    memcpy(hinfo[1].ip, WORKER_02_IP, sizeof(hinfo[1].ip));
    hinfo[1].port = WORKER_02_PORT; 
    hinfo[1].request_num = 0;


    memcpy(hinfo[2].ip, WORKER_03_IP, sizeof(hinfo[2].ip));
    hinfo[2].port = WORKER_03_PORT; 
    hinfo[2].request_num = 0;


    memcpy(hinfo[3].ip, WORKER_04_IP, sizeof(hinfo[3].ip));
    hinfo[3].port = WORKER_04_PORT; 
    hinfo[3].request_num = 0;


    memcpy(hinfo[4].ip, WORKER_05_IP, sizeof(hinfo[4].ip));
    hinfo[4].port = WORKER_05_PORT; 
    hinfo[4].request_num = 0;

}

uint32_t jenkins_one_at_a_time_hash(const uint8_t* key, size_t length) {
    size_t i = 0;
    uint32_t hash = 0;
    while (i != length) {
        hash += key[i++];
        hash += hash << 10;
        hash ^= hash >> 6;
    }
    hash += hash << 3;
    hash ^= hash >> 11;
    hash += hash << 15;
    return hash;
}

#endif
