#ifndef __NETWORK_H__ 
#define __NETWORK_H__




#include <unistd.h>
#include <fcntl.h>

static int set_socket_nonblocking(int fd)
{
    int flag, s; 

    flag = fcntl(fd, F_GETFL, 0);
    if(flag == -1)
    {
        perror("fcntl");
        return -1;
    }

    flag |= O_NONBLOCK;
    s = fcntl(fd, F_SETFL, flag);
    if(s == -1)
    {
        perror("fcntl");
        return -1;
    }

    return 0;
}
#endif
