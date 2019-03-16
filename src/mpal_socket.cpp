/* 
 * Virtual Linear Address SERvice
 *
 * Author :Liu Peng-Hong  Institute of Scientific Computing, Nankai Univ.
 *
 * Message Passing Abstract Layer
 * class vlaser::mpal_socket
 * Source File
 *
 * Mar 26, 2011  Original Design
 *
 */

#define REQ_LISTEN 2011
#define ACK_LISTEN 2012
#define SER_LISTEN 2013
#define CTL_LISTEN 2014
#define REQ_QUEUE_SIZE 128 // request listen socket's accept() queue size
#define ACK_QUEUE_SIZE 1
#define SER_QUEUE_SIZE 1
#define CTL_QUEUE_SIZE 128
#define SERREQSEND_TIMEOUT 300000 //microsecond, timeout for SerReqSend's socket sendibg
#define TIMEOUT_RETRY_INTERVAL 50000 // microsecond, reconnect interval of socket connection failure
#define TCP_SEND_FLAG MSG_DONTROUTE // do not route tcp package out of subnet
#define TCP_RECV_FLAG MSG_WAITALL // block the recieve function until the recv buffer is full or tcp is closed
#define TCP_SEND_BUFFER_SIZE 4096 // how many bytes will be sent for socket sending


#include "mpal_socket.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <fstream>
#include <set>

namespace vlaser {

  /*
   * Implementation of class mpal_socket
   */

  mpal_socket::mpal_socket(vsnodeid id, vsnodeid num, char my_addr_path[], char hosts_addr_path[]) :
  mpal(id, num)
  {
    id_map_vlaser_to_inet = new my_inet_addr_t[num];
    addr_list = new my_inet_addr_t[num];
    my_addr_file_path = my_addr_path;
    hosts_addr_file_path = hosts_addr_path;
    VLASER_DEB("got "<<num<<" nodes");
    VLASER_DEB("my addr file is "<<my_addr_path);
    VLASER_DEB("hosts addr file is "<<hosts_addr_path);

    req_channel_port = htons(REQ_LISTEN);
    ack_channel_port = htons(ACK_LISTEN);
    ser_channel_port = htons(SER_LISTEN);
    ctl_channel_port = htons(CTL_LISTEN);
    is_initialized = 0;
    is_busy = 0;
  }

  mpal_socket::~mpal_socket()
  {
    delete[] addr_list;
    delete[] id_map_vlaser_to_inet;
  }
  
  void
  mpal_socket::GetInetHosts()
  {
    std::ifstream my_addr(my_addr_file_path.c_str());
    std::ifstream hosts_addr(hosts_addr_file_path.c_str());
    std::string strtmp;
    struct in_addr addrtmp;

    VLASER_DEB("enter GetInetHosts()");

    std::getline(my_addr, strtmp);
    if(!inet_aton(strtmp.c_str(), &addrtmp))
      throw mpal_runtime_error("my inet address error in my address file: from mpal_socket::GetInetHosts()");
    VLASER_DEB("got my addr is "<<strtmp);
    my_inet_addr = addrtmp.s_addr;
    my_addr.close();

    /* get all nodes' ip addresses */
    for(int i = 0; i < node_num; ++i) {
      if(!std::getline(hosts_addr, strtmp))
        throw mpal_runtime_error("hosts file has not enough host addresses: from mpal_socket::GetInetHosts()");
      VLASER_DEB("got a host: "<<strtmp);
      if(!inet_aton(strtmp.c_str(), &addrtmp))
        throw mpal_runtime_error("host inet address error in hosts address file: from mpal_socket::GetInetHosts()");
      addr_list[i] = addrtmp.s_addr;
    }
    hosts_addr.close();
    VLASER_DEB("GetInetHosts() ok");
    return;
  }

  void
  mpal_socket::PrepareSocks()
  {
    struct sockaddr_in channel;

    /* create, bind, and listen all three channel's listen sockets */
    VLASER_DEB("enter PrepareSocks()");
    if((req_listen_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
      throw mpal_runtime_error("getting req_listen_socket failed: from mpal_socket::PrepareSocks()");
    if((ack_listen_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
      throw mpal_runtime_error("getting ack_listen_socket failed: from mpal_socket::PrepareSocks()");
    if((ser_listen_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
      throw mpal_runtime_error("getting ser_listen_socket failed: from mpal_socket::PrepareSocks()");
    if((ctl_listen_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
      throw mpal_runtime_error("getting ctl_listen_socket failed: from mpal_socket::PrepareSocks()");
    channel.sin_family = AF_INET;
    channel.sin_addr.s_addr = my_inet_addr;
    channel.sin_port = req_channel_port;
    if(bind(req_listen_socket, (struct sockaddr *)&channel, sizeof(channel)) != 0)
      throw mpal_runtime_error("binding req_listen_socket failed: from mpal_socket::PrepareSocks()");
    channel.sin_port = ack_channel_port;
    if(bind(ack_listen_socket, (struct sockaddr *)&channel, sizeof(channel)) != 0)
      throw mpal_runtime_error("binding ack_listen_socket failed: from mpal_socket::PrepareSocks()");
    channel.sin_port = ser_channel_port;
    if(bind(ser_listen_socket, (struct sockaddr *)&channel, sizeof(channel)) != 0)
      throw mpal_runtime_error("binding ser_listen_socket failed: from mpal_socket::PrepareSocks()");
    channel.sin_port = ctl_channel_port;
    if(bind(ctl_listen_socket, (struct sockaddr *)&channel, sizeof(channel)) != 0)
      throw mpal_runtime_error("binding ctl_listen_socket failed: from mpal_socket::PrepareSocks()");
    if(listen(req_listen_socket, REQ_QUEUE_SIZE) != 0)
      throw mpal_runtime_error("listen req_listen_socket failed: from mpal_socket::PrepareSocks()");
    if(listen(ack_listen_socket, ACK_QUEUE_SIZE) != 0)
      throw mpal_runtime_error("listen ack_listen_socket failed: from mpal_socket::PrepareSocks()");
    if(listen(ser_listen_socket, SER_QUEUE_SIZE) != 0)
      throw mpal_runtime_error("listen ser_listen_socket failed: from mpal_socket::PrepareSocks()");
    if(listen(ctl_listen_socket, CTL_QUEUE_SIZE) != 0)
      throw mpal_runtime_error("listen ctl_listen_socket failed: from mpal_socket::PrepareSocks()");
    VLASER_DEB("PrepareSocks() ok");
    return;
  }

  int
  mpal_socket::Initialize()
  {
    struct sockaddr_in paddr;
    int cflag, host_fail_flag, send_counter, c;
    socklen_t addr_len;
    vsnodeid id_tmp;
    int socktmp, ack_send_socket, req_send_socket;
    std::set<my_inet_addr_t> hosts_set;
    int* pcounter = NULL;
    unsigned char* pbuf = NULL;

    signal(SIGPIPE, SIG_IGN);
    GetInetHosts();
    PrepareSocks();
    /* sleep one second, waiting for all node's setup */
    usleep(1000000);

    VLASER_DEB("entered Initialize()");
    if(is_initialized)
      return 0;
    /* the first node in ip address list is used as host */
    if(addr_list[0] == my_inet_addr) { /* if I'm the host */
      VLASER_DEB("I am mpal host. Now enter the id table making sequence");
      host_fail_flag = 0;

      for(int i = 1; i < node_num; ++i)
        hosts_set.insert(addr_list[i]);

      pcounter = new int[node_num];
      for(int i = 0; i < node_num; ++i)
        pcounter[i] = 0;
      pcounter[my_id] = 1;
      id_map_vlaser_to_inet[my_id] = my_inet_addr;
      for(int i = 1; i < node_num; ++i) {
        VLASER_DEB("waiting a node to send its vlaser id in initial req");

        addr_len = sizeof(paddr);
        socktmp = accept(req_listen_socket, (struct sockaddr *)&paddr, &addr_len);
        recv(socktmp, &id_tmp, sizeof(vsnodeid), TCP_RECV_FLAG);
        close(socktmp);
        VLASER_DEB("got vlaser id "<<id_tmp<<" and its ipv4 address is "<<paddr.sin_addr.s_addr);

        if(id_tmp < node_num) {
          if(!pcounter[id_tmp])
            ++pcounter[id_tmp];
          else
            host_fail_flag = 1;

          id_map_vlaser_to_inet[id_tmp] = paddr.sin_addr.s_addr;
          /* erase the ip address from the set, if it is not in the set,
           * something goes wrong here
           */
          if(!hosts_set.erase(paddr.sin_addr.s_addr))
            host_fail_flag = 1;
        }
        else
          host_fail_flag = 1;
      }
      /* send host_fail_flag to all nodes */
      VLASER_DEB("host_fail_flag is "<<host_fail_flag<<" ,now send it to all nodes");
      for(int i = 1; i < node_num; ++i) {
        paddr.sin_family = AF_INET;
        paddr.sin_addr.s_addr = addr_list[i];
        paddr.sin_port = ack_channel_port;
        if((ack_send_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
          throw mpal_runtime_error("getting ack_send_socket failed: from mpal_socket::Initalize()");
        VLASER_DEB("connect "<<i<<" in addr_list for sending host_fail_flag");
        while((cflag = connect(ack_send_socket, (struct sockaddr *)&paddr, sizeof(paddr))) != 0 && (errno == ETIMEDOUT || errno == ECONNREFUSED))
          usleep(TIMEOUT_RETRY_INTERVAL);
        if(cflag < 0)
          throw mpal_runtime_error("ack connecting failed: from mpal_socket::Initialize()");
        send(ack_send_socket, &host_fail_flag, sizeof(int), TCP_SEND_FLAG);
        VLASER_DEB("sent host_fail_flag to "<<i<<" in addr_list");
        close(ack_send_socket);
      }
      delete[] pcounter;
    }
    else {
      /* For the case that I'm not host node
       */
      paddr.sin_family = AF_INET;
      paddr.sin_addr.s_addr = addr_list[0];
      paddr.sin_port = req_channel_port;
      VLASER_DEB("connecting mpal host to send my vlaser id, my id is "<<my_id);
      /* send my node id to host */
      if((req_send_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
        throw mpal_runtime_error("getting req_send_socket failed: from mpal_socket::Initalize()");
      while((cflag = connect(req_send_socket, (struct sockaddr *)&paddr, sizeof(paddr))) != 0 && (errno == ETIMEDOUT || errno == ECONNREFUSED))
        usleep(TIMEOUT_RETRY_INTERVAL);
      if(cflag < 0)
        throw mpal_runtime_error("req connecting failed: from mpal_socket::Initialize()");
      send(req_send_socket, &my_id, sizeof(vsnodeid), TCP_SEND_FLAG);
      close(req_send_socket);
      VLASER_DEB("vlaser id sending ok");
      VLASER_DEB("now waiting the mpal host to return the host_fail_flag");
      /* wait for the host_fail_flag */
      socktmp = accept(ack_listen_socket, NULL, NULL);
      recv(socktmp, &host_fail_flag, sizeof(int), TCP_RECV_FLAG);
      close(socktmp);
      VLASER_DEB("got host_fail_flag is "<<host_fail_flag);
    }
    if(host_fail_flag)
      throw mpal_runtime_error("hosts table is broken: from mpal_socket::Initialize()");
    if(addr_list[0] == my_inet_addr) /* I am mpal host */
      for(int i = 1; i < node_num; ++i) {
        paddr.sin_family = AF_INET;
        paddr.sin_addr.s_addr = addr_list[i];
        paddr.sin_port = ack_channel_port;
        VLASER_DEB("broadcasting id table to "<<i<<" in addr_list");
        if((ack_send_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
          throw mpal_runtime_error("getting ack_send_socket failed: from mpal_socket::Initalize()");
        while((cflag = connect(ack_send_socket, (struct sockaddr *)&paddr, sizeof(paddr))) != 0 && (errno == ETIMEDOUT || errno == ECONNREFUSED))
          usleep(TIMEOUT_RETRY_INTERVAL);
        if(cflag < 0)
          throw mpal_runtime_error("table broadcasting connecting failed: from mpal_socket::Initialize()");
        send_counter = sizeof(my_inet_addr_t) * node_num;
        pbuf = (unsigned char*) id_map_vlaser_to_inet;
        while(send_counter > 0)
          if(send_counter < TCP_SEND_BUFFER_SIZE) {
            c = send(ack_send_socket, pbuf, send_counter, TCP_SEND_FLAG);
            if(c != send_counter)
              throw mpal_runtime_error("socket sent less bytes than expected: from mpal_socket::SendProto()");
              send_counter = 0;
          }
          else {
            c = send(ack_send_socket, pbuf, TCP_SEND_BUFFER_SIZE, TCP_SEND_FLAG);
            if(c != TCP_SEND_BUFFER_SIZE)
              throw mpal_runtime_error("socket sent less bytes than expected: from mpal_socket::SendProto()");
            pbuf += TCP_SEND_BUFFER_SIZE;
            send_counter -= TCP_SEND_BUFFER_SIZE;
          }
          VLASER_DEB("broadcasting id table to "<<i<<" in addr_list ok");
        close(ack_send_socket);
      }
    else { /* I am not host */
      VLASER_DEB("waiting id table from mpal host");
      socktmp = accept(ack_listen_socket, NULL, NULL);
      send_counter = recv(socktmp, id_map_vlaser_to_inet, sizeof(my_inet_addr_t) * node_num, TCP_RECV_FLAG);
      if(send_counter != sizeof(my_inet_addr_t) * node_num)
          throw mpal_runtime_error("table broadcasting recv failed: from mpal_socket::Initialize()");
      VLASER_DEB("got id table");
      close(socktmp);
    }
    is_initialized = 1;
    VLASER_DEB("Initialize() ok");
    return 0;
  }
  
  int
  mpal_socket::Test(unsigned char flag)
  {
    int socktmp;
    vsnodeid* pcounter;
    vsnodeid pbuf[2];
    vsnodeid result_flag = 1;
    int cflag, req_send_socket, ack_send_socket;
    struct sockaddr_in paddr;

    VLASER_DEB("entered Test() with flag "<<(int)flag);
    if(is_initialized) {
      is_busy = 1;
      if(my_inet_addr == addr_list[0]) {
        VLASER_DEB("I am mpal host. Now enter flags collecting sequence");
        pcounter = new vsnodeid[node_num];
        for(int i = 0; i < node_num; ++i)
          pcounter[i] = 0; /* record how many times a vlaser id appears */
        pcounter[my_id] = 1;
        for(int i = 1; i < node_num; ++i) {
          /* collect every nodes' flag */
          VLASER_DEB("waiting any node for test flag");
          socktmp = accept(ctl_listen_socket, NULL, NULL);
          recv(socktmp, pbuf, sizeof(vsnodeid) * 2, TCP_RECV_FLAG);

          VLASER_DEB("got flag "<<pbuf[1]<<" from node "<<pbuf[0]);
          close(socktmp);

          if(pbuf[0] < node_num) {
            if(!pcounter[pbuf[0]])
              pcounter[pbuf[0]] = 1;
            else
              throw mpal_logic_error("vlaser id collision detected: from mpal_socket::Test()");
          }
          else
            throw mpal_logic_error("some node send test flag with broken vlaser id: from mpal_socket::Test()");
          if(!pbuf[1])
            result_flag = 0;
        }
        delete[] pcounter;
        if(!flag)
          result_flag = 0;
        /* send the result flag to every node */
        for(int i = 1; i < node_num; ++i) {
          paddr.sin_family = AF_INET;
          paddr.sin_addr.s_addr = addr_list[i];
          paddr.sin_port = ctl_channel_port;
          if((ack_send_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
            throw mpal_runtime_error("getting ack_send_socket failed: from mpal_socket::Test()");
          VLASER_DEB("connecting "<<i<<"in addr_list for sending test result flag");
          while((cflag = connect(ack_send_socket, (struct sockaddr *)&paddr, sizeof(paddr))) != 0 && errno == ETIMEDOUT)
            usleep(TIMEOUT_RETRY_INTERVAL);
          if(cflag < 0)
            throw mpal_runtime_error("test broadcasting connecting failed: from mpal_socket::Test()");
          send(ack_send_socket, &result_flag, sizeof(vsnodeid), TCP_SEND_FLAG);
          VLASER_DEB("sent result flag to "<<i<<"in addr_list");
          close(ack_send_socket);
        }
      }
      else { /* Fro the case that I'm not host node, send the flag and wait for the result flag */
        paddr.sin_family = AF_INET;
        paddr.sin_addr.s_addr = addr_list[0];
        paddr.sin_port = ctl_channel_port;
        if((req_send_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
          throw mpal_runtime_error("getting req_send_socket failed: from mpal_socket::Test()");
        VLASER_DEB("connecting host for sending test flag, my flag is "<<(int)flag);
        while((cflag = connect(req_send_socket, (struct sockaddr *)&paddr, sizeof(paddr))) != 0 && errno == ETIMEDOUT)
          usleep(TIMEOUT_RETRY_INTERVAL);
        if(cflag < 0)
          throw mpal_runtime_error("test gather connecting failed: from mpal_socket::Test()");
        send(req_send_socket, &my_id, sizeof(vsnodeid), TCP_SEND_FLAG);
        result_flag = flag; /* my flag, cast unsigned char to vsnodeid type */
        send(req_send_socket, &result_flag, sizeof(vsnodeid), TCP_SEND_FLAG);
        VLASER_DEB("test flag send ok");
        close(req_send_socket);
        VLASER_DEB("waiting host for test result");

        socktmp = accept(ctl_listen_socket, NULL, NULL);
        recv(socktmp, &result_flag, sizeof(vsnodeid), TCP_RECV_FLAG);
        VLASER_DEB("test result got, the result flag is "<<result_flag);
        close(socktmp);
      }
      is_busy = 0;
    }
    else
      throw mpal_logic_error("communication environment not initialized: mpal_socket::Test()");
    VLASER_DEB("Test() ok");
    return (int)result_flag;
  }

  int
  mpal_socket::Finalize()
  {
    if(is_initialized) {
      while(is_busy)
        usleep(10000);
      close(req_listen_socket);
      close(ack_listen_socket);
      close(ser_listen_socket);
      close(ctl_listen_socket);
      is_initialized = 0;
      VLASER_DEB("mpal_socket Finalize() ok");
    }
    return 0;
  }
  
  inline int
  mpal_socket::SendProto(my_inet_port_t the_port, vsnodeid dest, int tag, vsbyte* buf, int count)
  {
    struct sockaddr_in paddr;
    int cflag, the_socket, c, i;
    int send_counter;
    unsigned char* pbuf;
    
    if(dest >= node_num)
      throw mpal_logic_error("sending message to an error dest: from mpal_socket::SendProto()");
    paddr.sin_family = AF_INET;
    paddr.sin_addr.s_addr = id_map_vlaser_to_inet[dest];
    paddr.sin_port = the_port;
    if((the_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
      throw mpal_runtime_error("getting send_socket failed: from mpal_socket::SendProto()");
    i = 1;
    if(setsockopt(the_socket, IPPROTO_TCP, TCP_NODELAY, &i, sizeof(int)) < 0)
      throw mpal_runtime_error("set send_socket tcp_nodelay error: from mpal_socket::SendProto()");
    VLASER_DEB("connecting "<<dest<<" in SendProto()");
    while((cflag = connect(the_socket, (struct sockaddr *)&paddr, sizeof(paddr))) != 0 && errno == ETIMEDOUT)
      usleep(TIMEOUT_RETRY_INTERVAL);
    if(cflag < 0)
      throw mpal_runtime_error("req send connecting failed: from mpal_socket::SendProto()");

    if(send(the_socket, &my_id, sizeof(vsnodeid), TCP_SEND_FLAG) != sizeof(vsnodeid))
      throw mpal_runtime_error("send less bytes than expected when sending my_id: from mpal_socket::SendProto()");
    c = recv(the_socket, &i, sizeof(vsnodeid), TCP_RECV_FLAG);
    if(c != sizeof(int))
      throw mpal_runtime_error("lost send confirming: from mpal_socket::SendProto()");

    VLASER_DEB("got send confirming");
    if(send(the_socket, &tag, sizeof(int), TCP_SEND_FLAG) != sizeof(int))
      throw mpal_runtime_error("send less bytes than expected when sending tag: from mpal_socket::SendProto()");
    pbuf = (unsigned char*)buf;
    send_counter = count;
    while(send_counter > 0)
      if(send_counter < TCP_SEND_BUFFER_SIZE) {
        c = send(the_socket, pbuf, send_counter, TCP_SEND_FLAG);
        if(c != send_counter)
          throw mpal_runtime_error("socket sent less bytes than expected: from mpal_socket::SendProto()");
        send_counter = 0;
      }
      else {
        c = send(the_socket, pbuf, TCP_SEND_BUFFER_SIZE, TCP_SEND_FLAG);
        if(c != TCP_SEND_BUFFER_SIZE)
          throw mpal_runtime_error("socket sent less bytes than expected: from mpal_socket::SendProto()");
        pbuf += TCP_SEND_BUFFER_SIZE;
        send_counter -= TCP_SEND_BUFFER_SIZE;
      }
    VLASER_DEB("sent "<<dest<<" "<<count<<" bytes message in SendProto()");
    close(the_socket);
    return 0;
  }

  inline int
  mpal_socket::RecvProto(int the_socket, vsnodeid& source, int& tag, vsbyte* buf, int count)
  {
    int recv_sock;
    int c, i;

    VLASER_DEB("waiting in RecvProto()");
    recv_sock = accept(the_socket, NULL, NULL); /* accept a incoming connection from any source */
    i = 1;
    if(setsockopt(recv_sock, IPPROTO_TCP, TCP_NODELAY, &i, sizeof(int)) < 0)
      throw mpal_runtime_error("set recv_sock tcp_nodelay error: from mpal_socket::RecvProtoSend()");
    /* get the client's vlaser id */
    c = recv(recv_sock, &source, sizeof(vsnodeid), TCP_RECV_FLAG);
    if(c != sizeof(vsnodeid))
      throw mpal_runtime_error("receive failed as cannot get source id: from mpal_socket::RecvProto()");
    i = send(recv_sock, &source, sizeof(vsnodeid), TCP_SEND_FLAG);
    if(i == -1) {
      close(recv_sock);
      VLASER_DEB("recieve SYN failed, return -1");
      return -1;
    }
    /* get the message tag */
    c = recv(recv_sock, &i, sizeof(int), TCP_RECV_FLAG);
    if(c != sizeof(int))
      throw mpal_runtime_error("receive failed as cannot get source id: from mpal_socket::RecvProto()");
    if(i == -1) {
      close(recv_sock);
      VLASER_DEB("recieve SYN failed, return -1");
      return -1;
    }
    tag = i;

    c = recv(recv_sock, buf, count, TCP_RECV_FLAG);
    close(recv_sock);
    VLASER_DEB("received "<<c<<" bytes from "<<source<<" with tag "<<tag<<" in RecvProto() with recv confirming return value is "<<source);
    return 0;
  }

  int
  mpal_socket::ReqSend(vsnodeid dest, int tag, vsbyte* buf, int count)
  {
    if(is_initialized) {
      VLASER_DEB("Req sending "<<count<<" bytes REQ to node "<<dest<<" with tag "<<tag);
      return SendProto(req_channel_port, dest, tag, buf, count);
    }
    else
      throw mpal_logic_error("communication environment not initialized: mpal_socket::ReqSend()");
  }


  int
  mpal_socket::WaitReq(vsnodeid& source, int& tag, vsbyte* buf, int count)
  {
    if(is_initialized) {
      VLASER_DEB("waiting REQ from any node");
      return RecvProto(req_listen_socket, source, tag, buf, count);
    }
    else
      throw mpal_logic_error("communication environment not initialized: mpal_socket::WaitReq()");
  }

  int
  mpal_socket::AckSend(vsnodeid dest, int tag, vsbyte* buf, int count)
  {
    if(is_initialized) {
      VLASER_DEB("Ack sending "<<count<<" bytes ACK to node "<<dest<<" with tag "<<tag);
      return SendProto(ack_channel_port, dest, tag, buf, count);
    }
    else
      throw mpal_logic_error("communication environment not initialized: mpal_socket::AckSend()");  
  }

  int
  mpal_socket::WaitAck(vsnodeid source, int& tag, vsbyte* buf, int count)
  {
    vsnodeid source_comp;

    if(is_initialized) {
      VLASER_DEB("waiting ACK from node "<<source);
      RecvProto(ack_listen_socket, source_comp, tag, buf, count);
      if(source_comp != source)
        throw mpal_runtime_error("received an unexpected source's Ack: mpal_socket::WaitAck()");
      return 0;
    }
    else
      throw mpal_logic_error("communication environment not initialized: mpal_socket::WaitAck()");
  }

  int
  mpal_socket::SerSend(vsnodeid dest, int tag, vsbyte* buf, int count)
  {
    if(is_initialized) {
      VLASER_DEB("Ser sending "<<count<<" bytes SER to node "<<dest<<" with tag "<<tag);
      return SendProto(ser_channel_port, dest, tag, buf, count);
    }
    else
      throw mpal_logic_error("communication environment not initialized: mpal_socket::SerSend()");  
  }
  
  int
  mpal_socket::WaitSer(vsnodeid source, int& tag, vsbyte* buf, int count)
  {
    vsnodeid source_comp;

    if(is_initialized) {
      VLASER_DEB("waiting SER from node "<<source);
      RecvProto(ser_listen_socket, source_comp, tag, buf, count);
      if(source_comp != source)
        throw mpal_runtime_error("received an unexpected source's Ack: mpal_socket::WaitSer()");
      return 0;
    }
    else
      throw mpal_logic_error("communication environment not initialized: mpal_socket::WaitSer()");
  }

  int
  mpal_socket::TrySerReqSend(vsnodeid dest, int tag, vsbyte* buf, int count)
  {
    struct sockaddr_in paddr;
    int cflag, the_socket, c, i;
    int send_counter;
    unsigned char* pbuf;
    struct timeval myto;
    struct linger mylinger;

    VLASER_DEB("SerReq sending "<<count<<" bytes SERREQ to node "<<dest<<" with tag "<<tag);
    if(is_initialized) {
      if(dest >= node_num)
        throw mpal_logic_error("sending message to an error dest: from mpal_socket::TrySerReqSend()");
      paddr.sin_family = AF_INET;
      paddr.sin_addr.s_addr = id_map_vlaser_to_inet[dest];
      paddr.sin_port = req_channel_port;
      if((the_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
        throw mpal_runtime_error("getting send_socket failed: from mpal_socket::TrySerReqSend()");
      i = 1;
      if(setsockopt(the_socket, IPPROTO_TCP, TCP_NODELAY, &i, sizeof(int)) < 0)
        throw mpal_runtime_error("set send_socket tcp_nodelay error: from mpal_socket::TrySerReqSend()");
      myto.tv_sec = SERREQSEND_TIMEOUT / 1000000;
      myto.tv_usec = SERREQSEND_TIMEOUT % 1000000;
      if(setsockopt(the_socket, SOL_SOCKET, SO_RCVTIMEO, &myto, sizeof(myto)) < 0)
        throw mpal_runtime_error("set send_socket receive timeout sockopt failed: from mpal_socket::TrySerReqSend()");
      VLASER_DEB("connecting "<<dest<<" in SerReqSend()");
      while((cflag = connect(the_socket, (struct sockaddr *)&paddr, sizeof(paddr))) != 0 && errno == ETIMEDOUT)
        usleep(TIMEOUT_RETRY_INTERVAL);
      if(cflag < 0)
        throw mpal_runtime_error("req send connecting failed: from mpal_socket::TrySerReqSend()");

      if(send(the_socket, &my_id, sizeof(vsnodeid), TCP_SEND_FLAG) != sizeof(vsnodeid))
        throw mpal_runtime_error("send less bytes than expected when sending my_id: from mpal_socket::TrySerReqSend()");
      VLASER_DEB("sent "<<dest<<" SER message in SerReqSend(), now wait the SER confirming");
      c = recv(the_socket, &i, sizeof(vsnodeid), TCP_RECV_FLAG);
      if(c == -1) {
        VLASER_DEB("wait SER confirming timeout, now return -1");
        i = -1;
        c = send(the_socket, &i, sizeof(int), TCP_SEND_FLAG);
        if(c != sizeof(int))
          throw mpal_logic_error("sending -1 tag failure message error: from mpal_socket::TrySerReqSend()");
        close(the_socket);
        return -1;
      }
      if(c != sizeof(vsnodeid))
        throw mpal_logic_error("received broken SER confirming: from mpal_socket::TrySerReqSend()");

      VLASER_DEB("got SER confirming");
      if(send(the_socket, &tag, sizeof(int), TCP_SEND_FLAG) != sizeof(int))
        throw mpal_runtime_error("send less bytes than expected when sending tag: from mpal_socket::TrySerReqSend()");
      if(send(the_socket, buf, count, TCP_SEND_FLAG) != count)
        throw mpal_runtime_error("send less bytes than expected when sending message: from mpal_socket::TrySerReqSend()");
      close(the_socket);
    }
    else
      throw mpal_logic_error("communication environment not initialized: mpal_socket::TrySerReqSend()");
    return 0;
  }

} //end namespace vlaser
