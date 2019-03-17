/* 
 * Virtual Linear Address SERvice
 *
 * Author :Liu Peng-Hong  Institute of Scientific Computing, Nankai Univ.
 *
 * Message Passing Socket Implementation
 * class vlaser::mpal_socket
 * Header File
 *
 * Mar 26, 2011  Original Design
 *
 */

#ifndef _VLASER_MPAL_SOCKET_H_
#define _VLASER_MPAL_SOCKET_H_

#include "vstype.h"
#include "mpal.h"
#include <arpa/inet.h>
#include <string>
#include <stdexcept>

namespace vlaser {

  /*
   * CLASS mpal_socket
   *
   * Class mpal_socket is Linux socket/TCP implementation of
   * abstract bass class mpal.
   */

  class mpal_socket : public mpal {
  public:
    mpal_socket(vsnodeid id, vsnodeid num, char my_addr_path[], char hosts_addr_path[]);
    virtual ~mpal_socket();
    
    /* standard interface derives from mpal */

    int Test(unsigned char flag); 
    int Initialize();
    int Finalize();
    
    int ReqSend(vsnodeid dest, int tag, vsbyte* buf, int count);
    int WaitReq(vsnodeid& source, int& tag, vsbyte* buf, int count); 
    int AckSend(vsnodeid dest, int tag, vsbyte* buf, int count);
    int WaitAck(vsnodeid source, int& tag, vsbyte* buf, int count);
    int SerSend(vsnodeid dest, int tag, vsbyte* buf, int count);
    int WaitSer(vsnodeid source, int& tag, vsbyte* buf, int count);
    int TrySerReqSend(vsnodeid dest, int tag, vsbyte* buf, int count);

    /* class interface end */
    
  private:

    typedef uint32_t my_inet_addr_t; // type of ipv4 address
    typedef in_port_t my_inet_port_t; // type of port number
    volatile int is_initialized;
    volatile int is_busy;

    my_inet_addr_t my_inet_addr; // ip address of this node
    my_inet_addr_t* id_map_vlaser_to_inet; // translate vlaser node id to ip adress
    my_inet_addr_t* addr_list; //store all the node's ip address, include this node

    int req_listen_socket; // socket for listening request channel, waiting for the request's coming in
    int ack_listen_socket; // socket for listening acknowledgment channel
    int ser_listen_socket; // socket for listening service channel
    int ctl_listen_socket; // control channel for Test() member function

    my_inet_port_t req_channel_port; // request channel tcp port
    my_inet_port_t ack_channel_port; // acknowledgement channel tcp port
    my_inet_port_t ser_channel_port; // service channel tcp port
    my_inet_port_t ctl_channel_port; // control channel tcp port

    std::string my_addr_file_path; // path of the local node ip address file
    std::string hosts_addr_file_path; // path of all nodes' ip address file

    /* read all ip address from file */
    void GetInetHosts();
    /* create and listen three channel sockets */
    void PrepareSocks();

    int SendProto(my_inet_port_t the_port, vsnodeid dest, int tag, vsbyte* buf, int count);

    int RecvProto(int the_socket, vsnodeid& source, int& tag, vsbyte* buf, int count);

  }; //end class mpal_socket declaration

} //end namespace vlaser

#endif //#ifndef _VLASER_MPAL_SOCKET_H_

