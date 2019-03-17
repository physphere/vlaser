/* 
 * Virtual Linear Address SERvice
 *
 * Author :Liu Peng-Hong  Institute of Scientific Computing, Nankai Univ.
 *
 * Message Passing Abstract Layer
 * abstract class vlaser::mpal
 * Header File
 *
 * Feb 14, 2011  Original Design
 *
 */

#ifndef _VLASER_MPAL_H_
#define _VLASER_MPAL_H_

#include "vstype.h"
#include <stdexcept>

namespace vlaser {

  /*
   * CLASS mpal
   * 
   * Class mpal abstracts the communication
   * behaviors between nodes.
   *
   * 1) three channel for the messages:
   * request channel, acknowledgment channel, and service channel.
   * 2) Primitives are blocking methods, when it 
   * returns, the buf is ready for reusing.
   * 3) Primitives are all thread safe.
   * 4) Node id must be set by user when constructing.
   *
   */

  class mpal {
  public:

    class mpal_logic_error : public std::logic_error {
    public:
      mpal_logic_error(const char* msg = "") : logic_error(msg) {}
    };
    class mpal_runtime_error : public std::runtime_error {
    public:
      mpal_runtime_error(const char* msg = "") : runtime_error(msg) {}
    };

    //constructor
    mpal(vsnodeid id, vsnodeid num) : my_id(id), node_num(num) {}

    //destructor
    virtual ~mpal() {}

    /* class interface */
    const vsnodeid my_id; /* global unique identifier, used for identifying every node */
    const vsnodeid node_num;

    virtual int Test(unsigned char flag) = 0;

    virtual int Initialize() = 0; 

    virtual int Finalize() = 0;

    /* request channel methods, always success */
    virtual int ReqSend(vsnodeid dest, int tag, vsbyte* buf, int count) = 0; 

    virtual int WaitReq(vsnodeid& source, int& tag, vsbyte* buf, int count) = 0; 

    /* acknowledgment channel methods, always success */
    virtual int AckSend(vsnodeid dest, int tag, vsbyte* buf, int count) = 0;

    virtual int WaitAck(vsnodeid source, int& tag, vsbyte* buf, int count) = 0;

    /* service channel methods, always success */
    virtual int SerSend(vsnodeid dest, int tag, vsbyte* buf, int count) = 0;
    
    virtual int WaitSer(vsnodeid source, int& tag, vsbyte* buf, int count) = 0;

    /* timeout version of ReqSend()
     * this method will return -1 when request timeouts
     */
    virtual int TrySerReqSend(vsnodeid dest, int tag, vsbyte* buf, int count) = 0; 

    /* class interface end */

  protected:

    enum MessageChannel {
      MPAL_REQ, MPAL_ACK, MPAL_COMFIRM
    };

  }; //end class mpal declaration

} //end namespace vlaser

#endif //#ifndef _VLASER_MPAL_H_
