/* 
 * Virtual Linear Address SERvice
 *
 * Author :Liu Peng-Hong  Institute of Scientific Computing, Nankai Univ.
 *
 * Coherence Protocol Layer
 * class vlaser::cpl
 * Header File
 *
 * Feb 19, 2011  Original Design
 *
 */

#ifndef _VLASER_CPL_H_
#define _VLASER_CPL_H_

#include "vstype.h"
#include "vsmutex.h"
#include "vscache.h"
#include "mpal.h"
#include "lsal.h"
#include <pthread.h>
#include <set>

namespace vlaser {

  /*
   * CLASS cpl
   * 
   * Class cpl uses the sequential consistency
   * as memory consistency model and MESI cache 
   * coherence protocol.
   *
   * 1) Class cpl uses a second thread as service thread for
   * handling coherence protocol requests from other nodes.
   * All the requests will queue in message passing abstract
   * layer and will be processed one at a time.
   * 2) All requests have their acknowledgement message.
   * 3) Class cpl uses point-to-point communication, and
   * does not support broadcasting.
   * 4) The local directory is used to record the memory block's
   * cache-copy holder and the cache status.
   *
   */

  class cpl {
  public:
    
    class cpl_runtime_error : public std::runtime_error {
    public:
      cpl_runtime_error(const char* msg = "") : runtime_error(msg) {}
    };
    class cpl_logic_error : public std::logic_error {
    public:
      cpl_logic_error(const char* msg = "") : logic_error(msg) {}
    };

    /* constructor's parameters are:
     * the block size, number of blocks that the cache has,
     * number of blocks the local storage has, the node's vlaser id,
     * number of nodes, message passing abstract layer's pointer
     * local storage abstract layer's pointer
     */
    cpl(BlockSize bsize, vsaddr csize, vsaddr lvolume, vsnodeid thisid, vsnodeid nm, mpal* pmp, lsal* pls);

    virtual ~cpl();

    /* random read and write primitive for the global virtual address */
    int Read(globaladdress gd, vsbyte* buf, int count);

    int Write(globaladdress gd, vsbyte* buf, int count);

    /* initialize the coherence protocol enviroment.
     * this method will also initialize the message passing
     * abstract layer and local storage abstract layer
     */
    void Initialize();
    
    /* this method can be called from any node,
     * but only the very first calling starts the shutdown sequence,
     * following calls are neglected.
     */
    void ShutDown();
    
    /* when the main thread has nothing to do any more,
     * call this method to wait the whole system's shutdown.
     */
    void WaitShutDown();

    const int block_size;
    const vsaddr local_block_num; // how many blocks local storage has
    const vsaddr cache_block_num; // how many blocks cache has
    const vsnodeid my_id;
    const vsnodeid node_num; // how many nodes the whole system has

    const int cache_holder_advice_num;

  protected:

    /* coherence protocol message's tags */
    enum MessageTag {
      TAG_REQ_BLOCK                = 0, //request a read only block
      TAG_REQ_CACHED_BLOCK         = 1, //request a block in cache, but in local storage
      TAG_REQ_BLOCK_THIS_NODE      = 2, //force to request the block from the specific node
      TAG_REQ_BLOCK_EXCLUSIVE      = 3, //request the block as exclusive
      TAG_REQ_EXCLUSIVE            = 4, //request a block to be marked as exclusive
      TAG_REQ_WRITEBACK            = 5, //request writing back a dirty block

      TAG_SET_INVALID              = 6, //set a block as invalid
      TAG_SET_SHARED               = 7, //set a block as shared

      TAG_SHUTDOWN                 = 8, //shutdown signal
      TAG_FINISH                   = 9, //finish signal

      TAG_SELF_REQ_BLOCK           = 10, //request a read only block from local serivce
      TAG_SELF_REQ_BLOCK_EXCLUSIVE = 11, //request a exclusive block from local service

      TAG_ACK_BLOCK_SHARED         = 21, //ack the block as shared
      TAG_ACK_BLOCK_EXCLUSIVE      = 22, //ack the block as exclusived
      TAG_ACK_ASK_OTHER            = 23, //ack the advice list
      TAG_ACK_NOBLOCK              = 24, //ack no such block 
      TAG_ACK_CONFIRM              = 25, //ack confirm
      TAG_ACK_RETRY                = 26, //request failed because of ser sending timeout
      TAG_ACK_NOT_HOLDER           = 27, //ack the overdue request

      TAG_SER_SET_WRITEBACK        = 28, //ack to a set with the block writeback
      TAG_SER_SET_CONFIRM          = 29, //confirm the set
      TAG_SER_READY                = 30,
      TAG_SER_BEGIN                = 31
    };

    /* member function pointer table for the 12 requests' response procedures */
    void (cpl::*resp_table[12])(vsnodeid, vsaddr, vsaddr);

    /* request response methods */
    void Resp_req_block(vsnodeid, vsaddr, vsaddr);
    void Resp_req_cached_block(vsnodeid, vsaddr, vsaddr);
    void Resp_req_block_this_node(vsnodeid, vsaddr, vsaddr);
    void Resp_req_block_exclusive(vsnodeid, vsaddr, vsaddr);
    void Resp_req_exclusive(vsnodeid, vsaddr, vsaddr);
    void Resp_req_writeback(vsnodeid, vsaddr, vsaddr);
    void Resp_set_invalid(vsnodeid, vsaddr, vsaddr);
    void Resp_set_shared(vsnodeid, vsaddr, vsaddr);
    void Resp_shutdown(vsnodeid, vsaddr, vsaddr);
    void Resp_finish(vsnodeid, vsaddr, vsaddr);
    void Resp_self_req_block(vsnodeid, vsaddr, vsaddr);
    void Resp_self_req_block_exclusive(vsnodeid, vsaddr, vsaddr);

    void MakeRespTable();

    /* backoff_counter records how many times the Backoff()
     * method has been called yet
     */
    int backoff_counter;
    void MakeRandomSeed();
    void Backoff(); /* backoff for a interval */
    void CleanBackoffCounter(); /* clean the backoff_counter to 0 */

    /* three statuses of a block in local directory */
    enum StorageDirectoryStatus {
      DIR_EXCLUSIVE,
      DIR_SHARED,
      DIR_NONCACHED,
    };


    typedef std::set<vsnodeid> TypeOfHolderList;

    typedef struct {
      StorageDirectoryStatus status;
      TypeOfHolderList holders;
    } StorageDirectory;


    /*
     * multi-thread-shared resources
     */
    vscache* plocal_cache;
    mpal* pmessage_passing;
    lsal* plocal_storage;
    StorageDirectory* local_dir;
    
    /* mutex for local directory, cache, and local stroage
     * respectively
     */
    vlamutex dir_mutex, cache_mutex, storage_mutex;
    volatile int finish_signal; //finish signal used for shutdown sequence
    volatile int is_message_service_ready;
    volatile int io_busy; //indecates if read or write method is running

    const int message_buf_size;

    /*
     * message service thread's private resource
     */
    vsbyte* send_buf;
    vsbyte* recv_buf;
    
    /*
     * indicates which node start the shutdown sequence
     */
    vsnodeid finish_signal_source;

    /*
     * main thread private resource
     */
    vsbyte* message_buf;
    vsbyte* vsaddr_tag_only_buf;
    pthread_t service_thread_id;

    void PackAddr(vsaddr addr, vsbyte* packed);
    void UnpackAddr(vsbyte* packed, vsaddr& addr);

    /*
     * output a list of node ids which are holding a specific block
     */
    int AdviseCacheHolder(vsaddr localaddr, vsbyte* mes);

    /*
     * update the specific block's directory status according to
     * the expected new status st and the requester's id source_id.
     * the block is indecated by glocaladdr and localaddr
     */
    int UpdateDirectory(vsaddr globaladdr, vsaddr localaddr, StorageDirectoryStatus st, vsnodeid source_id);

    /* wait a request and then handle it*/
    void CopeWithOneReq();
    
    /* service thread main code */
    void MessageServiceThread();

    static void* _pthread_routine(void* pclass);

    void WriteWithinBlock(vsaddr addr, int startpoint, int count, vsbyte* buf);
    void ReadWithinBlock(vsaddr addr, int startpoint, int count, vsbyte* buf);

  }; //end class cpl declaration

} //end namespace vlaser

#endif //ifndef _VLASER_CPL_H_
