/* 
 * Virtual Linear Address SERvice
 *
 * Author :Liu Peng-Hong  Institute of Scientific Computing, Nankai Univ.
 *
 * Coherence Protocol Layer
 * class vlaser::cpl
 * Source File
 *
 * Feb 19, 2011  Original Design
 *
 */

#define BACKOFF_INTERVAL_UNIT 20000 //microsecond
#define BACKOFF_COUNTER_MAX 8

#include "cpl.h"
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>
#include <signal.h>
#include <cstring>
#include <iostream>

namespace vlaser {

  /*
   * Implementation for class cpl
   */

  inline void
  cpl::PackAddr(vsaddr addr, vsbyte* packed)
  {
    for(int i = 0; i < sizeof(vsaddr); ++i) {
      packed[i] = addr % 256;
      addr /= 256;
    }
    return;
  }
 
  inline void
  cpl::UnpackAddr(vsbyte* packed, vsaddr& addr)
  {
    addr = 0;
    for(int i = sizeof(vsaddr) - 1; i >= 0; --i) {
      addr *= 256;
      addr += packed[i];
    }
    return;
  }

  inline int
  cpl::AdviseCacheHolder(vsaddr localaddr, vsbyte* mes)
  {
    int i;
    int j = local_dir[localaddr].holders.size();
    TypeOfHolderList::iterator tmp = local_dir[localaddr].holders.begin();
    vsbyte* my_check = mes;

    /* if this cache block is being held by more vlaser
     * nodes than cache_holder_advice_num + 1, only give
     * the number of cache_holder_advice_num's advised nodes
     */
    VLASER_DEB("making advice of cache holders");
    if(j < 2)
      throw cpl_logic_error("AdviseCacheHolder called when less than 2 holders in holder list: from cpl::AdviseCacheHolder()");
    if(j > cache_holder_advice_num + 1)
      j = cache_holder_advice_num;
    else
      --j; /* pre-exclude this node from the holder list, despite whether this node is actually holding it */

    VLASER_DEB("will advise "<<j<<" holders");
    PackAddr(j, mes);
    mes += sizeof(vsaddr);
    i = j;
    while(i > 0) {
      if(*tmp != my_id) {
        PackAddr(*tmp, mes);
        mes += sizeof(vsaddr);
        --i;
      }
      ++tmp;
    }
    if((my_check + (j + 1) * sizeof(vsaddr)) != mes)
      throw cpl_logic_error("local storage directory entry contain duplicate VLASER node id: from cpl::AdviseCacheHolder()");
    VLASER_DEB("making advice of cache holders ok");
    /* return how many bytes had been written to mes */
    return (sizeof(vsaddr) * (j + 1));
  }

  int
  cpl::UpdateDirectory(vsaddr globaladdr, vsaddr localaddr, cpl::StorageDirectoryStatus st, vsnodeid source_id)
  {
    TypeOfHolderList::iterator tmp;
    int tag;
    vscache::BlockStatus cache_tag;
    vscache::BlockStatus cst;
    vsbyte* pbuf;
    int i, s;

    /*
     * 1) if new status is DIR_EXCLUSIVE, it means that source_id node wants
     * an exclusive cache block copy, and is very likely to write it.
     * 2) DIR_SHARED means source_id node now only want a read only duplicate.
     * In this case, DIR_EXCLUSIVE may actually be set to directory if
     * source_id will be the only cache block holder.
     */
    if((st != DIR_EXCLUSIVE) && (st != DIR_SHARED))
      throw cpl_logic_error("given wrong new status for the local storage block: from cpl::UpdateDirectory");

    VLASER_DEB("|UPDIR|updating global block "<<globaladdr<<" which local address is "<<localaddr<<" with new status "<<st<<" and source id "<<source_id);
    VLASER_DEB("|UPDIR|original status is "<<local_dir[localaddr].status);
    switch(local_dir[localaddr].status) {
      case DIR_NONCACHED:
        local_dir[localaddr].status = DIR_EXCLUSIVE;
        local_dir[localaddr].holders.insert(source_id);
        break;

      case DIR_SHARED:
        if(st == DIR_EXCLUSIVE) {
          /* source_id node needs a exclusive cache block copy */
          tmp = local_dir[localaddr].holders.begin();
          PackAddr(globaladdr, send_buf);
          s = 0;
          VLASER_DEB("|UPDIR|now block "<<globaladdr<<" has "<<local_dir[localaddr].holders.size()<<" holders");
          while(tmp != local_dir[localaddr].holders.end()) { /* scan all the block's holders */
            VLASER_DEB("|UPDIR|process node "<<*tmp<<" for ser");
            if(*tmp != source_id) { /* neglect source_id in the holders set */
              if(*tmp == my_id) {
                /* if this node has a copy in local cache, set it as invalid */
                cache_mutex.lock();
                if(plocal_cache->IsCached(globaladdr, cst)) {
                  if(cst == MODIFIED)
                    throw cpl_logic_error("local cache does not agree with local storage directory: from cpl::UpdateDirectory()");
                  plocal_cache->SetBlockStatus(globaladdr, INVALID);
                  VLASER_DEB("|UDIR|set the block as invalid in my cache");
                }
                cache_mutex.unlock();
              }
              else {
                /* if some remote nodes have cache copys of the block, tell them to set invalid */
                VLASER_DEB("|UPDIR|sending TAG_SET_INVALID to "<<*tmp);
                i = pmessage_passing->TrySerReqSend(*tmp, TAG_SET_INVALID, send_buf, sizeof(vsaddr));
                if(i == -1) {
                  VLASER_DEB("|UPDIR|message passing send timeout detected, now return -1 without change the dir status");
                  if(s) {
                    VLASER_DEB("|UPDIR|put source node "<<source_id<<" back to block "<<globaladdr<<"'s dir");
                    local_dir[localaddr].holders.insert(source_id);
                  }
                  return -1;
                }
                /* wait for nodes confirming the block has been set as invalid */
                pmessage_passing->WaitSer(*tmp, tag, recv_buf, message_buf_size);
                if(tag != TAG_SER_SET_CONFIRM)
                  throw cpl_logic_error("remote cache status does not agree with local storage directory: from cpl::UpdateDirectory()");
                VLASER_DEB("|UPDIR|received TAG_SER_SET_CONFIRM from "<<*tmp);
              }
            }
            else
              s = 1;
            local_dir[localaddr].holders.erase(tmp);
            VLASER_DEB("|UPDIR|holders' number changed to "<<local_dir[localaddr].holders.size());
            tmp = local_dir[localaddr].holders.begin();
          }
          /* clean the holder list */
          local_dir[localaddr].status = st;
          local_dir[localaddr].holders.clear();
          local_dir[localaddr].holders.insert(source_id);
        }
        else /* source_id node only wants a read only cache copy, than just add source_id to the holder list */
          local_dir[localaddr].holders.insert(source_id);
        break;

      case DIR_EXCLUSIVE: /* if updating an exclusive block */
        if(local_dir[localaddr].holders.size() != 1)
          cpl_logic_error("the exclusive block's holder list is broken: from cpl::UpdateDirectory()");
        tmp = local_dir[localaddr].holders.begin();
        if(*tmp != source_id) {
          if(st == DIR_SHARED) { /* source_id wants a read only cache copy */
            tag = TAG_SET_SHARED;
            cache_tag = SHARED;
          }
          else { /* source_id wants a exclusive duplicate */
            tag = TAG_SET_INVALID;
            cache_tag = INVALID;
          }
          if(*tmp == my_id) { /* if the block is currently being held by this node */
            cache_mutex.lock();
            if(plocal_cache->IsCached(globaladdr, cst)) {
              if(cst == MODIFIED) { /* if modified, write it back */
                pbuf = plocal_cache->AccessBlock(globaladdr, 0);
                storage_mutex.lock();
                plocal_storage->WrBlock(localaddr, pbuf);
                storage_mutex.unlock();
                VLASER_DEB("|UPDIR|write back the block to local storage");
              }
              /*
               * set the correct cache status
               */
              plocal_cache->SetBlockStatus(globaladdr, cache_tag);
            }
            cache_mutex.unlock();
          }
          else { /* if the block is being held by a remote node */
            PackAddr(globaladdr, send_buf);
            /* ask the remote node to set invalid or set shared */
            VLASER_DEB("|UPDIR|sending TAG_SET_INVALID or TAG_SET_SHARED to "<<*tmp);
            i = pmessage_passing->TrySerReqSend(*tmp, tag, send_buf, sizeof(vsaddr));
            if(i == -1) {
              VLASER_DEB("|UPDIR|message passing send timeout detected, now return -1 without change the dir status");
              return -1;
            }
            pmessage_passing->WaitSer(*tmp, tag, recv_buf, message_buf_size);
            VLASER_DEB("|UPDIR|got SER ACK tagged as "<<tag<<" from "<<*tmp);
            if(tag == TAG_SER_SET_WRITEBACK) {
              /* if write back tag is returned, write it back to local storage */
              storage_mutex.lock();
              plocal_storage->WrBlock(localaddr, recv_buf);
              storage_mutex.unlock();
              VLASER_DEB("|UPDIR|write back the block to local storage");
            }
            else if(tag != TAG_SER_SET_CONFIRM)
              throw cpl_logic_error("remote cache return error tag: from cpl::UpdateDirectory()");
          }
          if(st == DIR_SHARED)
            local_dir[localaddr].holders.insert(source_id);
          else {
            local_dir[localaddr].holders.clear();
            local_dir[localaddr].holders.insert(source_id);
          }
        }
        local_dir[localaddr].status = st;
        break;
    }
    /* return the actual status that has been set to the directory */
    VLASER_DEB("|UPDIR|update block "<< globaladdr << " directory status to "
      <<local_dir[localaddr].status<<" ok, now block globaladdr has "<<local_dir[localaddr].holders.size()<<" holders");
    return (local_dir[localaddr].status);
  } /* cpl::UpdateDirectory method definition end */

  inline void
  cpl::CopeWithOneReq()
  {
    vsnodeid source;
    vsaddr gaddr;
    vsaddr laddr;
    int req;

    VLASER_DEB("waiting a request in CopeWithOnReq()");
    /* wait for a request's incoming */
    if(pmessage_passing->WaitReq(source, req, recv_buf, message_buf_size) == -1) {
      VLASER_DEB("got a false request, now do nothing, just return");
      return;
    }
    UnpackAddr(recv_buf, gaddr);
    VLASER_DEB("got a request as "<<req<<" from source "<<source<<" with gaddr is "<<gaddr);
    laddr = gaddr % local_block_num; /* get the local address from global address */
    if(req < 12 && req >= 0)
      (this->*resp_table[req])(source, gaddr, laddr);
    else
      throw cpl_runtime_error("got wrong req tag: from cpl::CopeWithOneReq");
    return;
  }

  void
  cpl::MakeRespTable()
  {
    resp_table[TAG_REQ_BLOCK] = &cpl::Resp_req_block;
    resp_table[TAG_REQ_CACHED_BLOCK] = &cpl::Resp_req_cached_block;
    resp_table[TAG_REQ_BLOCK_THIS_NODE] = &cpl::Resp_req_block_this_node;
    resp_table[TAG_REQ_BLOCK_EXCLUSIVE] = &cpl::Resp_req_block_exclusive;
    resp_table[TAG_REQ_EXCLUSIVE] = &cpl::Resp_req_exclusive;
    resp_table[TAG_REQ_WRITEBACK] = &cpl::Resp_req_writeback;
    resp_table[TAG_SET_INVALID] = &cpl::Resp_set_invalid;
    resp_table[TAG_SET_SHARED] = &cpl::Resp_set_shared;
    resp_table[TAG_SHUTDOWN] = &cpl::Resp_shutdown;
    resp_table[TAG_FINISH] = &cpl::Resp_finish;
    resp_table[TAG_SELF_REQ_BLOCK] = &cpl::Resp_self_req_block;
    resp_table[TAG_SELF_REQ_BLOCK_EXCLUSIVE] = &cpl::Resp_self_req_block_exclusive;
    return;
  }

  void
  cpl::Resp_req_block(vsnodeid source, vsaddr gaddr, vsaddr laddr)
  {
    vsbyte* pbuf = NULL;
    int i;
    int flag = 0;

    if(gaddr / local_block_num != my_id) {
      /* if the block does not belong to this node, ack as no block */
      VLASER_DEB("ack no such block "<<gaddr);
      pmessage_passing->AckSend(source, TAG_ACK_NOBLOCK, send_buf, 0);
      return;
    }
    dir_mutex.lock();
    if(local_dir[laddr].holders.size() > 2) {
      /* the block has more than two holders but is not being tagged as shared in directory is impossible */
      if(local_dir[laddr].status != DIR_SHARED)
        throw cpl_logic_error("local storage directory entry is broken: from cpl::CopeOneReq()");

      cache_mutex.lock();
      pbuf = plocal_cache->AccessBlock(gaddr, 0);
      if(pbuf != NULL && plocal_cache->IsIntegrity(gaddr)) { /* if cache hits, ack the block to source node by using local cache directly */
        VLASER_DEB("ack the block from local cache");
        pmessage_passing->AckSend(source, TAG_ACK_BLOCK_SHARED, pbuf, block_size);
        flag = 1;
      }
      cache_mutex.unlock();
      if(!flag) { 
        /* if cache misses, give souce node an advice
         * about which remote nodes may be holding the block
         * in cache, and let the source node get the block
         * from the remote nodes' cache first rather than the
         * slower local storage.
         */
        VLASER_DEB("ack a advice cache holder list");
        i = AdviseCacheHolder(laddr, send_buf);
        pmessage_passing->AckSend(source, TAG_ACK_ASK_OTHER, send_buf, i); /* use the message tag TAG_ACK_ASK_OTHER */
      }

      UpdateDirectory(gaddr, laddr, DIR_SHARED, source);
      dir_mutex.unlock();
      return;
    }
    
    /*
     * if less than 2 nodes are holding the block in cache,
     * just unlock the local directory and run the
     * TAG_REQ_BLOCK_THIS_NODE's response sequence
     */
    dir_mutex.unlock();
    Resp_req_block_this_node(source, gaddr, laddr);
    return;
  }

  void
  cpl::Resp_req_cached_block(vsnodeid source, vsaddr gaddr, vsaddr laddr)
  {
    vsbyte* pbuf = NULL;

    cache_mutex.lock();
    /* search it in local cache */
    pbuf = plocal_cache->AccessBlock(gaddr, 0);
    if(pbuf != NULL && plocal_cache->IsIntegrity(gaddr))  {
      VLASER_DEB("ack the cached block "<<gaddr<<" from non-owner node");
      pmessage_passing->AckSend(source, TAG_ACK_BLOCK_SHARED, pbuf, block_size);
    }
    else {
      VLASER_DEB("ack no such cached block "<<gaddr<<" from non-owner node");
      pmessage_passing->AckSend(source, TAG_ACK_NOBLOCK, send_buf, 0);
    }
    cache_mutex.unlock();
    return;
  }

  void
  cpl::Resp_req_block_this_node(vsnodeid source, vsaddr gaddr, vsaddr laddr)
  {
    vsbyte* pbuf = NULL;
    int i, tag, flag = 0;

    if(gaddr / local_block_num != my_id) {
      VLASER_DEB("ack no such block "<<gaddr);
      pmessage_passing->AckSend(source, TAG_ACK_NOBLOCK, send_buf, 0);
      return;
    }
    dir_mutex.lock();
    /*
     * update the block as shared in local storage directory
     */
    VLASER_DEB("will force to ack the block "<<gaddr<<" from this node");
    i = UpdateDirectory(gaddr, laddr, DIR_SHARED, source);
    if(i == -1) {
      VLASER_DEB("update block "<<gaddr<<"'s dir fail, now tell node "<<source<<" retry");
      pmessage_passing->AckSend(source, TAG_ACK_RETRY, send_buf, 0);
      dir_mutex.unlock();
      return;
    }
    if(i == DIR_SHARED)
      tag = TAG_ACK_BLOCK_SHARED;
    else
      tag = TAG_ACK_BLOCK_EXCLUSIVE;
    if(tag == TAG_ACK_BLOCK_SHARED) {
      /* if block is being held by several nodes, it may be cached in local cache */
      cache_mutex.lock();
      pbuf = plocal_cache->AccessBlock(gaddr, 0);
      if(pbuf != NULL && plocal_cache->IsIntegrity(gaddr)) {
        VLASER_DEB("ack the block from local cache");
        pmessage_passing->AckSend(source, tag, pbuf, block_size);
        flag = 1;
      }
      cache_mutex.unlock();
    }
    if(!flag) { /* if local cache misses */
      /* read the block from local storage */
      storage_mutex.lock();
      plocal_storage->RdBlock(laddr, send_buf);
      storage_mutex.unlock();
      VLASER_DEB("ack the block "<<gaddr);
      pmessage_passing->AckSend(source, tag, send_buf, block_size);
    }
    dir_mutex.unlock();
    return;
  }

  void
  cpl::Resp_req_block_exclusive(vsnodeid source, vsaddr gaddr, vsaddr laddr)
  {
    int i;

    if(gaddr / local_block_num != my_id) {
      VLASER_DEB("ack no such block "<<gaddr);
      pmessage_passing->AckSend(source, TAG_ACK_NOBLOCK, send_buf, 0);
      return;
    }
    dir_mutex.lock();
    /* update the local storage directory as exclusive */
    VLASER_DEB("updating directory");
    i = UpdateDirectory(gaddr, laddr, DIR_EXCLUSIVE, source);
    if(i == -1) {
      VLASER_DEB("update block "<<gaddr<<"'s dir fail, now tell node "<<source<<" retry");
      pmessage_passing->AckSend(source, TAG_ACK_RETRY, send_buf, 0);
      dir_mutex.unlock();
      return;
    }

    storage_mutex.lock();
    plocal_storage->RdBlock(laddr, send_buf);
    storage_mutex.unlock();
    VLASER_DEB("ack block as exclusive");
    pmessage_passing->AckSend(source, TAG_ACK_BLOCK_EXCLUSIVE, send_buf, block_size);
    dir_mutex.unlock();
    return;
  }

  void
  cpl::Resp_req_exclusive(vsnodeid source, vsaddr gaddr, vsaddr laddr)
  {
    int i;

    if(gaddr / local_block_num != my_id) {
      VLASER_DEB("ack no such block "<<gaddr);
      pmessage_passing->AckSend(source, TAG_ACK_NOBLOCK, send_buf, 0);
      return;
    }
    dir_mutex.lock();
    if(local_dir[laddr].holders.find(source) != local_dir[laddr].holders.end()) {
      /* if the source node is a holder, set it as exclusive */
      VLASER_DEB("update directory");
      i = UpdateDirectory(gaddr, laddr, DIR_EXCLUSIVE, source);
      if(i == -1) {
        VLASER_DEB("update block "<<gaddr<<"'s dir fail, now tell node "<<source<<" retry");
        pmessage_passing->AckSend(source, TAG_ACK_RETRY, send_buf, 0);
        dir_mutex.unlock();
        return;
      }
      VLASER_DEB("ack confirm");
      pmessage_passing->AckSend(source, TAG_ACK_CONFIRM, send_buf, 0); /* ack confirm*/
    }
    else {
      /* if source node is not in the holder list, it indicates that this
       * is a overdue request, some other node have grabbed the block  
       * before this request being answered. tell the source the failure.
       */
      VLASER_DEB("ack overdue request");
      pmessage_passing->AckSend(source, TAG_ACK_NOT_HOLDER, send_buf, 0);
    }
    dir_mutex.unlock();
    return;
  }

  void
  cpl::Resp_req_writeback(vsnodeid source, vsaddr gaddr, vsaddr laddr)
  {
    if(gaddr / local_block_num != my_id) {
      VLASER_DEB("ack no such block "<<gaddr);
      pmessage_passing->AckSend(source, TAG_ACK_NOBLOCK, send_buf, 0);
      return;
    }
    dir_mutex.lock();
    if(local_dir[laddr].holders.find(source) != local_dir[laddr].holders.end()) {
      if(local_dir[laddr].status == DIR_EXCLUSIVE) {
        /* empty the holder list, and tag the block as uncached */
        VLASER_DEB("clean the holder list of block "<<gaddr);
        local_dir[laddr].status = DIR_NONCACHED;
        local_dir[laddr].holders.clear();
        storage_mutex.lock();
        /* write back to local storage */
        VLASER_DEB("write back block "<<gaddr);
        plocal_storage->WrBlock(laddr, (recv_buf + sizeof(vsaddr)));
        storage_mutex.unlock();
        }
    }
    /* if source node id is not in the holder list, or it is in holder list but 
     * the block status is not DIR_EXCLUSIVE, this indicates that
     * the block had already being writed back, this is due to that source node
     * answered a TAG_SET_INVALID or TAG_SET_SHARED request before sending the
     * write back message, just neglect this writeback request.
     */
    VLASER_DEB("ack confirm");
    pmessage_passing->AckSend(source, TAG_ACK_CONFIRM, send_buf, 0); /* ack confirm in all condition */
    dir_mutex.unlock();
    return;
  }

  void
  cpl::Resp_set_invalid(vsnodeid source, vsaddr gaddr, vsaddr laddr)
  {
    vsbyte* pbuf = NULL;
    vscache::BlockStatus cst;

    cache_mutex.lock();
    if(plocal_cache->IsCached(gaddr, cst)) { /* if local cache hits */
      if(cst == MODIFIED) {/* if it is a dirty cache block, write it back */
        VLASER_DEB("ack write back block "<<gaddr<<" first");
        pbuf = plocal_cache->AccessBlock(gaddr, 0);
        pmessage_passing->SerSend(source, TAG_SER_SET_WRITEBACK, pbuf, block_size);
        plocal_cache->SetBlockStatus(gaddr, INVALID);
        cache_mutex.unlock();
        return;
      }
      VLASER_DEB("set block "<<gaddr<<" invalid");
      plocal_cache->SetBlockStatus(gaddr, INVALID);
    }

    VLASER_DEB("ack set confirm without writing back");
    pmessage_passing->SerSend(source, TAG_SER_SET_CONFIRM, send_buf, 0);
    cache_mutex.unlock();
    return;
  }

  void
  cpl::Resp_set_shared(vsnodeid source, vsaddr gaddr, vsaddr laddr)
  {
    vsbyte* pbuf = NULL;
    vscache::BlockStatus cst;

    cache_mutex.lock();
    if(plocal_cache->IsCached(gaddr, cst)) { /* if local cache hits */
      /* !!! Sharing a shared cache block is not a logic error.
      if(cst == SHARED)
        throw cpl_logic_error("sharing a shared cache block: from cpl::Resp_set_shared()");
       */
      if(cst == MODIFIED){ /* if modified, write back */
        VLASER_DEB("ack write back block "<<gaddr);
        pbuf = plocal_cache->AccessBlock(gaddr, 0);
        pmessage_passing->SerSend(source, TAG_SER_SET_WRITEBACK, pbuf, block_size);
        VLASER_DEB("set block "<<gaddr<<" shared");
        plocal_cache->SetBlockStatus(gaddr, SHARED); /* set it as shared */
        cache_mutex.unlock();
        return;
      }
      VLASER_DEB("set block "<<gaddr<<" shared");
      plocal_cache->SetBlockStatus(gaddr, SHARED);
    }

    VLASER_DEB("ack set confirm without writing back");
    pmessage_passing->SerSend(source, TAG_SER_SET_CONFIRM, send_buf, 0);
    cache_mutex.unlock();
    return;  
  }

  void
  cpl::Resp_shutdown(vsnodeid source, vsaddr gaddr, vsaddr laddr)
  {

    if((my_id == 0) && (finish_signal == 0)) {
      VLASER_DEB("begin shutdown sequence in node 0");
      finish_signal = 1;
      finish_signal_source = my_id;
    }
    return;
  }

  void
  cpl::Resp_finish(vsnodeid source, vsaddr gaddr, vsaddr laddr)
  {
    ++finish_signal;
    VLASER_DEB("finish signal grows to "<<finish_signal);
    if(finish_signal == 1) {
      finish_signal_source = gaddr;
      VLASER_DEB("set finish signal source is "<<gaddr);
    }
    return;
  }

  void
  cpl::Resp_self_req_block(vsnodeid source, vsaddr gaddr, vsaddr laddr)
  {
    int i, tag;

    dir_mutex.lock();
    VLASER_DEB("self request block "<<gaddr<<" from local node");
    i = UpdateDirectory(gaddr, laddr, DIR_SHARED, source);
    if(i == DIR_SHARED)
      tag = TAG_ACK_BLOCK_SHARED;
    else
      tag = TAG_ACK_BLOCK_EXCLUSIVE;
    if(i == -1) {
      VLASER_DEB("update block "<<gaddr<<"'s dir fail, now tell node "<<source<<" retry");
      tag = TAG_ACK_RETRY;
    }
    pmessage_passing->AckSend(source, tag, send_buf, 0);
    dir_mutex.unlock();
    return;
  }

  void
  cpl::Resp_self_req_block_exclusive(vsnodeid source, vsaddr gaddr, vsaddr laddr)
  {
    int i;

    dir_mutex.lock();
    VLASER_DEB("self request block "<<gaddr<<" exclusive from local node");
    i = UpdateDirectory(gaddr, laddr, DIR_EXCLUSIVE, source);
    if(i == -1) {
      VLASER_DEB("update block "<<gaddr<<"'s dir fail, now tell node "<<source<<" retry");
      pmessage_passing->AckSend(source, TAG_ACK_RETRY, send_buf, 0);
      dir_mutex.unlock();
      return;
    }
    pmessage_passing->AckSend(source, TAG_ACK_BLOCK_EXCLUSIVE, send_buf, 0);
    dir_mutex.unlock();
    return;
  }

  inline void
  cpl::MakeRandomSeed()
  {
    struct timeval mytv;
    unsigned int s;

    gettimeofday(&mytv, NULL);
    s = (mytv.tv_sec + mytv.tv_usec) * (my_id + 1) % (1024 * 1024 * 1024);
    VLASER_DEB("the random seed is "<<s);
    srandom(s);
  }

  inline void
  cpl::Backoff()
  {
    unsigned int t;

    /* the binary exponential backoff */
    if(backoff_counter != 0) {
      t = 1 << backoff_counter - 1;
      t = random() % t;
      VLASER_DEB("backoff for "<<t * BACKOFF_INTERVAL_UNIT<<" microsecond");
      usleep(t * BACKOFF_INTERVAL_UNIT);
      if(backoff_counter < BACKOFF_COUNTER_MAX) {
        ++backoff_counter;
      }
    }
    else
      ++backoff_counter;
    return;
  }

  inline void
  cpl::CleanBackoffCounter()
  {
    backoff_counter = 0;
    return;
  }

  cpl::cpl(BlockSize bsize, vsaddr csize, vsaddr lvolume, vsnodeid thisid, vsnodeid nm, mpal* pmp, lsal* pls) :
  block_size(bsize),
  cache_block_num(csize),
  local_block_num(lvolume),
  my_id(thisid),
  node_num(nm),
  plocal_storage(pls),
  pmessage_passing(pmp),
  cache_holder_advice_num(4),
  message_buf_size(bsize + sizeof(vsaddr)) /* largest size of message is block size + one VLASER global address' size */
  {
    plocal_cache = new vscache(bsize, csize);

    local_dir = new StorageDirectory[lvolume];
    for(int i = 0; i < lvolume; ++i)
      local_dir[i].status = DIR_NONCACHED; /* initialize all directory entries as noncached */
    send_buf = new vsbyte[message_buf_size];
    recv_buf = new vsbyte[message_buf_size];
    message_buf = new vsbyte[message_buf_size];
    vsaddr_tag_only_buf = new vsbyte[sizeof(vsaddr)];
    finish_signal = 0;
    is_message_service_ready = 0;
    io_busy = 0;
  }

  cpl::~cpl()
  {
    /*
     * class cpl owns local cache and local storage directory, free them.
     */
    delete plocal_cache;
    delete[] local_dir;
    delete[] send_buf;
    delete[] recv_buf;
    delete[] message_buf;
    delete[] vsaddr_tag_only_buf;
  }

  void
  cpl::MessageServiceThread()
  {
    vsaddr gaddr;
    vsbyte* pbuf;
    vsbyte* ptmp;
    vsnodeid nextid, tmpid;
    int tag;

    VLASER_DEB("enter MessageServiceThread()");
    VLASER_DEB("make the request response methods table");
    MakeRespTable();
    /* initialize the message passing communication environment */
    VLASER_DEB("initialize message passing enviroment");
    try {
      pmessage_passing->Initialize();

      VLASER_DEB("test message passing enviroment");
      pmessage_passing->Test(1);

      VLASER_DEB("initialize local storage");
      plocal_storage->Initialize();
      VLASER_DEB("message service is ready");
      is_message_service_ready = 1;

      /* wait and handle every request until TAG_FINISH message arrives,
       * once TAG_FINISH message arrives, finish_signal will be set to 1
       */
      while(!finish_signal)
        CopeWithOneReq();

      /* when recived the finish message, start the shutdown sequence */
      /* find which node is next to me */
      if(my_id + 1 == node_num)
        nextid = 0;
      else
        nextid = my_id + 1;
      /* pass on the TAG_FINISH signal to next node */
      VLASER_DEB("passing TAG_FINISH signal to node "<<nextid);
      PackAddr(finish_signal_source, send_buf);
      pmessage_passing->ReqSend(nextid, TAG_FINISH, send_buf, sizeof(vsaddr));
      /* continue to answer the requests, until the TAG_FINISH
       * signal arrives for the second time
       */
      while(finish_signal < 2)
        CopeWithOneReq();
      /* once gets the TAG_FINISH message second time,
       * it indicates that all nodes will no longer accept new
       * reading/writing requests.
       * however, here may still have unfinished requests in main thread.
       * so, wait the io_busy changing to 0.
       */
      while(io_busy)
        usleep(BACKOFF_INTERVAL_UNIT);
      dir_mutex.lock();
      cache_mutex.lock();
      storage_mutex.lock();
      /* write back all modified cache block */
      VLASER_DEB("begin local cache cleaning sequence");
      while(plocal_cache->CleanCache(gaddr, &pbuf)) {
        VLASER_DEB("write back cache block "<<gaddr);
        tmpid = gaddr / local_block_num;
        if(tmpid == my_id) 
          /* if it's a local block, just write back to local storage,
           * as no node will acquire any new cache block, so it is
           * unnecessary to update local storage directory.
           */
          plocal_storage->WrBlock(gaddr % local_block_num, pbuf);
        else {/* write back to remote node */
          PackAddr(gaddr, send_buf);
          ptmp = send_buf + sizeof(vsaddr);
          memcpy(ptmp, pbuf, block_size);
          pmessage_passing->ReqSend(tmpid, TAG_REQ_WRITEBACK, send_buf, message_buf_size);
          pmessage_passing->WaitAck(tmpid, tag, recv_buf, message_buf_size);
        }
        VLASER_DEB("write back cache block "<<gaddr<<" ok");
      }
      storage_mutex.unlock();
      cache_mutex.unlock();
      dir_mutex.unlock();
      /* after cleaning local cache, pass on the TAG_FINISH signal
       * second time to start next node's cache cleaning sequence
       */
      VLASER_DEB("passing TAG_FINISH signal second time to node "<<nextid);
      pmessage_passing->ReqSend(nextid, TAG_FINISH, send_buf, 0);
      /* continue to answer the write back request from other nodes
       * until the finish signal arrives for the third time
       */
      while(finish_signal < 3)
        CopeWithOneReq();
      /* once the node recives finish signal for the third time,
       * it says that all nodes' caches are clean, so pass on the
       * signal if next node is not the finish signal's source node,
       * and terminate
       */
      if(nextid != finish_signal_source) {
        VLASER_DEB("passing TAG_FINISH signal third time to node "<<nextid);
        pmessage_passing->ReqSend(nextid, TAG_FINISH, send_buf, 0);
      }
    }
    /* print the all error messages here and rethrow the exceptions */
    catch(std::logic_error& except) {
      std::cout<<"|FATAL| get logic error"<<std::endl<<except.what()<<std::endl
        <<"catched in cpl::MessageServiceThread"<<std::endl
        <<"will not handle it, now rethrow."<<std::endl;
      throw;
    }
    catch(std::runtime_error& except) {
      std::cout<<"|FATAL| get runtime error"<<std::endl<<except.what()<<std::endl
        <<"catched in cpl::MessageServiceThread"<<std::endl
        <<"will not handle it, now rethrow."<<std::endl;
      throw;
    }
    /* finalize the message passing environment */
    is_message_service_ready = 0;
    plocal_storage->Finalize();
    VLASER_DEB("local storage finialized");
    pmessage_passing->Finalize();
    VLASER_DEB("message passing environment finialized, and service thread exit");
    return;
  }

  int
  cpl::Read(globaladdress gd, vsbyte* buf, int count)
  {
    vsaddr endaddr, startaddr;
    vscache::BlockStatus bs;
    vsbyte* ptmp;
    int i;

    /* if this node has been told to terminate, do nothing, just return */
    if(finish_signal || !is_message_service_ready)
      return 0;
    try{
      io_busy = 1;
      VLASER_DEB("begin global random read()");
      endaddr = (gd + count - 1) / block_size;
      startaddr = gd / block_size;

      if(((startaddr / local_block_num) > (node_num - 1)) || ((endaddr / local_block_num) > (node_num - 1)))
        throw cpl_runtime_error("global space address overflow: from cpl::read()");
      /* read all the blocks that being covered by the reading range*/
      if(startaddr == endaddr)
        ReadWithinBlock(startaddr, gd % block_size, count, buf);
      else {
        i = gd % block_size;
        ReadWithinBlock(startaddr, i, block_size - i, buf);
        buf += (block_size - i);
        ++startaddr;
        while(startaddr != endaddr) {
          ReadWithinBlock(startaddr, 0, block_size, buf);
          ++startaddr;
          buf += block_size;
        }
        ReadWithinBlock(startaddr, 0, (gd + count) % block_size, buf);
      }
      io_busy = 0;
    }
    catch(std::logic_error& except) {
      std::cout<<"|FATAL| get logic error"<<std::endl<<except.what()<<std::endl<<"catched in cpl::Read"
        <<std::endl<<"will not handle it, now rethrow."<<std::endl;
      throw;
    }
    catch(std::runtime_error& except) {
      std::cout<<"|FATAL| get runtime error"<<std::endl<<except.what()<<std::endl<<"catched in cpl::Read"
        <<std::endl<<"will not handle it, now rethrow."<<std::endl;
      throw;
    }
    VLASER_DEB("global random read ok");
    return 1;
  }

  void
  cpl::ReadWithinBlock(vsaddr addr, int startpoint, int count, vsbyte* buf)
  {
    vsbyte* ptmp;
    vsbyte* pmes;
    int swap_flag;
    int wb_flag;
    int holders_flag;
    vsaddr i, n;
    vsaddr swap_addr;
    vsnodeid tmpid;
    int tag;
    vscache::BlockStatus bs;

    VLASER_DEB("|RD|reading "<<count<<" bytes in block "<<addr<<" start at "<<startpoint);
    cache_mutex.lock();

    ptmp = plocal_cache->AccessBlock(addr, 1);
    if(ptmp != NULL) { /* if local cache hits */
      VLASER_DEB("|RD|local cache hit, return the data directly");
      memcpy(buf, ptmp + startpoint, count);
      cache_mutex.unlock();
      VLASER_DEB("|RD|read block "<<addr<<" from cache ok");
      return;
    }
    /* if cache misses */
    wb_flag = 0;
    /* find a cache line to store this new block */
    swap_flag = plocal_cache->FindReplacingBlock(swap_addr, wb_flag);
    if(wb_flag) {
      VLASER_DEB("|RD|write back block "<<swap_addr<<" first");

      ptmp = plocal_cache->AccessBlock(swap_addr, 0);
      /* unlock the local cache before we send the write back message
       * to avoid deadlock
       * notice that call pmessage_passing->ReqSend() method with holding
       * the local cache lock will inevitably lead to deadlock. 
       */
      cache_mutex.unlock();

      tmpid = swap_addr / local_block_num;
      if(tmpid != my_id) { /* if a remote block */
        PackAddr(swap_addr, message_buf); 
        pmes = message_buf + sizeof(vsaddr);
        memcpy(pmes, ptmp, block_size);
        /* before we send this writeback message, the block may already been
         * writed back.
         * because we are not holding the local cache's lock, other nodes may
         * be obtaining this block and the SET_INVALID or SET_SHARED messages
         * from them has already arrived and been answered.
         * the block's owner node will handle this correctly.
         */
        VLASER_DEB("|RD|request writing back to node "<<tmpid);
        pmessage_passing->ReqSend(tmpid, TAG_REQ_WRITEBACK, message_buf, message_buf_size);
        pmessage_passing->WaitAck(tmpid, tag, message_buf, message_buf_size);
      }
      else { /* if the writeback block is a local storage block */
        VLASER_DEB("|RD|writing back to local storage");

        dir_mutex.lock();
        /* lock the local cache again
         * notice that when using these locks, we must follow the order,
         * locking order: dir_mutex, cache_mutex, storage_mutex
         * unlocking order: storage_mutex, cache_mutex, dir_mutex
         */
        cache_mutex.lock();
        if((ptmp = plocal_cache->AccessBlock(swap_addr,0)) != NULL) /* if the block is still in cache */
          if(plocal_cache->GetBlockStatus(swap_addr) == MODIFIED) { /* and if the block is modified */
            n = swap_addr % local_block_num;
            if(local_dir[n].status != DIR_EXCLUSIVE)
              throw cpl_logic_error("find discord between local cache and local dir when local writeback: from cpl::ReadWithinBlock()");
            storage_mutex.lock();
            /* write back to local storage */
            plocal_storage->WrBlock(n, ptmp);
            storage_mutex.unlock();
            local_dir[n].status = DIR_NONCACHED;
            local_dir[n].holders.clear();
          }
        cache_mutex.unlock();
        dir_mutex.unlock();
      }
      VLASER_DEB("|RD|write back block "<<swap_addr<<" ok");
    }
    else
      cache_mutex.unlock();
      
    tmpid = addr / local_block_num;
    VLASER_DEB("|RD|try to get new block "<<addr<<" to cache");
    VLASER_DEB("clean the backoff counter");
    CleanBackoffCounter();
    /* try to obtain the new block */
    if(tmpid == my_id) { /* if it is a local block */
      while(1) { /* keep running this sequence until we get the block */
        /*
         * !!! finish sequence may not answer the self-node-requests,
         * continue retrying may lead to deadlock
         * so if the finish sequence in service thread has already being
         * started, just return here.
         * notice that this will lead the read/write method failure without
         * warning the user.
         *
         */
        if(finish_signal)
          return;
        Backoff();
        cache_mutex.lock();

        ptmp = plocal_cache->PushBlock(addr, EXCLUSIVE, swap_flag, swap_addr);        
        cache_mutex.unlock();
        swap_flag = 0;

        PackAddr(addr, vsaddr_tag_only_buf); 
        VLASER_DEB("|RD|send self request to get block "<<addr);
        pmessage_passing->ReqSend(my_id, TAG_SELF_REQ_BLOCK, vsaddr_tag_only_buf, sizeof(vsaddr));
        pmessage_passing->WaitAck(my_id, tag, message_buf, message_buf_size);
        if(tag == TAG_ACK_RETRY) {
          VLASER_DEB("|RD|self request fail, as TAG_ACK_RETRY got");
          cache_mutex.lock();
          plocal_cache->SetBlockStatus(addr, INVALID);
          cache_mutex.unlock();
          continue;
        }
        VLASER_DEB("|RD|self request block "<<addr<<" ok");
        cache_mutex.lock();
        /* now check if the block is still in cache */
        ptmp = plocal_cache->AccessBlock(addr, 1);
        if(ptmp != NULL) {
          storage_mutex.lock();

          n = addr % local_block_num;
          plocal_storage->RdBlock(n, ptmp);
          storage_mutex.unlock();
          /* if self request procedure returns exclusive status, we also change cache status to EXCLUSIVE */
          if(tag == TAG_ACK_BLOCK_SHARED)
            plocal_cache->SetBlockStatus(addr, SHARED);

          plocal_cache->SetIntegrity(addr);
          memcpy(buf, ptmp + startpoint, count); /* give the data back to user */
          cache_mutex.unlock();
          VLASER_DEB("|RD|read block "<<addr<<" from local storage ok");
          return;
         }
        else {
          cache_mutex.unlock();
          VLASER_DEB("|RD|the block "<<addr<<" had been grabbed from my cache, now try again");
        }
      }
    }
    else while(1) { /* obtain the block from remote node */
      Backoff();
      tmpid = addr / local_block_num; 
      /*
       * we are not sure that we can get the block with one try,
       * so we keep running this while(1){} sequence until we get the block
       *
       */
      cache_mutex.lock();
      /* we push the new block in advance and set it as exclusive,
       * but no other nodes know that we have this copy at the moment
       * because the directory has not been updated.
       */
      ptmp = plocal_cache->PushBlock(addr, EXCLUSIVE, swap_flag, swap_addr);        
      cache_mutex.unlock();
      swap_flag = 0;
      /* sent message to the owner node to acquire the block */
      VLASER_DEB("|RD|request new block from node "<<tmpid);
      PackAddr(addr, vsaddr_tag_only_buf); 
      pmessage_passing->ReqSend(tmpid, TAG_REQ_BLOCK, vsaddr_tag_only_buf, sizeof(vsaddr));
      pmessage_passing->WaitAck(tmpid, tag, message_buf, message_buf_size);
      if(tag == TAG_ACK_ASK_OTHER) {
        holders_flag = 0;
        VLASER_DEB("|RD|be told to ask other");
        UnpackAddr(message_buf, n);
        VLASER_DEB("|RD|got "<<n<<" nodes in holder list");
        i = n;
        while(i > 0) { /* ask each node in the list for the cache block */
          UnpackAddr(message_buf + sizeof(vsaddr) * (n - i + 1), tmpid);
          if(n != my_id) { /* if not me */
            /* send message to acquire the block */
            VLASER_DEB("|RD|asking node "<<tmpid);
            pmessage_passing->ReqSend(tmpid, TAG_REQ_CACHED_BLOCK, vsaddr_tag_only_buf, sizeof(vsaddr));
            pmessage_passing->WaitAck(tmpid, tag, message_buf, message_buf_size);
            if(tag == TAG_ACK_BLOCK_SHARED) {
              VLASER_DEB("|RD|got the block from node "<<tmpid<<"'s cache ok");
              holders_flag = 1;
              break;
            }
          }
          --i;
        }
        if(!holders_flag) { /* if no node in the advice list can provide us with the block */
          /*
           * ask the owner node again with TAG_REQ_BLOCK_THIS_NODE tag, this forces
           * the owner node to provide the block itself
           *
           */
          tmpid = addr / local_block_num;
          VLASER_DEB("|RD|force to get the block from owner node "<<tmpid);
          pmessage_passing->ReqSend(tmpid, TAG_REQ_BLOCK_THIS_NODE, vsaddr_tag_only_buf, sizeof(vsaddr));
          pmessage_passing->WaitAck(tmpid, tag, message_buf, message_buf_size);
        }
      }
      if(tag == TAG_ACK_RETRY) {
        VLASER_DEB("|RD|request block from node "<<tmpid<<" fail, as TAG_ACK_RETRY got");
        cache_mutex.lock();
        plocal_cache->SetBlockStatus(addr, INVALID);
        cache_mutex.unlock();
        continue;
      }
      if((tag != TAG_ACK_BLOCK_SHARED) && (tag != TAG_ACK_BLOCK_EXCLUSIVE))
        throw cpl_logic_error("block's host node return it does not have the block: from cpl::ReadWithinBlock()");
      VLASER_DEB("|RD|got the new block "<<addr<<" ok");
      cache_mutex.lock();
      ptmp = plocal_cache->AccessBlock(addr, 1);
      if(ptmp != NULL) { /* check whether the block is still in cache */
        memcpy(ptmp, message_buf, block_size);
        /* we have set the block as exclusive in advance, so if the status which
         * owner node returned is shared, we set the cache block also as SHARED
         */
        if(tag == TAG_ACK_BLOCK_SHARED)
          plocal_cache->SetBlockStatus(addr, SHARED);
        plocal_cache->SetIntegrity(addr);
        memcpy(buf, ptmp + startpoint, count); /* give the data back to user */
        cache_mutex.unlock();
        VLASER_DEB("|RD|read the block "<<addr<<" ok");
        return;
      }
      else { /* if the block had been set as invalid */
        /* we just release the local cache, and return to the very beginning of while(1){} sequence to try again */
        VLASER_DEB("|RD|the block "<<addr<<" had been grabbed from my cache, now try again");
        cache_mutex.unlock();
      }
    }
  }

  void
  cpl::WriteWithinBlock(vsaddr addr, int startpoint, int count, vsbyte* buf)
  {
    vsbyte* ptmp;
    vsbyte* pmes;
    int swap_flag;
    int wb_flag;
    vsaddr n;
    vsaddr swap_addr;
    vsnodeid tmpid;
    int tag;
    vscache::BlockStatus bs;

    VLASER_DEB("|WR|writing "<<count<<" bytes in block "<<addr<<" start at "<<startpoint);
    /* we first search the block in local cache */
    cache_mutex.lock();
    ptmp = plocal_cache->AccessBlock(addr, 1);
    if(ptmp != NULL) { /* if cache hits */
      bs = plocal_cache->GetBlockStatus(addr);
      if(bs != SHARED) {
        /* set the block as modified */
        VLASER_DEB("|WR|local cache hit, and the block is not shared");
        if(bs == EXCLUSIVE)
          plocal_cache->SetBlockStatus(addr, MODIFIED);
        pmes = ptmp + startpoint;
        memcpy(pmes, buf, count); /* give the data back to user */
        cache_mutex.unlock();
        VLASER_DEB("|WR|write block "<<addr<<" to cache ok");
        return;
      }
    }
    cache_mutex.unlock();

    CleanBackoffCounter();
    PackAddr(addr, vsaddr_tag_only_buf); 
    while(1) { /* we continue running this sequence until we write the block correctly */
      /*
       * !!!finish sequence may can not answer the self-requests,
       * continue retrying may lead to deadlock
       *
       */
      if(finish_signal)
        return;
      Backoff();
      cache_mutex.lock();
      ptmp = plocal_cache->AccessBlock(addr, 0); /* check whether the block is in cache */
      cache_mutex.unlock();
      if(ptmp == NULL) {/* if cache misses */
        wb_flag = 0;
        swap_flag = 0;
        cache_mutex.lock();
        /* find a cache line to store the new block */
        swap_flag = plocal_cache->FindReplacingBlock(swap_addr, wb_flag);
        if(wb_flag) { /* if we need to write back a old dirty block first */
          VLASER_DEB("|WR|write back block "<<swap_addr<<" first");
          ptmp = plocal_cache->AccessBlock(swap_addr, 0);
          cache_mutex.unlock();
          tmpid = swap_addr / local_block_num;
          if(tmpid != my_id) { /* if a remote node */
            PackAddr(swap_addr, message_buf);
            pmes = message_buf + sizeof(vsaddr);
            memcpy(pmes, ptmp, block_size);
            /* send it to its owner node with writeback tag */
            VLASER_DEB("|WR|request writing back to node "<<tmpid);
            pmessage_passing->ReqSend(tmpid, TAG_REQ_WRITEBACK, message_buf, message_buf_size);
            pmessage_passing->WaitAck(tmpid, tag, message_buf, message_buf_size);
          }
          else { /* if writing back a local block */
            VLASER_DEB("|WR|writing back to local storage");
            dir_mutex.lock();
            cache_mutex.lock();
            /* if the block is still in cache and still modified */
            if((ptmp = plocal_cache->AccessBlock(swap_addr,0)) != NULL)
              if(plocal_cache->GetBlockStatus(swap_addr) == MODIFIED) {
                n = swap_addr % local_block_num;
                storage_mutex.lock();
                plocal_storage->WrBlock(n, ptmp);
                storage_mutex.unlock();
                if(local_dir[n].status != DIR_EXCLUSIVE)
                  throw cpl_logic_error("find discord between local cache and local dir when local writeback: from cpl::WriteWithinBlock()");
                local_dir[n].status = DIR_NONCACHED;
                local_dir[n].holders.clear();
              }
              cache_mutex.unlock();
              dir_mutex.unlock();
          }
          VLASER_DEB("|WR|write back block "<<swap_addr<<" ok");
        }
        else /* if no need to writeback, just release the cache lock */
          cache_mutex.unlock();
        tmpid = addr / local_block_num;
        VLASER_DEB("|WR|try to get new block "<<addr<<" to cache");
        if(tmpid == my_id) { /* if a local block */
          cache_mutex.lock();
          /* we push the new block in advance and set it as exclusive */
          ptmp = plocal_cache->PushBlock(addr, EXCLUSIVE, swap_flag, swap_addr);        
          cache_mutex.unlock();

          PackAddr(addr, vsaddr_tag_only_buf); 
          VLASER_DEB("|WR|send self request to write block "<<addr);
          pmessage_passing->ReqSend(my_id, TAG_SELF_REQ_BLOCK_EXCLUSIVE, vsaddr_tag_only_buf, sizeof(vsaddr));
          pmessage_passing->WaitAck(my_id, tag, message_buf, message_buf_size);
          if(tag == TAG_ACK_RETRY) {
            VLASER_DEB("|WR|self request fail, as TAG_ACK_RETRY got");
            cache_mutex.lock();
            plocal_cache->SetBlockStatus(addr, INVALID);
            cache_mutex.unlock();
            continue;
          }
          VLASER_DEB("|WR|self request ok");
          cache_mutex.lock();
          /* now check if the block is still in cache, and is still exclusive */
          ptmp = plocal_cache->AccessBlock(addr, 1);
          if(ptmp == NULL) {
            /* if not in cache any more, return to the very beginning and try again */
            cache_mutex.unlock();
            VLASER_DEB("|WR|the block "<<addr<<" had been grabbed from my cache, now try again");
            continue;
          }
          if(plocal_cache->GetBlockStatus(addr) != EXCLUSIVE) {
            /* if it had been set as shared by other node,
             * still fill the cache with this block, and than return to the beginning to try again */
            storage_mutex.lock();
            n = addr % local_block_num;
            plocal_storage->RdBlock(n, ptmp);
            storage_mutex.unlock();
            plocal_cache->SetIntegrity(addr);
            cache_mutex.unlock();
            VLASER_DEB("|WR|the block "<<addr<<" had already been shared, now try again");
            continue;
          }
          storage_mutex.lock();

          n = addr % local_block_num;
          plocal_storage->RdBlock(n, ptmp);
          storage_mutex.unlock();

          plocal_cache->SetIntegrity(addr);
          pmes = ptmp + startpoint;
          memcpy(pmes, buf, count);
          plocal_cache->SetBlockStatus(addr, MODIFIED);
          cache_mutex.unlock();
          VLASER_DEB("|WR|write block "<<addr<<" from local storage ok");
          return;
        }
        else { /* acquiring the block from remote node */
          cache_mutex.lock();
          /* push the new block as exclusive, not modified, to avoid wrong writeback from this node's service thread */
          ptmp = plocal_cache->PushBlock(addr, EXCLUSIVE, swap_flag, swap_addr);
          cache_mutex.unlock();

          VLASER_DEB("|WR|request new block as exclusive from node "<<tmpid);
          pmessage_passing->ReqSend(tmpid, TAG_REQ_BLOCK_EXCLUSIVE, vsaddr_tag_only_buf, sizeof(vsaddr));
          pmessage_passing->WaitAck(tmpid, tag, message_buf, message_buf_size);
          if(tag == TAG_ACK_RETRY) {
            VLASER_DEB("|WR|request block from node "<<tmpid<<" fail, as TAG_ACK_RETRY got");
            cache_mutex.lock();
            plocal_cache->SetBlockStatus(addr, INVALID);
            cache_mutex.unlock();
            continue;
          }
          /* lock the cache to check whether the block is still in cache and is still exclusive */
          cache_mutex.lock();
          ptmp = plocal_cache->AccessBlock(addr, 1);
          if(ptmp == NULL) {
            /* if not in cache any more, return to the very beginning and try again */
            cache_mutex.unlock();
            VLASER_DEB("|WR|the block "<<addr<<" had been grabbed from my cache, now try again");
            continue;
          }
          if(plocal_cache->GetBlockStatus(addr) != EXCLUSIVE) {
            /* if it had been set as shared by other node, 
             * write the block to cache still, and than return to the beginning and try again */
            memcpy(ptmp, message_buf, block_size);
            plocal_cache->SetIntegrity(addr);
            cache_mutex.unlock();
            VLASER_DEB("|WR|the block "<<addr<<" had already been shared, now try again");
            continue;
          }
          /* all conditions have been satisfied */
          memcpy(ptmp, message_buf, block_size);
          plocal_cache->SetIntegrity(addr);
          pmes = ptmp + startpoint;
          memcpy(pmes, buf, count);
          plocal_cache->SetBlockStatus(addr, MODIFIED);
          cache_mutex.unlock();
          VLASER_DEB("|WR|write block "<<addr<<" ok");
          return;
        }
      }
      else { /* if the block is already in the cache, but the status is SHARED */
        /* make the block writable */
        tmpid = addr / local_block_num;
        VLASER_DEB("|WR|writing block cache hit but tagged as shared");
        if(tmpid == my_id) {
          cache_mutex.lock();
          ptmp = plocal_cache->AccessBlock(addr, 0);
          if(ptmp == NULL) {
            cache_mutex.unlock();
            VLASER_DEB("|WR|the block "<<addr<<" had been grabbed from my cache, now try again");
            continue;
          }
          plocal_cache->SetBlockStatus(addr, EXCLUSIVE);
          cache_mutex.unlock();

          PackAddr(addr, vsaddr_tag_only_buf); 
          VLASER_DEB("|WR|send self request to tag block "<<addr<<" as exclusive");
          pmessage_passing->ReqSend(my_id, TAG_SELF_REQ_BLOCK_EXCLUSIVE, vsaddr_tag_only_buf, sizeof(vsaddr));
          pmessage_passing->WaitAck(my_id, tag, message_buf, message_buf_size);
          if(tag == TAG_ACK_RETRY) {
            VLASER_DEB("|WR|self request exclusive fail, as TAG_ACK_RETRY got");
            continue;
          }
          VLASER_DEB("|WR|self request ok");
          cache_mutex.lock();
          /* now check if the block is still in cache, and is still exclusive */
          ptmp = plocal_cache->AccessBlock(addr, 1);
          if(ptmp == NULL) {
            /* if not in cache any more, return to the very beginning and try again */
            cache_mutex.unlock();
            VLASER_DEB("|WR|the block "<<addr<<" had been grabbed from my cache, now try again");
            continue;
          }
          if(plocal_cache->GetBlockStatus(addr) != EXCLUSIVE) {
            /* if it had been set as shared by other node, also return to the beginning */
            cache_mutex.unlock();
            VLASER_DEB("|WR|the block "<<addr<<" had already been shared, now try again");
            continue;
          }
          storage_mutex.lock();

          n = addr % local_block_num;
          plocal_storage->RdBlock(n, ptmp);
          storage_mutex.unlock();

          plocal_cache->SetIntegrity(addr);
          pmes = ptmp + startpoint;
          memcpy(pmes, buf, count);
          plocal_cache->SetBlockStatus(addr, MODIFIED);
          cache_mutex.unlock();
          VLASER_DEB("|WR|write block "<<addr<<" from local storage ok");
          return;
        }
        else { /* if the block is a remote block */
          /*
           * set the block as exclusive in advance, to make it be possible for service 
           * thread to answer the SET_SHARED and SET_INVALID request
           *
           */
          cache_mutex.lock();
          ptmp = plocal_cache->AccessBlock(addr, 0);
          if(ptmp == NULL) {
            cache_mutex.unlock();
            VLASER_DEB("|WR|the block "<<addr<<" had been grabbed from my cache, now try again");
            continue;
          }
          plocal_cache->SetBlockStatus(addr, EXCLUSIVE);
          cache_mutex.unlock();

          VLASER_DEB("|WR|request node "<<tmpid<<" to set block "<<addr<<" as exclusive");
          pmessage_passing->ReqSend(tmpid, TAG_REQ_EXCLUSIVE, vsaddr_tag_only_buf, sizeof(vsaddr));
          pmessage_passing->WaitAck(tmpid, tag, message_buf, message_buf_size);
          if(tag != TAG_ACK_CONFIRM) {
            VLASER_DEB("|WR|request block exclusive from node "<<tmpid<<" fail, as tag "<<tag<<" returned");
            continue;
          }
          /*
           * lock the cache to check if the block is been set as invalid
           * or shared. If so, return to the beginning to try again.
           *
           */
          cache_mutex.lock();
          ptmp = plocal_cache->AccessBlock(addr, 1);
          if(ptmp == NULL) {
            cache_mutex.unlock();
            VLASER_DEB("|WR|the block "<<addr<<" had been grabbed from my cache, now try again");
            continue;
          }
          if(plocal_cache->GetBlockStatus(addr) != EXCLUSIVE) {
            cache_mutex.unlock();
            VLASER_DEB("|WR|the block "<<addr<<" had already been shared, now try again");
            continue;
          }
          /* all conditions have been satisfied*/
          plocal_cache->SetBlockStatus(addr, MODIFIED);
          pmes = ptmp + startpoint;
          memcpy(pmes, buf, count);
          cache_mutex.unlock();
          VLASER_DEB("|WR|write block "<<addr<<" ok");
          return;
        }
      }
    }
  }
  
  int
  cpl::Write(globaladdress gd, vsbyte* buf, int count)
  {
    vsaddr endaddr, startaddr;
    vscache::BlockStatus bs;
    vsbyte* ptmp;
    int i;
    
    /* if this node has been told to terminate, do nothing, just return */
    if(finish_signal || !is_message_service_ready)
      return 0;
    try{
      io_busy = 1;
      VLASER_DEB("begin global random write()");

      endaddr = (gd + count - 1) / block_size;
      startaddr = gd / block_size;

      if(((startaddr / local_block_num) > (node_num - 1)) || ((endaddr / local_block_num) > (node_num - 1)))
        throw cpl_runtime_error("global space address overflow: from cpl::read()");
      /* write all the blocks that being covered by the writing range */
      if(startaddr == endaddr)
        WriteWithinBlock(startaddr, gd % block_size, count, buf);
      else {
        i = gd % block_size;
        WriteWithinBlock(startaddr, i, block_size - i, buf);
        buf += (block_size - i);
        ++startaddr;
        while(startaddr != endaddr) {
          WriteWithinBlock(startaddr, 0, block_size, buf);
          ++startaddr;
          buf += block_size;
        }
        WriteWithinBlock(startaddr, 0, (gd + count) % block_size, buf);
      }
      io_busy = 0;
    }
    catch(std::logic_error& except) {
      std::cout<<"|FATAL| get logic error"<<std::endl<<except.what()<<std::endl<<"catched in cpl::Write"
        <<std::endl<<"will not handle it, now rethrow."<<std::endl;
      throw;
    }
    catch(std::runtime_error& except) {
      std::cout<<"|FATAL| get runtime error"<<std::endl<<except.what()<<std::endl<<"catched in cpl::Write"
        <<std::endl<<"will not handle it, now rethrow."<<std::endl;
      throw;
    }
    VLASER_DEB("global random write() ok");
    return 1;
  }

  void*
  cpl::_pthread_routine(void* pclass)
  {
    ((cpl*)pclass)->MessageServiceThread();
    return NULL;
  }

  void
  cpl::Initialize()
  {
    pthread_attr_t service_thread_attr;

    signal(SIGPIPE, SIG_IGN);

    if(pthread_attr_init(&service_thread_attr) != 0)
      throw cpl_runtime_error("can not initialize pthread attribute: from cpl::Initialize()");
    /* create the message service thread */
    if(pthread_create(&service_thread_id, &service_thread_attr, (void* (*)(void*))(&vlaser::cpl::_pthread_routine), this) != 0)
      throw cpl_runtime_error("can not create new thread with pthread_create: from cpl::Initialize()");

    std::cout<<"|STD| VLASER Node "<<my_id<<" :"<<std::endl;
    MakeRandomSeed();
    std::cout<<"|STD| Make random seed ok."<<std::endl;
    std::cout<<"|STD| Initializing coherence protocol service thread and waiting for all vlaser nodes get ready..."<<std::endl;
    /* block the main thread, and wait for the message serivce thread being ready */
    while(!is_message_service_ready)
      usleep(100000);
    std::cout<<"|STD| Coherence protocol service is OK now."<<std::endl;
    return;
  }

  void
  cpl::ShutDown()
  {
    /* send shutdown signal to node 0. Can be called from any node in the cluster */
    if(is_message_service_ready && (!finish_signal)) {
      VLASER_DEB("passing TAG_SHUTDOWN signal from node "<<my_id<<" to node 0");
      pmessage_passing->ReqSend(0, TAG_SHUTDOWN, message_buf, 0);
      VLASER_DEB("waiting service thread exit");
      pthread_join(service_thread_id, NULL);
    }
    return;
  }

  void
  cpl::WaitShutDown()
  {
    if(is_message_service_ready) {
      VLASER_DEB("waiting service thread exit");
      pthread_join(service_thread_id, NULL);
    }
    return;
  }

} //end namespace vlaser
