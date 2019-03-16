/* 
 * Virtual Linear Address SERvice
 *
 * Author :Liu Peng-Hong  Institute of Scientific Computing, Nankai Univ.
 *
 * Virtual Space Cache Component
 * class vlaser::vscache
 * Source File
 *
 * Feb 11, 2011  Original Design
 * May 11, 2011  Add a list to record all invalid
 *               blocks in cache to speed up the
 *               replacement procedure.
 *
 */

#include "vscache.h"
#include <cstring>

namespace vlaser {

  /*
   * Implementation of Class vscache
   */

  vscache::vscache(BlockSize bsize, vsaddr n) :
  cache_block_size(bsize), /* block size */
  cache_size(n) /* number of total blocks */
  {
    cache_list.clear();
    cache_map.clear();
    invalid_blocks.clear();
    modified_blocks.clear();
    
    /* allocate all cache block data space */
    cacheX = (vsbyte*)(new unsigned char[sizeof(vsbyte) * bsize * n]);

    /* allocate all CacheBlock */
    cache_blocks = new CacheBlock[n];

    for(int i = 0; i < n; ++i) {
      (cache_blocks[i]).data = cacheX + i * cache_block_size;
      (cache_blocks[i]).status = INVALID;
      (cache_blocks[i]).pinning_flag = 0;
      (cache_blocks[i]).integrity_flag = 0;

      cache_map.insert(TypeOfCacheMap::value_type(i, &(cache_blocks[i])));
      cache_list.push_front(i);
      invalid_blocks.push_front(i);
      (cache_blocks[i]).list_pos = cache_list.begin();
      (cache_blocks[i]).invalid_pos = invalid_blocks.begin();
    }
    pinning_block_num = 0;
    is_create_modified_list = 0;
  }

  vscache::~vscache()
  {
    delete[] cacheX;
    delete[] cache_blocks;
  }

  inline vscache::CacheBlock*
  vscache::FindBlock(vsaddr addr)
  {
    TypeOfCacheMap::iterator temp;
    if((temp = cache_map.find(addr)) == cache_map.end())
      throw cache_miss("cache miss: from vscache::find()");
    else
      return temp->second;
  }

  vscache::BlockStatus
  vscache::GetBlockStatus(vsaddr addr)
  {
    return FindBlock(addr)->status; 
  }

  void
  vscache::SetBlockStatus(vsaddr addr, BlockStatus status)
  {
    CacheBlock* tmp = FindBlock(addr);
    
    /* trace the change of the invalid blocks' number */
    if(tmp->status == INVALID && status != INVALID)
      invalid_blocks.erase(tmp->invalid_pos);
    else if(tmp->status != INVALID && status == INVALID) {
      invalid_blocks.push_front(addr);
      tmp->invalid_pos = invalid_blocks.begin();
    }
    tmp->status = status;
    return;
  }

  void
  vscache::SetIntegrity(vsaddr addr)
  {
    CacheBlock* tmp = FindBlock(addr);

    tmp->integrity_flag = 1;
    return;
  }
    
  int
  vscache::IsIntegrity(vsaddr addr)
  {
    CacheBlock* tmp = FindBlock(addr);
        
    return tmp->integrity_flag;
  }

  vsbyte*
  vscache::AccessBlock(vsaddr addr, int rec_flag)
  {
    TypeOfCacheMap::iterator map_iter;
    CacheBlock* tmp;

    if((map_iter = cache_map.find(addr)) == cache_map.end())
      return NULL;
    tmp = map_iter->second;
    if(tmp->status == INVALID)
      return NULL;

    if(rec_flag) {
      cache_list.erase(tmp->list_pos);
      cache_list.push_back(addr);
      tmp->list_pos = --cache_list.end();
    }
    return (tmp->data);
  }

  void
  vscache::LockBlock(vsaddr addr)
  {
    CacheBlock* tmp;
    tmp = FindBlock(addr);
    if(tmp->status == INVALID)
      throw cache_miss("Locking a invalid block: from vscache::AccessBlock()"); 
    tmp->pinning_flag = 1;
    ++pinning_block_num;
    return;
  }
  
  void
  vscache::ReleaseBlock(vsaddr addr)
  {
    CacheBlock* tmp;
    tmp = FindBlock(addr);
    if(tmp->pinning_flag == 1) {
      tmp->pinning_flag = 0;
      --pinning_block_num;
      if(pinning_block_num < 0)
        throw cache_logic_error("pinning block counter is broken: from vscache::ReleaseBlock()");
    }
    return;
  }

  inline vsbyte*
  vscache::SwapBlock(vscache::TypeOfCacheMap::iterator oldmap_iter, vsaddr newaddr, vscache::BlockStatus newstatus)
  {
    CacheBlock* tmp;
    tmp = oldmap_iter->second; /* get old block's pointer */
    /* set all new block's properties */
    tmp->pinning_flag = 0;
    tmp->status = newstatus;
    tmp->integrity_flag = 0;
    /* delete the old block from LRU list, hash map, and invalid list */
    cache_list.erase(tmp->list_pos);
    cache_map.erase(oldmap_iter);
    /* insert the new block to hash map */
    cache_map.insert(TypeOfCacheMap::value_type(newaddr, tmp));
    /* insert new block to LRU list's end */
    cache_list.push_back(newaddr);
    tmp->list_pos = --cache_list.end();
    return tmp->data;
  }

  int
  vscache::FindReplacingBlock(vsaddr& addr, int& if_writeback)
  {
    TypeOfCacheMap::iterator map_iter;
    TypeOfLRUList::iterator list_iter;

    if(invalid_blocks.empty()) {
      list_iter = cache_list.begin();
      while(list_iter != cache_list.end()) {
        map_iter = cache_map.find(*list_iter);
        if(map_iter == cache_map.end()) /* if find nothing, LRU list or hash map may be broken, there should be design problems */
          throw cache_logic_error("LRU list discords with cache hash map: from vscache::FindReplacingBlock()");
        if(map_iter->second->pinning_flag == 0) {
          /* if not pinning, use it */
          if(map_iter->second->status == MODIFIED)
            if_writeback = 1;
          else
            if_writeback = 0;
          addr = map_iter->first;
          return 1;
        }
      ++list_iter;
      }
      if(cache_map.size() != cache_size)
        throw cache_logic_error("cache hash map size does not equate to cache blocks set number: from vscache::ReplaceBlock()");
      throw cache_pinning("all cache block pins up: from vscache::ReplaceBlock()"); /* all cache block is pinning */
    }
    else
      return 0;
  }

  vsbyte*
  vscache::PushBlock(vsaddr newaddr, BlockStatus st, int swapblock, vsaddr swappedaddr)
  {
    TypeOfCacheMap::iterator map_iter;
    CacheBlock* ptmp;
    int bingo = 0;

    if(swapblock) {
      /* push new block by replacing the given old block */
      map_iter = cache_map.find(newaddr);
      if(map_iter != cache_map.end())
        throw cache_logic_error("pushing a cached virtual address: from vscache::PushBlock()");
      map_iter = cache_map.find(swappedaddr);
      if(map_iter == cache_map.end())
        throw cache_logic_error("can not find the swapblock address in cache: from vscache::PushBlock()");
      if(map_iter->second->status == INVALID)
        invalid_blocks.erase(map_iter->second->invalid_pos);
      if(st == INVALID) {
        invalid_blocks.push_front(newaddr);
        map_iter->second->invalid_pos = invalid_blocks.begin();
      }
      return SwapBlock(map_iter, newaddr, st);
    }
    else {
      if(invalid_blocks.empty())
        throw cache_logic_error("call PushBlock method using no swap mode, but find there is no invalid block: from vscache::PushBlock()");
      
      map_iter = cache_map.find(newaddr);
      if(map_iter != cache_map.end()) {
        /* if the cached block is INVALID, just replace it */
        if(map_iter->second->status == INVALID) {
          if(st != INVALID)
            invalid_blocks.erase(map_iter->second->invalid_pos);
          return SwapBlock(map_iter, newaddr, st);
        }
        else
          throw cache_logic_error("Pushing a cached virtual address: from vscache::PushBlock()");
      }

      map_iter = cache_map.find(invalid_blocks.front());
      if(map_iter == cache_map.end())
        throw cache_logic_error("can not find a invalid block in cache_map, which the block is from invalid list: from ReplaceBlock()");
      invalid_blocks.pop_front();
      
      if(st == INVALID) {
        invalid_blocks.push_front(newaddr);
        map_iter->second->invalid_pos = invalid_blocks.begin();
      }
      return SwapBlock(map_iter, newaddr, st);
    }
  }

  inline void
  vscache::FindAllModified()
  {
    TypeOfCacheMap::iterator map_iter = cache_map.begin();

    modified_blocks.clear();
    /* record all the blocks that is tagged as MODIFIED */
    while(map_iter != cache_map.end()) {
      if(map_iter->second->status == MODIFIED)
        modified_blocks.push_back(map_iter->first);
      ++map_iter;
    }
    is_create_modified_list = 1;
    return;
  }

  int
  vscache::CleanCache(vsaddr& addr, vsbyte** buf)
  {
    TypeOfCacheMap::iterator map_iter;
    
    if(!is_create_modified_list)
      FindAllModified();
    if(modified_blocks.size() == 0)
      return 0;
    else {
      map_iter = cache_map.find(modified_blocks.back());
      if(map_iter == cache_map.end())
        throw cache_logic_error("modified blocks list is broken: from vscache::CleanCache()");
      /* return its data and global address */
      *buf = map_iter->second->data;
      addr = map_iter->first;
      map_iter->second->status = INVALID;
      modified_blocks.pop_back();
      return 1;
    }
  }

  int
  vscache::IsCached(vsaddr addr, BlockStatus& status)
  {
    TypeOfCacheMap::iterator temp;
    if((temp = cache_map.find(addr)) == cache_map.end())
      return 0;
    else
      if(temp->second->status == INVALID)
        return 0;
      else {
        status = temp->second->status;
        return 1;
      }
  }

  inline vsaddr
  vscache::GetPinningNum()
  {
    return pinning_block_num;
  }

} //end namespace vlaser
