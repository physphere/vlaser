/* 
 * Virtual Linear Address SERvice
 *
 * Author :Liu Peng-Hong  Institute of Scientific Computing, Nankai Univ.
 *
 * Virtual Space Cache Component
 * class vlaser::vscache
 * Header File
 *
 * Feb 11, 2011  Original Design
 * May 11, 2011  Add a list to record all invalid
 *               blocks in cache to speed up the
 *               replacement procedure.
 *
 */

#ifndef _VLASER_VSCACHE_H_
#define _VLASER_VSCACHE_H_

#define VLASER_VSCACHE_HASH_MAP_NAMESPACE __gnu_cxx

#include "vstype.h"
#include <hash_map>
#include <list>
#include <vector>
#include <stdexcept>

namespace vlaser {

  /* CLASS vscache
   *
   * LRU algorithm for cache block replacement.
   * The hash map is used for address mapping.
   * And the link list is for implementing the 
   * acurrate LRU.
   *
   */
   
  class vscache {
  public:
    /* MESI for the cache block status */
    typedef MESICoherence BlockStatus;
    
    /* runtime exceptions, could recover in runtime */
    class cache_miss : public std::runtime_error {
    public:
      cache_miss(const char* msg = "") : runtime_error(msg) {}
    };
    class cache_pinning :public std::runtime_error {
    public:
      cache_pinning(const char* msg = "") : runtime_error(msg) {}
    };

    /* non-recoverable runtime error or design flaws */
    class cache_runtime_error : public std::runtime_error {
    public:
      cache_runtime_error(const char* msg = "") : runtime_error(msg) {}
    };
    class cache_logic_error : public std::logic_error {
    public:
      cache_logic_error(const char* msg = "") : logic_error(msg) {}
    };

    vscache(BlockSize bsize, vsaddr n); /* indicate the block size and the number of total blocks */
    
    virtual ~vscache();

    /* class interface */

    const vsaddr cache_size; /* number of blocks */
    const BlockSize cache_block_size;
  
    BlockStatus GetBlockStatus(vsaddr addr);

    void SetBlockStatus(vsaddr addr, BlockStatus status); 
    
    vsbyte* AccessBlock(vsaddr addr, int rec_flag); 

    void LockBlock(vsaddr addr);

    void ReleaseBlock(vsaddr addr); 
    
    /* tells which block will be replaced according to LRU
     * algorithms and return 1, if there is invalid block
     * ready for replacement, the method will return 0.
     */
    int FindReplacingBlock(vsaddr& addr, int& if_writeback);

    /* if swapblock is set to 1, the method will
     * occupy a new block in the cache by kicking out the block
     * which is indicated by swappedaddr
     * if swapblock is set to 0, it says that there have
     * invalid blocks in cache, new block will use that space
     */
    vsbyte* PushBlock(vsaddr newaddr, BlockStatus st, int swapblock, vsaddr swappedaddr); 

    void SetIntegrity(vsaddr addr);
    
    int IsIntegrity(vsaddr addr);

    int IsCached(vsaddr addr, BlockStatus& status); /* a fast and non-exception version of finding block */
    
    vsaddr GetPinningNum();

    int CleanCache(vsaddr& addr, vsbyte** buf);

    /* class interface end */

  protected:

    /* type of LRU list */
    typedef std::list<vsaddr> TypeOfLRUList;
    typedef std::list<vsaddr> TypeOfInvalidList;
    
    /* cache block's structure */
    typedef struct {
      BlockStatus status; /* MESI */
      vsbyte* data; /* pointer for cache block data */
      unsigned int pinning_flag;
      TypeOfLRUList::iterator list_pos; /* indicating the block's position in LRU list */
      unsigned int integrity_flag;
      TypeOfInvalidList::iterator invalid_pos; //indicating the block's position in invalid list
    } CacheBlock;
    
    /* address mapping hash */
    typedef VLASER_VSCACHE_HASH_MAP_NAMESPACE::hash_map<vsaddr, CacheBlock*, VLASER_VSCACHE_HASH_MAP_NAMESPACE::hash<vsaddr> > TypeOfCacheMap;


    TypeOfLRUList cache_list; /* the cache blocks' LRU list */
    TypeOfCacheMap cache_map; /* the address hash map contains each pointer of block */
    vsbyte* cacheX; /* holding all cache block data space */
    CacheBlock* cache_blocks;
    vsaddr pinning_block_num;
    std::vector<vsaddr> modified_blocks; /* record the blocks which are tagged as MODIFIED */
    TypeOfInvalidList invalid_blocks; // record all the invalid blocks;
    int is_create_modified_list;

    CacheBlock* FindBlock(vsaddr addr); /* find a block with address addr, throw exception when cache miss */

    void FindAllModified(); /* find all MODIFIED blocks, and write them to ModifiedBlocks list */

    /* use the new block replace the old block
     * and return the new block's data pointer
     */
    vsbyte* SwapBlock(TypeOfCacheMap::iterator oldmap_iter, vsaddr newaddr, BlockStatus newstatus);
    
	};//end class vscache declaration
}//end namespace vlaser

#endif //#ifndef _VSCACHE_H_

