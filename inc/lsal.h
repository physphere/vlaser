/* 
 * Virtual Linear Address SERvice
 *
 * Author :Liu Peng-Hong  Institute of Scientific Computing, Nankai Univ.
 *
 * Local Storage Abstract Layer
 * abstract class vlaser::lsal
 * class vlaser::lasl_fileemulate
 * Header File
 *
 * Feb 16, 2011  Original Design
 * May 15, 2011  Add class lsal_air
 *
 */

#ifndef _VLASER_LSAL_H_
#define _VLASER_LSAL_H_

#include "vstype.h"
#include <stdexcept>

namespace vlaser {

  /*
   * CLASS lsal
   * 
   * Class lsal define the abstract behaviors
   * of local storage of VLASER.
   *
   * 1) local storage is block storage,
   * has only RdBlock() and WrBlock() interface
   * 2) whether is lsal thread safe is undefined,
   * it depends on specific implementation.
   *
   */

  class lsal {
  public:

    class lsal_logic_error : public std::logic_error {
    public:
      lsal_logic_error(const char* msg = "") : logic_error(msg){}
    };
    class lsal_runtime_error : public std::runtime_error {
    public:
      lsal_runtime_error(const char* msg = "") : runtime_error(msg){}
    };


    lsal(BlockSize bs, vsaddr vol) : block_size(bs), block_num(vol) {}

    virtual ~lsal(){}

    /* class interface */

    const BlockSize block_size; /* indicating the block size */
    const vsaddr block_num; /* how many blocks the local storage has, set by user */

    virtual int Initialize() = 0;

    virtual int Finalize() = 0;

    /*
     * readings and writings are in unit of block.
     * these methods are supposed to be never fail,
     */
    virtual void RdBlock(vsaddr blockno, vsbyte* buf) = 0;

    virtual void WrBlock(vsaddr blockno, vsbyte* buf) = 0;

    /* random access version of read and write.
     * reserved for functional integrity.
     */
    virtual int Rd(vsaddr blockno, blockoffset pos, vsbyte* buf, blockoffset size) {
      throw lsal_logic_error("lsal::Rd() method has been called: from lsal::Rd()");
      return 0;
    }
    virtual int Wr(vsaddr blockno, blockoffset pos, vsbyte* buf, blockoffset size) {
      throw lsal_logic_error("lsal::Wr() method has been called: from lsal::Wr()");
      return 0;
    }
    /* class interface end */
  };

  /* linux file emulating version of lsal.
   * not thread safe, use mutex outside
   * the class for synchronization.
   */
  class lsal_fileemulate : public lsal {
  public:
    lsal_fileemulate(BlockSize bs, vsaddr vol, const char* fp);
    ~lsal_fileemulate();
    int Initialize();
    int Finalize();
    void RdBlock(vsaddr blockno, vsbyte* buf); 
    void WrBlock(vsaddr blockno, vsbyte* buf);
  private:
    int fd; /* file descriptor */
  };

  class lsal_air: public lsal {
  public:
    lsal_air(BlockSize bs, vsaddr vol) : lsal(bs, vol) {}
    ~lsal_air() {}
    int Initialize() {}
    int Finalize() {}
    void RdBlock(vsaddr blockno, vsbyte* buf) {}
    void WrBlock(vsaddr blockno, vsbyte* buf) {}
  };
} // end namespace vlaser


#endif //#ifndef _VLASER_LSAL_H_
