/* 
 * Virtual Linear Address SERvice
 *
 * Author :Liu Peng-Hong  Institute of Scientific Computing, Nankai Univ.
 *
 * Local Storage Abstract Layer
 * abstract class vlaser::lsal
 * class vlaser::lasl_fileemulate
 * Source File
 *
 * Feb 16, 2011  Original Design
 * May 15, 2011  Add class lsal_air
 *
 */

#define _LARGEFILE64_SOURCE

#include "lsal.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace vlaser {

  /*
   * Implementation for class lsal_fileemulate
   */

  lsal_fileemulate::lsal_fileemulate(BlockSize bs, vsaddr vol, const char* fp) :
  lsal(bs, vol)
  {
    if((fd = open(fp, O_CREAT|O_RDWR|O_SYNC|O_LARGEFILE, S_IRUSR|S_IWUSR)) == -1)
      throw lsal_runtime_error("encounter failure during opening local storage emulation file: from lsal_fileemulate::lsal_fileemulate()");
  }

  lsal_fileemulate::~lsal_fileemulate()
  {
    close(fd);
  }

  int
  lsal_fileemulate::Initialize()
  {
    return 0;
  }

  int
  lsal_fileemulate::Finalize()
  {

    return 0;
  }

  void
  lsal_fileemulate::RdBlock(vsaddr blockno, vsbyte* buf)
  {
    if(blockno >= block_num)
      throw lsal_runtime_error("RdBlock() method address overflow: from lsal_fileemulate::RdBlock()");
    lseek64(fd, blockno * block_size, SEEK_SET);
    if(read(fd, buf, block_size) != block_size)
      throw lsal_runtime_error("reading block fail: from lsal_fileemulate::RdBlock()");
    return;
  }

  void
  lsal_fileemulate::WrBlock(vsaddr blockno, vsbyte* buf)
  {
    if(blockno >= block_num)
      throw lsal_runtime_error("WrBlock() method address overflow: from lsal_fileemulate::WrBlock()");
    lseek64(fd, blockno * block_size, SEEK_SET);
    if(write(fd, buf, block_size) != block_size)
      throw lsal_runtime_error("writing block fail: from lsal_fileemulate::WrBlock()");
    return;
  }
  
} //end namesapce vlaser
