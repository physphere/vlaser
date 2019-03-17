/* 
 * Virtual Linear Address SERvice
 *
 * Author :Liu Peng-Hong  Institute of Scientific Computing, Nankai Univ.
 *
 * Global Type Header File
 *
 * Feb 11, 2011  Original Design
 *
 */


#ifndef _VLASER_VSTYPE_H_
#define _VLASER_VSTYPE_H_

#ifdef VLASER_DEBUG_OUT
#include <iostream>
#define VLASER_DEB(x) std::cout<<"|DEBUG| id "<<my_id<<" : "<<x<<std::endl
#else
#define VLASER_DEB(ignore) ((void) 0)
#endif

namespace vlaser {
  enum BlockSize {
    B4K = 4 * 1024,
    B8K = 8 * 1024,
    B16K = 16 * 1024,
    B32K = 32 * 1024,
    B64K = 64 * 1024,
    B128K = 128 * 1024,
    B256K = 256 * 1024,
    B512K = 512 * 1024,
    B1M = 1024 * 1024,
    B2M = 2 * 1024 * 1024,
    B4M = 4 * 1024 * 1024,
    B8M = 8 * 1024 * 1024
  };

  typedef unsigned int vsaddr;
  typedef unsigned int blockoffset;
  typedef unsigned char vsbyte;
  typedef unsigned long long globaladdress;

  typedef vsaddr vsnodeid;

  enum MESICoherence {
    MODIFIED, EXCLUSIVE, SHARED, INVALID
  };//MESI coherency Protocol
}

#endif //#ifndef _VSTYPE_H_
