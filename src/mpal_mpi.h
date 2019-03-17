/* 
 * Virtual Linear Address SERvice
 *
 * Author :Liu Peng-Hong  Institute of Scientific Computing, Nankai Univ.
 *
 * Message Passing Abstract Layer
 * class vlaser::mpal_mpi
 * Header File
 *
 * Feb 14, 2011  Original Design
 *
 */

#ifndef _VLASER_MPAL_MPI_H_
#define _VLASER_MPAL_MPI_H_

#include "vstype.h"
#include "mpal.h"
#include "mpi.h"
#include <stdexcept>

namespace vlaser {

  /*
   * CLASS mpal_mpi
   *
   * Class mpal_mpi is MPI's implementation of
   * abstract bass class mpal.
   * Class mpal requires that all the communication
   * primitives are thread safe, so the MPI
   * must support MPI_THREAD_MULTIPLE level of the
   * MPI thread environment.
   *
   */

  class mpal_mpi : public mpal {
  public:
    mpal_mpi(vsnodeid id, vsnodeid num);
    virtual ~mpal_mpi();

    /* interface derives from mpal */

    int Test(unsigned char flag); 
    int Initialize();
    int Finalize();
    
    int ReqSend(vsnodeid dest, int tag, vsbyte* buf, int count);
    int WaitReq(vsnodeid& source, int& tag, vsbyte* buf, int count); 
    int AckSend(vsnodeid dest, int tag, vsbyte* buf, int count);
    int WaitAck(vsnodeid source, int& tag, vsbyte* buf, int count);
    int SerSend(vsnodeid dest, int tag, vsbyte* buf, int count);
    int WaitSer(vsnodeid source, int& tag, vsbyte* buf, int count);

    /* class interface end */

  private:

    typedef int TypeOfNodeId;
#define TypeOfNodeIdForMpi MPI_UNSIGNED
#define TypeOfDataForMpi MPI_UNSIGNED_CHAR

    TypeOfNodeId mpi_global_rank; /* mpi rank in gourp of MPI_COMM_WORLD */
    TypeOfNodeId* id_map_vlaser_to_mpi; /* translate the VLASER node id to mpi global rank*/
    TypeOfNodeId* id_map_mpi_to_vlaser; /* translate the mpi global rank to VLASER node id*/
    int is_initialized; 

    MPI_Comm req_channel; /* MPI communication context for request channel */
    MPI_Comm ack_channel; /* MPI communication context for acknowledgement channel */
    MPI_Comm ser_channel; /* MPI communication context for service channel */
    MPI_Comm ctl_channel; /* MPI communicatino context for management */

  }; //end class mpal_mpi declaration

} //end namespace vlaser

#endif //#ifdef _VLASER_MPAL_MPI_H
