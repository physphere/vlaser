/* 
 * Virtual Linear Address SERvice
 *
 * Author :Liu Peng-Hong  Institute of Scientific Computing, Nankai Univ.
 *
 * Message Passing Abstract Layer
 * class vlaser::mpal_mpi
 * Source File
 *
 * Feb 14, 2011  Original Design
 *
 */

#include "mpal_mpi.h"

namespace vlaser {

  /*
   * Implementation of class mpal_mpi
   */

  mpal_mpi::mpal_mpi(vsnodeid id, vsnodeid num) :
  mpal(id, num)
  {
    id_map_vlaser_to_mpi = new TypeOfNodeId[node_num];
    id_map_mpi_to_vlaser = new TypeOfNodeId[node_num];
    is_initialized = 0;
  }

  mpal_mpi::~mpal_mpi()
  {
    delete[] id_map_vlaser_to_mpi;
    delete[] id_map_mpi_to_vlaser;
  }

  int
  mpal_mpi::Initialize()
  {
    unsigned char* pcounter = NULL;
    int j;
    int host_fail_flag = 0;

    if(is_initialized)
      throw mpal_runtime_error("already initialized: from mpal_mpi::Initialize()");
    MPI_Init(NULL, NULL);
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Comm_size(MPI_COMM_WORLD, &j);
    if(j != node_num) {
      MPI_Finalize();
      throw mpal_runtime_error("mpi process number is not equal to VLASER node number, some node is unreachable: from mpal_mpi::Initialize()");
    }
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_global_rank);

    MPI_Comm_dup(MPI_COMM_WORLD, &ctl_channel);
    MPI_Barrier(ctl_channel);
    MPI_Comm_dup(MPI_COMM_WORLD, &req_channel);
    MPI_Barrier(ctl_channel);
    MPI_Comm_dup(MPI_COMM_WORLD, &ack_channel);
    MPI_Barrier(ctl_channel);
    MPI_Comm_dup(MPI_COMM_WORLD, &ser_channel);


    /* gather all MPI process' VLASER node id to the MPI node 0 */
    MPI_Gather(&my_id, 1, TypeOfNodeIdForMpi, id_map_mpi_to_vlaser, 1, TypeOfNodeIdForMpi, 0, ctl_channel);
    
    if(mpi_global_rank == 0) { /* MPI mode 0 is used as master */
      pcounter = new unsigned char[node_num];
      for(j = 0; j < node_num; ++j)
        pcounter[j] = 0;

      for(j = 0; j < node_num; ++j) 

        if(id_map_mpi_to_vlaser[j] < node_num && pcounter[id_map_mpi_to_vlaser[j]] == 0) {
          id_map_vlaser_to_mpi[id_map_mpi_to_vlaser[j]] = j;
          ++pcounter[id_map_mpi_to_vlaser[j]]; /* tag this VLASER id as used */
        }
        else
          host_fail_flag = 1;

      delete[] pcounter;
    }
    MPI_Bcast(&host_fail_flag, 1, MPI_INT, 0, ctl_channel);
    if(host_fail_flag)
      throw mpal_runtime_error("VLASER node id colliding or invalid id found: from mpal_mpi::Initialize()");

    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Bcast(id_map_mpi_to_vlaser, node_num, TypeOfNodeIdForMpi, 0, ctl_channel);
    MPI_Bcast(id_map_vlaser_to_mpi, node_num, TypeOfNodeIdForMpi, 0, ctl_channel);

    is_initialized = 1;
    return 0;
  }

  int
  mpal_mpi::Finalize()
  {
    if(is_initialized) {
      MPI_Finalize();
      is_initialized = 0;
    }
    return 0;
  }

  /* communications in req_channel MPI communicator */
  int
  mpal_mpi::ReqSend(vsnodeid dest, int tag, vsbyte* buf, int count)
  {
    if(is_initialized)
      MPI_Send(buf, count, TypeOfDataForMpi, id_map_vlaser_to_mpi[dest], tag, req_channel);
    else
      throw mpal_logic_error("communication environment not initialized: mpal_mpi::ReqSend()");
    return 0;
  }
  
  int
  mpal_mpi::WaitReq(vsnodeid& source, int& tag, vsbyte* buf, int count)
  {
    MPI_Status st;
    if(is_initialized) {
      MPI_Recv(buf, count,  TypeOfDataForMpi, MPI_ANY_SOURCE, MPI_ANY_TAG, req_channel, &st);
      source = id_map_mpi_to_vlaser[st.MPI_SOURCE];
      tag = st.MPI_TAG;
    }
    else
      throw mpal_logic_error("communication environment not initialized: mpal_mpi::WaitReq()");
    return 0;
  }

  /* communications in ask_channel MPI communicator */
  int
  mpal_mpi::AckSend(vsnodeid dest, int tag, vsbyte* buf, int count)
  {
    if(is_initialized)
      MPI_Send(buf, count, TypeOfDataForMpi, id_map_vlaser_to_mpi[dest], tag, ack_channel);
    else
      throw mpal_logic_error("communication environment not initialized: mpal_mpi::AckSend()");
    return 0;
  }

  int
  mpal_mpi::WaitAck(vsnodeid source, int& tag, vsbyte* buf, int count)
  {
    MPI_Status st;
    if(is_initialized) {
      MPI_Recv(buf, count, TypeOfDataForMpi, id_map_vlaser_to_mpi[source], MPI_ANY_TAG, ack_channel, &st);
      tag = st.MPI_TAG;
    }
    else
      throw mpal_logic_error("communication environment not initialized: mpal_mpi::WaitAck()");
    return 0;
  }

  int
  mpal_mpi::SerSend(vsnodeid dest, int tag, vsbyte* buf, int count)
  {
    if(is_initialized)
      MPI_Send(buf, count, TypeOfDataForMpi, id_map_vlaser_to_mpi[dest], tag, ser_channel);
    else
      throw mpal_logic_error("communication environment not initialized: mpal_mpi::SerSend()");
    return 0;   
  }

  int
  mpal_mpi::WaitSer(vsnodeid source, int& tag, vsbyte* buf, int count)
  {
    MPI_Status st;
    if(is_initialized) {
      MPI_Recv(buf, count, TypeOfDataForMpi, id_map_vlaser_to_mpi[source], MPI_ANY_TAG, ser_channel, &st);
      tag = st.MPI_TAG;
    }
    else
      throw mpal_logic_error("communication environment not initialized: mpal_mpi::WaitAck()");
    return 0;
  }


  int
  mpal_mpi::Test(unsigned char flag)
  {
    unsigned char* ptmp = NULL;
    unsigned char global_flag = 1;
    
    if(is_initialized) {
      if(mpi_global_rank == 0)
        ptmp = new unsigned char[node_num];

      /* Gather flags from every mpi process to the master process */
      MPI_Gather(&flag, 1, MPI_UNSIGNED_CHAR, ptmp, 1, MPI_UNSIGNED_CHAR, 0, ctl_channel);
      /* if I am master process, check all flags */
      if(mpi_global_rank == 0)
        for(int i = 0; i < node_num; ++i)
          global_flag &= ptmp[i];
      /* broadcast the result to all processes */
      MPI_Bcast(&global_flag, 1, MPI_UNSIGNED_CHAR, 0, ctl_channel);
      /* if master process, clean the resource */
      if(mpi_global_rank == 0)
        delete[] ptmp;

      if(global_flag == 1)
        return 1;
    }
    else
      throw mpal_logic_error("communication environment not initialized: mpal_mpi::Test()");

    return 0;
  }

} //end namespace vlaser
