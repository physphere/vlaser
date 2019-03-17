/* 
 * Virtual Linear Address SERvice
 *
 * Author :Liu Peng-Hong  Institute of Scientific Computing, Nankai Univ.
 *
 * Global mutex
 *
 * Feb 11, 2011  Original Design
 *
 */


#ifndef _VSMUTEX_H_
#define _VSMUTEX_H_

#include <pthread.h>
#include <stdexcept>

namespace vlaser {

  class vlamutex {
  public:

    /* non-recoverable error */
    class mutex_runtime_error : public std::runtime_error {
    public:
      mutex_runtime_error(const char* msg = "") : runtime_error(msg) {}
    };

    vlamutex() {
      /* initialise the pthread mutex */
      if(pthread_mutex_init(&mylock, NULL) != 0)
        throw mutex_runtime_error("mutex error: from class vlamutex");
    }
    ~vlamutex() {
      /* destroy the semaphore */
      pthread_mutex_destroy(&mylock) != 0;
    }

    void lock() {
      /* get the mutex  */
      if(pthread_mutex_lock(&mylock) != 0)
        throw mutex_runtime_error("mutex error: from class vlamutex");
    }
    /* release */
    void unlock() {
      if(pthread_mutex_unlock(&mylock) != 0)
        throw mutex_runtime_error("mutex error: from class vlamutex");
    }
  private:
    pthread_mutex_t mylock;
  };
} //end namespace vlaser

#endif //#ifndef _VSMUTEX_H_
