#include "cpl.h"
#include "lsal.h"
#include "mpal_socket.h"
#include "vstype.h"
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>
#include <signal.h>
#include <iostream>
#include <fstream>

#define CLI_OUT(x) cout<<"|CLIENT| id "<<my_id<<" : "<<x<<endl


int
main()
{
  using namespace std;
  using namespace vlaser;

  mpal* mymp;
  lsal* myls;
  cpl* mycpl;

  int my_id;
  BlockSize my_blocksize = B64K;
  vsaddr my_localsize = 8 * 1024;
  char my_localstorage[] = "/vlaser/local_storage";
  char my_id_file[] = "/vlaser/id_d128";
  char hosts_path[] = "/vlaser/hosts_d128";
  char my_addr_path[] = "/vlaser/my_addr";
  int my_vlaser_node_num = 128;
  int my_vscache_size = 2 * 1024;
  int RANDOM_COUNT = 16 * 1024;
  vsaddr random_v;

  vsbyte* my_buf;

  struct timeval tclo1, tclo2;
  double tclo;

  signal(SIGPIPE, SIG_IGN);

  my_buf = new vsbyte[my_blocksize];

  ifstream myfstream(my_id_file);
  myfstream>>my_id;
  CLI_OUT("got my id: "<<my_id);
  mymp = new mpal_socket(my_id, my_vlaser_node_num, my_addr_path, hosts_path);
  CLI_OUT("mpal_socket constructed");
  myls = new lsal_fileemulate(my_blocksize, my_localsize, my_localstorage);
//  myls = new lsal_air(my_blocksize, my_localsize);
  CLI_OUT("localstorage constructed");
  mycpl = new cpl(my_blocksize, my_vscache_size, my_localsize, my_id, my_vlaser_node_num, mymp, myls);
  CLI_OUT("cpl constructed");

  try {
    mycpl->Initialize();

//    if(my_id % 2 == 1) {
    CLI_OUT("|BEGIN|begin testintg");
    gettimeofday(&tclo1, NULL);
    for(int i = 0; i < RANDOM_COUNT; ++i) {
      CLI_OUT("Access Test "<<i);
      random_v = random() % 2;
      if(random_v) {
        random_v = random() * random() % (my_vlaser_node_num * my_localsize);
        CLI_OUT("global read block "<<random_v);
        mycpl->Read(random_v * my_blocksize, my_buf, my_blocksize);
        CLI_OUT("global read block "<<random_v<<" ok");
      }
      else {
        random_v = random() * random() % (my_vlaser_node_num * my_localsize);
        for(int j = 0; j < my_blocksize; ++j)
          my_buf[j] = 66;
        CLI_OUT("global write block "<<random_v);
        mycpl->Write(random_v * my_blocksize, my_buf, my_blocksize);
        CLI_OUT("global write block "<<random_v<<" ok");
      }
    }
    gettimeofday(&tclo2, NULL);
//}
   CLI_OUT("|OK|testing ok, now shutdown...");

    mymp->Test(1);
    sleep(5);
    if(my_id == 0)
    mycpl->ShutDown();
    else
    mycpl->WaitShutDown();
  }
  catch(logic_error& exc) {
    cout<<"|CLIENT RETHROW| id "<<my_id<<" "<<exc.what()<<endl;cout.flush();
  }
  catch(runtime_error& exc) {
    cout<<"|CLIENT RETHROW| id "<<my_id<<" "<<exc.what()<<endl;cout.flush();
  }

  tclo = tclo2.tv_sec - tclo1.tv_sec + (tclo2.tv_usec - tclo1.tv_usec) / 1000000.0;
  CLI_OUT("|FIN|finish testing, used "<<tclo<<" second wall time for "<<RANDOM_COUNT<<" IOs, so random access time is "
<<tclo * 1000 / RANDOM_COUNT<<" milli second, IOPS is "<<RANDOM_COUNT / tclo);
  delete mycpl;
  CLI_OUT("mycpl free");
  delete mymp;
  CLI_OUT("mymp free");
  delete myls;
  CLI_OUT("myls free");
  delete[] my_buf;
  CLI_OUT("my_buf free");

  return 0;
}
