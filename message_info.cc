#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>

#include <string>
#include <vector>

#define log(M, ...) fprintf(stderr, M, ##__VA_ARGS__)
#define log_info(M, ...) fprintf(stderr, "[INFO] " M "\n", ##__VA_ARGS__)
#define log_error(M, ...) fprintf(stderr, "[ERROR] " M "\n", ##__VA_ARGS__)

#define ID_UNICAST_MASK                         0x1000000000000000ull  /* if the bit is set, then is not unicast */
#define ID_DURABLE_MASK                         0x2000000000000000ull  /* if the bit is set, then it is a durable message */
#define ID_VERSION_MASK                         0x4000000000000000ull  /* if the bit is set, then this message is for miop5 protocal */
#define ID_TIME_MASK                            0x000FFFFFFFFFFFFFull  /* the unit is microsecond */

static void Usage() {
  log("Usage:\n\t./message_info efan_message_id\n");
}

static int32_t ShowTime(const uint64_t ts) {
  //GMT to CST
  time_t t = ts / 1000000 + 8 * 3600;
  struct tm *p;
  p = gmtime(&t);
  char s[80];
  strftime(s, 80, "%Y-%m-%d %H:%M:%S", p);
  printf("Send time : %s\n", s);
}

int32_t main(int32_t argc, char* argv[]) {

  if (argc < 2) {
    Usage();
    return -1;
  }

  std::string msgid_str = argv[1];
  uint64_t msgid_num = static_cast<uint64_t>(atoll(msgid_str.data()));
  uint64_t ts = msgid_num & 0x000FFFFFFFFFFFFFull;
  printf("Message id : %s\n", msgid_str.c_str());
  ShowTime(ts);
  bool is_v5 = msgid_num & ID_VERSION_MASK;
  bool is_unicast = msgid_num & ID_UNICAST_MASK;
  bool is_durable = msgid_num & ID_DURABLE_MASK;
  printf("Is v5 protocol : %d\n", is_v5);
  printf("Is unicast : %d\n", is_unicast);
  printf("Is durable : %d\n", is_durable);
  return 0;
}
