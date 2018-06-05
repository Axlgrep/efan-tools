
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <iostream>

#include <string>
#include <vector>

#include "hiredis-vip/hiredis.h"
#include "hiredis-vip/hircluster.h"

#define log(M, ...) fprintf(stderr, M, ##__VA_ARGS__)
#define log_info(M, ...) fprintf(stderr, "[INFO] " M "\n", ##__VA_ARGS__)
#define log_error(M, ...) fprintf(stderr, "[ERROR] " M "\n", ##__VA_ARGS__)

#define ID_TIME_MASK 0x000FFFFFFFFFFFFFull  /* the unit is microsecond */

static pthread_mutex_t lock;

static void Usage() {
  log("Usage:\n\t./clean_offline_msg keep_days ip:port ip:port ...\n");
}

static uint64_t cur_ts = time(NULL)*1000000ull;                           /* unit is microsecond */
static uint64_t keep_time = 24ull*3600ull*1000000ull;                     /* unit is microsecond */

static int32_t ParseIpPort(const std::string ipport,
                           std::string* ip,
                           int32_t* port) {
  size_t pos = ipport.find(":");
  if (pos == std::string::npos
      || !pos
      || pos == ipport.size() - 1 ) {
    return -1;
  }
  *ip = ipport.substr(0, pos);
  *port = atoi(ipport.substr(pos+1).data());
  return 0;
}

static uint64_t GetMsgid(const std::string& key) {
  size_t sz = key.size();
  if (key.substr(0, 2) != "m:"
      || sz <= 5
      || key.substr(sz-3) != ":$k") {
    return -1;
  }
  return static_cast<uint64_t>(atoll(key.substr(2, sz - 5).data()));
}

static int32_t ShowTime(const uint64_t ts) {
  //GMT to CST
  time_t t = ts / 1000000 + 8 * 3600;
  struct tm *p;
  p = gmtime(&t);
  char s[80];
  strftime(s, 80, "%Y-%m-%d %H:%M:%S", p);
  printf("%lld : %s\n", ts, s);
}

int32_t CleanOfflineMsg(const std::string& redis_addr) {
  static const timeval tv = {4, 0};

  std::string host;
  int32_t port;
  if (ParseIpPort(redis_addr, &host, &port) == -1) {
    log_info("%s: redis address error\n", redis_addr.data());
    return -1;
  }
  log_info("%s: clean offline msg starting\n", redis_addr.data());

  redisContext* rc = redisConnectWithTimeout(host.data(), port, tv);
  if (!rc
      || rc->err) {
    redisFree(rc);
    log_error("%s: connect error, error msg: %s\n", redis_addr.data(), rc ? rc->errstr : "");
    return -1;
  }
  redisClusterContext* rcc = redisClusterConnectWithTimeout(redis_addr.data(), tv, HIRCLUSTER_FLAG_ROUTE_USE_SLOTS);
  if (!rcc
      || rcc->err) {
    redisClusterFree(rcc);
    log_error("%s: cluster connect error, error msg: %s\n", redis_addr.data(), rcc ? rcc->errstr : "");
    return -1;
  }


  const char *argv[6] = {"scan", "0", "match", "m:*:$k", "count", "100"};
  size_t argvlen[6] = {4, 1, 5, 6, 5, 3};

  redisReply* res = NULL, *res_a = NULL;
  std::string cursor, key;
  uint64_t msgid, ts;
  while (cursor != "0") {
    res = reinterpret_cast<redisReply*>(redisCommandArgv(rc,
                                                         6,
                                                         reinterpret_cast<const char**>(argv),
                                                         reinterpret_cast<const size_t*>(argvlen)));
    if (!res
        || res->type != REDIS_REPLY_ARRAY
        || res->elements != 2) {
       log_error("%s: scan error, error msg: %s\n", redis_addr.data(), res ? res->str : "");

       freeReplyObject(res);
       redisFree(rc);
       redisClusterFree(rcc);
       return -1;
    }
    if (res->element[0]->type != REDIS_REPLY_STRING) {
       log_error("%s: scan error, first reply element for cursor type error\n", redis_addr.data());
       freeReplyObject(res);
       redisFree(rc);
       redisClusterFree(rcc);
       return -1;
    }
    cursor = res->element[0]->str;
    argv[1] = cursor.data();
    argvlen[1] = cursor.size();

    res_a = res->element[1];
    if (res_a->type == REDIS_REPLY_NIL) {
      freeReplyObject(res);
      res = NULL;
      continue;
    }
    if (res_a->type != REDIS_REPLY_ARRAY) {
      log_error("%s: scan error, second element for keys is not nil or array\n", redis_addr.data());
      freeReplyObject(res);
      redisFree(rc);
      redisClusterFree(rcc);
      return -1;
    }
    std::vector<std::string> del_keys;
    for (size_t idx = 0; idx != res_a->elements; ++idx) {
      key = res_a->element[idx]->str;
      if ((msgid = GetMsgid(key)) == -1) {
        log_info("%s: get msg id failed\n", redis_addr.data());
        continue;
      }
      ts = msgid & ID_TIME_MASK;
      if (ts <= cur_ts - keep_time) {
        del_keys.push_back(key);
      }
    }
    freeReplyObject(res);
    res = NULL;

    if (del_keys.empty()) {
      continue;
    }

    const char* argv1[101] = {"del"};
    size_t argvlen1[101] = {3};
    for (size_t idx = 0; idx != del_keys.size(); ++idx) {
      argv1[idx + 1] = del_keys[idx].data();
      argvlen1[idx + 1] = del_keys[idx].size();
    }

    res = reinterpret_cast<redisReply*>(redisClusterCommandArgv(rcc,
                                                                del_keys.size() + 1,
                                                                reinterpret_cast<const char**>(argv1),
                                                                reinterpret_cast<size_t*>(argvlen1)));
    if (!res || res->type != REDIS_REPLY_INTEGER) {
      log_info("%s, redis del failed, error msg : %s", redis_addr.data(), !res ? res->str : "");
    } else {
      pthread_mutex_lock(&lock);
      for (size_t idx = 0; idx != del_keys.size(); ++idx) {
        msgid = GetMsgid(del_keys[idx]);
        ts = msgid & ID_TIME_MASK;
        std::cout << del_keys[idx] << " : ";
        ShowTime(ts);
      }
      pthread_mutex_unlock(&lock);
    }
    freeReplyObject(res);
    res = NULL;
  }

  redisFree(rc);
  redisClusterFree(rcc);
  log_info("clean clean offline msg for %s finished\n", redis_addr.data());
  return 0;
}

void* thm(void* arg) {
  std::string ipport = reinterpret_cast<char*>(arg);
  CleanOfflineMsg(ipport);
}

int32_t main(int32_t argc, char* argv[]) {
  if (argc < 3) {
    Usage();
    return -1;
  }

  uint32_t live_days = atoi(argv[1]);
  if (live_days <= 0 || live_days > 7) {
    log_error("Support survival time 1 ~ 7");
    return -1;
  } else {
    keep_time *= live_days;
    log_info("keep time : %lld\n", keep_time);
  }

  std::vector<pthread_t> ths;
  pthread_t tid;
  pthread_mutex_init(&lock, NULL);
  for (uint32_t idx = 2; idx != argc; ++idx) {
    if (pthread_create(&tid, NULL, thm, argv[idx])) {
      log_error("%s: create thread failed, error msg: %s", argv[idx], strerror(errno));
      continue;
    }
    ths.push_back(tid);
  }

  for (size_t idx = 0; idx != ths.size(); ++idx) {
    pthread_join(ths[idx], NULL);
  }
  pthread_mutex_destroy(&lock);
  return 0;
}
