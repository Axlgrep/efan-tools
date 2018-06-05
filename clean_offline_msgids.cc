
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

#define DAY_MICRO_SEC 86400000000ull

static pthread_mutex_t lock;

static void Usage() {
  log("Usage:\n\t./clean_offline_msgids keep_days ip:port ip:port ...\n");
}

static uint64_t cur_ts = time(NULL) * 1000000ull;                           /* unit is microsecond */
static uint64_t min_score = 0;
static uint64_t max_score = 0;
static std::string min_score_str;
static std::string max_score_str;

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

static std::string ShowTime(const uint64_t ts) {
  //GMT to CST
  time_t t = ts / 1000000 + 8 * 3600;
  struct tm *p;
  p = gmtime(&t);
  char s[80];
  strftime(s, 80, "%Y-%m-%d %H:%M:%S", p);
  return std::string(s);
}

static void showOfflineMessageIds(const std::vector<std::string>& offline_keys, redisContext* rc) {

  uint64_t ts = 0;
  redisReply* res = NULL;
  const char* argv[5] = {"zrange", NULL, "0", "-1", "withscores"};
  size_t argvlen[5] = {6, 0, 1, 2, 10};
  for (uint32_t idx = 0; idx != offline_keys.size(); ++idx) {
    argv[1] = offline_keys[idx].data();
    argvlen[1] = offline_keys[idx].size();
    if (redisAppendCommandArgv(rc,
                               5,
                               reinterpret_cast<const char**>(argv),
                               reinterpret_cast<const size_t*>(argvlen)) != REDIS_OK) {
      log_error("show offline message ids redisAppendCommandArgv error\n", rc ? rc->errstr : "");
      return;
    }
  }

  std::cout << std::endl;
  for (uint32_t idx = 0; idx != offline_keys.size(); ++idx) {
    if (redisGetReply(rc,reinterpret_cast<void**>(&res)) != REDIS_OK
      || !res
      || res->type != REDIS_REPLY_ARRAY
      || res->integer & 0x1ul) {
      log_error("show offline message ids redisGetReply error\n", rc ? rc->errstr : "");
      freeReplyObject(res);
      res = NULL;
      return;
    } else {
      std::cout << "***************SHOW OFFLINE MESSAGE IDS***************" << std::endl;
      std::cout << "offline key : " << offline_keys[idx] << " remain cnt : " << (res->elements >> 1) << std::endl;
      for (uint32_t sidx = 0; sidx != res->elements; sidx += 2) {
        ts = atoll(res->element[sidx + 1]->str);
        std::cout << res->element[sidx]->str << " : " << res->element[sidx + 1]->str << " : " << ShowTime(ts) << std::endl;
      }
      std::cout << "************************END***************************" << std::endl << std::endl;
      freeReplyObject(res);
      res = NULL;
    }
  }
}

int32_t CleanOfflineMsgIds(const std::string& redis_addr) {
  int32_t port;
  std::string host;
  static const timeval tv = {4, 0};

  if (ParseIpPort(redis_addr, &host, &port) == -1) {
    log_info("%s: redis address error\n", redis_addr.data());
    return -1;
  }
  log_info("%s: clean offline msg ids starting\n", redis_addr.data());

  redisContext* rc = redisConnectWithTimeout(host.data(), port, tv);
  if (!rc || rc->err) {
    redisFree(rc);
    log_error("%s: connect error, error msg: %s\n", redis_addr.data(), rc ? rc->errstr : "");
    return -1;
  }

  const char *argv[6] = {"scan", "0", "match", "om:*:$z", "count", "100"};
  size_t argvlen[6] = {4, 1, 5, 7, 5, 3};

  redisReply* res = NULL, *res_a = NULL;
  std::string cursor, key;
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
       return -1;
    }
    if (res->element[0]->type != REDIS_REPLY_STRING) {
      log_error("%s: scan error, first reply element for cursor type error\n", redis_addr.data());

      freeReplyObject(res);
      redisFree(rc);
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
      return -1;
    }
    std::vector<std::string> offline_keys;
    for (size_t idx = 0; idx != res_a->elements; ++idx) {
      key.assign(res_a->element[idx]->str, res_a->element[idx]->len);
      offline_keys.push_back(key);
    }
    freeReplyObject(res);
    res = NULL;

    if (offline_keys.empty()) {
      continue;
    }

    const char* argv1[4] = {"zremrangebyscore", NULL, min_score_str.data(), max_score_str.data()};
    size_t argvlen1[4] = {16, 0, min_score_str.size(), max_score_str.size()};

    for (uint32_t idx = 0; idx != offline_keys.size(); ++idx) {
      argv1[1] = offline_keys[idx].data();
      argvlen1[1] = offline_keys[idx].size();
      if (redisAppendCommandArgv(rc,
                                 4,
                                 reinterpret_cast<const char**>(argv1),
                                 reinterpret_cast<const size_t*>(argvlen1)) != REDIS_OK) {
        log_error("%s: %s redisAppendCommandArgv error", redis_addr.data(), offline_keys[idx].data());
        return -1;
      }
    }

    pthread_mutex_lock(&lock);
    for (uint32_t idx = 0; idx != offline_keys.size(); ++idx) {
      if (redisGetReply(rc, reinterpret_cast<void**>(&res)) != REDIS_OK
        || !res
        || res->type != REDIS_REPLY_INTEGER) {
        log_error("%s: %s redisGetReply error" , redis_addr.data(), offline_keys[idx].data());
        return -1;
      }
      std::cout << offline_keys[idx] << " delete " << res->integer << " expired message ids" << std::endl;
      freeReplyObject(res);
      res = NULL;
    }
    //Display the user offline messages ids after processed
    showOfflineMessageIds(offline_keys, rc);
    pthread_mutex_unlock(&lock);
  }
  redisFree(rc);
  log_info("clean offline msg ids for %s finished\n", redis_addr.data());
  return 0;
}


void* thm(void* arg) {
  std::string ipport = reinterpret_cast<char*>(arg);
  CleanOfflineMsgIds(ipport);
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
    max_score = cur_ts - live_days * DAY_MICRO_SEC;
  }

  min_score_str = std::to_string(min_score);
  max_score_str = std::to_string(max_score);

  std::string end_time = ShowTime(max_score);
  log_info("clean offline msg ids range in %s ~ %s (0 ~ %s)\n", min_score_str.data(), max_score_str.data(), end_time.data());

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

  std::string current_time = ShowTime(time(NULL) * 1000000ull);
  log_info("clean offline msg ids finish %s\n", current_time.data());
  return 0;
}

