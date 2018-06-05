#!/bin/bash
g++ clean_msg_count.cc -g -O2 -std=c++11 -L./hiredis-vip -Wl,-Bstatic -lhiredis_vip -Wl,-Bdynamic -lpthread -o clean_msg_count
g++ clean_offline_msgids.cc -g -O2 -std=c++11 -L./hiredis-vip -Wl,-Bstatic -lhiredis_vip -Wl,-Bdynamic -lpthread -o clean_offline_msgids
g++ clean_offline_msg.cc -g -O2 -std=c++11 -L./hiredis-vip -Wl,-Bstatic -lhiredis_vip -Wl,-Bdynamic -lpthread -o clean_offline_msg
g++ clean_msg_stats.cc -g -O2 -std=c++11 -L./hiredis-vip -Wl,-Bstatic -lhiredis_vip -Wl,-Bdynamic -lpthread -o clean_msg_stats
g++ message_info.cc -g -O2 -std=c++11 -L./hiredis-vip -Wl,-Bstatic -lhiredis_vip -Wl,-Bdynamic -lpthread -o message_info
