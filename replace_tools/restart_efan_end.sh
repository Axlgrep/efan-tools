#!/bin/bash
efan_end_host="
"

HOSTS=${efan_end_host}
user="wuxianjian-iri"
efan_end="./efan_end"
efan_end_conf="./efan_end.conf"
delete_remote_efan_end="sudo rm -rf /data/efan/bin/efan_end"
delete_remote_efan_end_conf="sudo rm -rf /data/efan/conf/efan_end.conf"
cp_efan_end="sudo cp efan_end /data/efan/bin"
cp_efan_end_conf="sudo cp efan_end.conf /data/efan/conf/efan_end.conf"
kill_remote_efan_end="ps -ef | grep efan_end | grep -v grep | awk '{print \$2}' | sudo xargs kill"
restart_efan_end="sudo /data/efan/bin/efan_end /data/efan/conf/efan_end.conf"
delete_home_path_efan="rm -rf efan_end efan_end.conf"

for host in ${HOSTS[@]}
do
  ./auto scp $efan_end $user@$host:~/
  ./auto scp $efan_end_conf $user@$host:~/
  ./auto ssh $user@$host $delete_remote_efan_end
  ./auto ssh $user@$host $delete_remote_efan_end_conf
  ./auto ssh $user@$host $cp_efan_end
  ./auto ssh $user@$host $cp_efan_end_conf
  ./auto ssh $user@$host $kill_remote_efan_end
  ./auto ssh $user@$host $restart_efan_end
  ./auto ssh $user@$host $delete_home_path_efan
done

echo -e "\n\n*************Check process*************\n\n"
for host in ${HOSTS[@]}
do
  echo $host
  ./auto ssh $user@$host "ps -ef | grep efan_end | grep -v grep"
done
