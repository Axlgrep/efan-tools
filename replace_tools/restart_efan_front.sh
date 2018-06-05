#!/bin/bash
efan_front_host="
"

HOSTS=${efan_front_host}
user="wuxianjian-iri"
efan_front="./efan_front"
efan_front_conf="./efan_front_80.conf ./efan_front_443.conf"
delete_remote_efan_front="sudo rm -rf /data/efan/bin/efan_front"
delete_remote_efan_front_conf="sudo rm -rf /data/efan/conf/efan_front_80.conf /data/efan/conf/efan_front_443.conf"
cp_efan_front="sudo cp efan_front /data/efan/bin"
cp_efan_front_conf="sudo cp efan_front_80.conf efan_front_443.conf /data/efan/conf"
kill_remote_efan_front="ps -ef | grep efan_front | grep -v grep | awk '{print \$2}' | sudo xargs kill"
restart_efan_front_80="sudo /data/efan/bin/efan_front /data/efan/conf/efan_front_80.conf"
restart_efan_front_443="sudo /data/efan/bin/efan_front /data/efan/conf/efan_front_443.conf"
delete_home_path_efan_front="rm -rf efan_front efan_front_80.conf efan_front_443.conf"

for host in ${HOSTS[@]}
do
  ./auto scp $efan_front $user@$host:~/
  ./auto scp $efan_front_conf $user@$host:~/
  ./auto ssh $user@$host $delete_remote_efan_front
  ./auto ssh $user@$host $delete_remote_efan_front_conf
  ./auto ssh $user@$host $cp_efan_front
  ./auto ssh $user@$host $cp_efan_front_conf
  ./auto ssh $user@$host $kill_remote_efan_front
  ./auto ssh $user@$host $restart_efan_front_80
  ./auto ssh $user@$host $restart_efan_front_443
  ./auto ssh $user@$host $delete_home_path_efan_front
done

echo -e "\n\n*************Check process*************\n\n"
for host in ${HOSTS[@]}
do
  echo $host
  ./auto ssh $user@$host "ps -ef | grep efan_front | grep -v grep"
done
