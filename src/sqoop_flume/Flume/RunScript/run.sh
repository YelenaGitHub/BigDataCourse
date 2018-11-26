#!/usr/bin/env bash

flume-ng agent --conf /home/cloudera/HW_FLUME/conf -f /home/cloudera/HW_FLUME/conf/linuxMsg.conf -n linuxMsgAgent -Dflume.root.logger=INFO,console