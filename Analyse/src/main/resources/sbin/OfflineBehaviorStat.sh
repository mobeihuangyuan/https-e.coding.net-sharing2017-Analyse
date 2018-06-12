#!/usr/bin/env bash

binDir=/apps/recommend/online/bin
currentDate=`date +%Y-%m-%d`
numOfDays=7
${binDir}/submit.sh com.sharing.output.stat.OfflineBehaviorStatToHDFS xia.jun --paramMap endDate=${currentDate},numOfDays=${numOfDays}