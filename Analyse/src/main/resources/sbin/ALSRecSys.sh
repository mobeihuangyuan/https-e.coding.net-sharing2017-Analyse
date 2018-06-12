#!/usr/bin/env bash

#基于ALS执行推荐模型的离线训练
numOfDays=7
iter=5
alpha=20
rank=20
regParam=0.01
topK=500
binDir=/apps/recommend/online/bin
$binDir/bin/submit.sh com.sharing.models.recommendation.ALSRecSys xia.jun --isOnline true --paramMap numOfDays=${numOfDays},iter=${iter},alpha=${alpha},rank=${rank},regParam=${regParam},topK=${topK}
$binDir/bin/submit.sh com.sharing.output.ALSResultToES xia.jun --isOnline true --algType als_baseline
