#!/usr/bin/env bash

giverTopK=1000
#intimacyThreshold=0.1
binDir=/apps/recommend/online/bin
$binDir/submit.sh com.sharing.models.relationships.RelationshipGraph xia.jun --isOnline true
$binDir/submit.sh com.sharing.models.recommendation.RelationshipRecSys xia.jun --isOnline true --paramMap giverTopK=${giverTopK}
$binDir/submit.sh com.sharing.output.QualityGiverToES xia.jun --isOnline true --algType quality_giver
$binDir/submit.sh com.sharing.output.IntimacyRelationToES xia.jun --isOnline true --algType intimacy_one_hot
$binDir/submit.sh com.sharing.output.GroupRelationshipToES xia.jun --isOnline true
#$binDir/submit.sh com.sharing.output.RelationshipResultToRedis xia.jun --isOnline true