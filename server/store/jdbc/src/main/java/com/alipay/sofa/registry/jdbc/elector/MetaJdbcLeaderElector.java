/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.registry.jdbc.elector;

import com.alipay.sofa.registry.jdbc.config.MetaElectorConfig;
import com.alipay.sofa.registry.jdbc.constant.TableEnum;
import com.alipay.sofa.registry.jdbc.domain.DistributeLockDomain;
import com.alipay.sofa.registry.jdbc.domain.FollowCompeteLockDomain;
import com.alipay.sofa.registry.jdbc.mapper.DistributeLockMapper;
import com.alipay.sofa.registry.log.Logger;
import com.alipay.sofa.registry.log.LoggerFactory;
import com.alipay.sofa.registry.store.api.config.DefaultCommonConfig;
import com.alipay.sofa.registry.store.api.elector.AbstractLeaderElector;
import com.alipay.sofa.registry.store.api.meta.RecoverConfig;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author xiaojian.xj
 * @version $Id: MetaJdbcLeaderElector.java, v 0.1 2021年03月12日 10:18 xiaojian.xj Exp $
 */
public class MetaJdbcLeaderElector extends AbstractLeaderElector implements RecoverConfig {

  private static final Logger LOG =
      LoggerFactory.getLogger("META-ELECTOR", "[MetaJdbcLeaderElector]");

  public static final String lockName = "META-MASTER";

  @Autowired DistributeLockMapper distributeLockMapper;

  @Autowired MetaElectorConfig metaElectorConfig;

  @Autowired DefaultCommonConfig defaultCommonConfig;

  /**
   * start elect, return current leader
   *
   * @return
   */
  @Override
  protected LeaderInfo doElect() {
    // com.alipay.sofa.registry.store.api.elector.AbstractLeaderElector.init // @PostConstruct
    //  -> [async] com.alipay.sofa.registry.store.api.elector.AbstractLeaderElector.LeaderElectorTrigger.runUnthrowable
    //   -> com.alipay.sofa.registry.store.api.elector.AbstractLeaderElector.elect
    //    -> com.alipay.sofa.registry.store.api.elector.AbstractLeaderElector.doElect
    //     -> com.alipay.sofa.registry.jdbc.elector.MetaJdbcLeaderElector.doElect
    
    //distribute_lock表结构：
    //CREATE TABLE distribute_lock (
    //  id bigint(20) NOT NULL AUTO_INCREMENT primary key,
    //  data_center varchar(128) NOT NULL,
    //  lock_name varchar(1024) NOT NULL,
    //  owner varchar(512) NOT NULL,
    //  duration bigint(20) NOT NULL,
    //  term     bigint(20) unsigned NOT NULL DEFAULT 0 COMMENT '任期',
    //  term_duration    bigint(20) unsigned NOT NULL DEFAULT 0 COMMENT '租期',
    //  gmt_create timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    //  gmt_modified timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP, -- timestamp(3)精确到毫秒
    //  UNIQUE KEY `uk_data_center_lock` (`data_center`, `lock_name`),
    //  KEY `idx_lock_owner` (`owner`)
    //);
    
    // 1、查询锁
    DistributeLockDomain lock =
        distributeLockMapper.queryDistLock(defaultCommonConfig.getClusterId(tableName()), lockName);
    // dataCenter/defaultCommonConfig.getClusterId(tableName()): DefaultDataCenter
    // lockName: META-MASTER
    // sql:
    //   select
    //   /*+ QUERY_TIMEOUT(2000000) */ *,
    //   NOW(3) as gmt_db_server_time,
    //   UNIX_TIMESTAMP(gmt_modified)*1000000000 as gmt_modified_unixnanos
    //   from distribute_lock
    //   where data_center = #{dataCenter} and lock_name = #{lockName}

    /** compete and return leader */
    // 2、不存在则创建锁
    if (lock == null) {
      return competeLeader(defaultCommonConfig.getClusterId(tableName()));
    }
    // 3、判断角色
    ElectorRole role = amILeader(lock.getOwner()) ? ElectorRole.LEADER : ElectorRole.FOLLOWER;
    if (role == ElectorRole.LEADER) {
      lock = onLeaderWorking(lock, myself()); // 4、提交心跳
    } else {
      lock = onFollowWorking(lock, myself()); // 5、判断过期与否，如过期，则cas竞争锁
    }
    LeaderInfo result = leaderFrom(lock); // 6、锁信息转换为LeaderInfo
    LOG.info("meta role : {}, leaderInfo: {}", role, result);
    return result;
  }

  /**
   * compete and return leader
   *
   * @param dataCenter
   * @return
   */
  private LeaderInfo competeLeader(String dataCenter) {
    DistributeLockDomain lock =
        new DistributeLockDomain(
            dataCenter, lockName, myself(), metaElectorConfig.getLockExpireDuration());
    try {
      // throw exception if insert fail
      distributeLockMapper.competeLockOnInsert(lock);
      // compete finish.
      lock = distributeLockMapper.queryDistLock(dataCenter, lockName);

      LOG.info("meta: {} compete success, become leader.", myself());
    } catch (Throwable t) {
      // compete leader error, query current leader
      lock = distributeLockMapper.queryDistLock(dataCenter, lockName);
      LOG.info("meta: {} compete error, leader is: {}.", myself(), lock.getOwner());
    }
    return leaderFrom(lock);
  }

  public static LeaderInfo leaderFrom(DistributeLockDomain lock) {
    return calcLeaderInfo(
        lock.getOwner(),
        lock.getGmtModifiedUnixMillis(),
        lock.getGmtModifiedUnixMillis(),
        lock.getDuration());
  }
  /**
   * query current leader
   *
   * @return
   */
  @Override
  protected LeaderInfo doQuery() {
    DistributeLockDomain lock =
        distributeLockMapper.queryDistLock(defaultCommonConfig.getClusterId(tableName()), lockName);
    if (lock == null) {
      return LeaderInfo.HAS_NO_LEADER;
    }

    return leaderFrom(lock);
  }

  private DistributeLockDomain onLeaderWorking(DistributeLockDomain lock, String myself) {

    try {
      /** as leader, do heartbeat */
      distributeLockMapper.ownerHeartbeat(lock);
      LOG.info("leader heartbeat: {}", myself);
      return distributeLockMapper.queryDistLock(lock.getDataCenter(), lock.getLockName());
    } catch (Throwable t) {
      LOG.error("leader:{} heartbeat error.", myself, t);
    }
    return lock;
  }

  public DistributeLockDomain onFollowWorking(DistributeLockDomain lock, String myself) {
    /** as follow, do compete if lock expire */
    if (lock.expire()) {
      LOG.info("lock expire: {}, meta elector start: {}", lock, myself);
      distributeLockMapper.competeLockOnUpdate(
          new FollowCompeteLockDomain(
              lock.getDataCenter(),
              lock.getLockName(),
              lock.getOwner(),
              lock.getGmtModified(),
              myself,
              lock.getDuration(),
              lock.getTerm(),
              lock.getTermDuration()));
      DistributeLockDomain newLock =
          distributeLockMapper.queryDistLock(lock.getDataCenter(), lock.getLockName());
      LOG.info("elector finish, new lock: {}", lock);
      return newLock;
    }
    return lock;
  }

  @Override
  public String tableName() {
    return TableEnum.DISTRIBUTE_LOCK.getTableName();
  }
}
