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
package com.alipay.sofa.registry.server.data.cache;

import com.alipay.sofa.registry.common.model.ConnectId;
import com.alipay.sofa.registry.common.model.dataserver.Datum;
import com.alipay.sofa.registry.common.model.dataserver.DatumVersion;
import com.alipay.sofa.registry.common.model.store.Publisher;
import com.alipay.sofa.registry.server.data.bootstrap.DataServerConfig;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * cache of datum, providing query function to the upper module
 *
 * @author kezhu.wukz
 * @author qian.lqlq
 * @version $Id: DatumCache.java, v 0.1 2017-12-06 20:50 qian.lqlq Exp $
 */
public class DatumCache {
    @Autowired
    private DatumStorage     localDatumStorage;

    @Autowired
    private DataServerConfig dataServerConfig;

    /**
     * get datum by specific dataCenter and dataInfoId
     *
     * @param dataCenter
     * @param dataInfoId
     * @return
     */
    public Datum get(String dataCenter, String dataInfoId) {
        return localDatumStorage.get(dataInfoId);
    }

    /**
     * get datum of all data centers by dataInfoId
     *
     * @param dataInfoId
     * @return
     */
    public Map<String, Datum> get(String dataInfoId) {
        Map<String, Datum> datumMap = new HashMap<>();

        //local
        Datum localDatum = localDatumStorage.get(dataInfoId);
        if (localDatum != null) {
            datumMap.put(dataServerConfig.getLocalDataCenter(), localDatum);
        }
        return datumMap;
    }

    public void clean(String dataCenter, String dataInfoId) {
        localDatumStorage.remove(dataInfoId, null);
    }

    /**
     * get datum of all data centers by dataInfoId
     *
     * @param dataInfoId
     * @return
     */
    public Map<String, DatumVersion> getVersions(String dataInfoId) {
        Map<String, DatumVersion> datumMap = Maps.newHashMap();
        //local
        DatumVersion version = localDatumStorage.getVersion(dataInfoId);
        if (version != null) {
            datumMap.put(dataServerConfig.getLocalDataCenter(), version);
        }
        return datumMap;
    }

    public Map<String, Map<String, DatumVersion>> getVersions(int slotId) {
        Map<String, Map<String, DatumVersion>> datumMap = new HashMap<>();
        //local
        Map<String, DatumVersion> versions = localDatumStorage.getVersions(slotId);
        if (!CollectionUtils.isEmpty(versions)) {
            datumMap.put(dataServerConfig.getLocalDataCenter(), versions);
        }
        return datumMap;
    }

    /**
     * get datum group by dataCenter
     *
     * @param dataCenter
     * @param dataInfoId
     * @return
     */
    public Map<String, Datum> getDatumGroupByDataCenter(String dataCenter, String dataInfoId) {
        Map<String, Datum> map = new HashMap<>();
        if (StringUtils.isEmpty(dataCenter)) {
            map = this.get(dataInfoId);
        } else {
            Datum datum = this.get(dataCenter, dataInfoId);
            if (datum != null) {
                map.put(dataCenter, datum);
            }
        }
        return map;
    }

    /**
     * get all datum
     *
     * @return
     */
    public Map<String, Map<String, Datum>> getAll() {
        Map<String, Map<String, Datum>> datumMap = new HashMap<>();
        datumMap.put(dataServerConfig.getLocalDataCenter(), localDatumStorage.getAll());
        return datumMap;
    }

    public Map<String, Publisher> getByConnectId(ConnectId connectId) {
        return localDatumStorage.getByConnectId(connectId);
    }
}