/*******************************************************************************
 * Licensed Materials - Property of IBM
 * � Copyright IBM Corporation 2018. All Rights Reserved.
 *
 * Note to U.S. Government Users Restricted Rights:
 * Use, duplication or disclosure restricted by GSA ADP Schedule
 * Contract with IBM Corp.
 *******************************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openjena.atlas.lib.cache;

import org.openjena.atlas.lib.CacheSet;

import com.google.common.cache.CacheBuilder ;

/** Wrapper around a shaded com.google.common.cache */
final public class CacheSetGuava<K> implements CacheSet<K>
{
    final static Object theOnlyValue = new Object() ;
    
    private com.google.common.cache.Cache<K,Object> cache ;

    public CacheSetGuava(int size)
    {
        this.cache = CacheBuilder.newBuilder()
            .maximumSize(size)
            .recordStats()
            .concurrencyLevel(8)
            .build() ;
    }


    @Override
    public boolean isEmpty() {
        return this.cache.size() == 0 ;
    }

    @Override
    public void clear() {
        this.cache.invalidateAll() ;
    }

    @Override
    public long size() {
        return this.cache.size() ;
    }

    
	@Override
	public void add(K e) {
		cache.put(e,  theOnlyValue);		
	}


	@Override
	public boolean contains(K obj) {
		return null != cache.getIfPresent(obj);
	}


	@Override
	public void remove(K obj) {
		cache.invalidate(obj);
	}
}
