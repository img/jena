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

import java.util.Iterator ;
import org.openjena.atlas.lib.ActionKeyValue;
import org.openjena.atlas.lib.Cache ;
import com.google.common.cache.CacheStats ;
import com.google.common.cache.CacheBuilder ;
import com.google.common.cache.RemovalListener ;
import com.google.common.cache.RemovalNotification;

/** Wrapper around a shaded com.google.common.cache */
final public class CacheGuava<K,V> implements Cache<K, V>
{
    private ActionKeyValue<K, V> dropHandler = null ;

    private com.google.common.cache.Cache<K,V> cache ;

    public CacheGuava(int size)
    {
        RemovalListener<K,V> drop = new RemovalListener<K,V>(){

			public void onRemoval(RemovalNotification<K, V> notification) {
				if ( CacheGuava.this.dropHandler != null )
					CacheGuava.this.dropHandler.apply(notification.getKey(),
	                                   notification.getValue()) ;
			}};

        this.cache = CacheBuilder.newBuilder()
            .maximumSize(size)
            .removalListener(drop)
            .recordStats()
            .concurrencyLevel(8)
            .build() ;
    }

    
    @Override
    public V get(K key) {
        return this.cache.getIfPresent(key) ;
    }

    @Override
    public V put(K key, V thing) {
    	V old = get(key);
		if (thing == null)
			this.cache.invalidate(key);
		else
			this.cache.put(key, thing);
		return old;
    }

    @Override
    public boolean containsKey(K key) {
        return this.cache.getIfPresent(key) != null ;
    }

    @Override
    public boolean remove(K key) {
    	boolean isPresent = containsKey(key);
        this.cache.invalidate(key) ;
        return isPresent;
    }

    @Override
    public Iterator<K> keys() {
        return this.cache.asMap().keySet().iterator() ;
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
    public void setDropHandler(ActionKeyValue<K,V> dropHandler) {
        this.dropHandler = dropHandler ;
    }

    public CacheStats stats() {
        return this.cache.stats() ;
    }
}
