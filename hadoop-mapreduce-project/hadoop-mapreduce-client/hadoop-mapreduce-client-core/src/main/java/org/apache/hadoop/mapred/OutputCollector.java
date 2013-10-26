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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Collects the <code>&lt;key, value&gt;</code> pairs output by {@link Mapper}s
 * and {@link Reducer}s.
 *  
 * <p><code>OutputCollector</code> is the generalization of the facility 
 * provided by the Map-Reduce framework to collect data output by either the 
 * <code>Mapper</code> or the <code>Reducer</code> i.e. intermediate outputs 
 * or the output of the job.</p>  
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface OutputCollector<K, V> {
  
  /** Adds a key/value pair to the output.
   *
   * @param key the key to collect.
   * @param value to value to collect.
   * @throws IOException
   */
  void collect(K key, V value) throws IOException;
  void collect(K key, V value, long recordsRepresented) throws IOException;
}
