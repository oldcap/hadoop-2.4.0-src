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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.ByteBufferUtil;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.HasEnhancedByteBufferAccess;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.client.ClientMmap;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.IdentityHashStore;

import com.google.common.annotations.VisibleForTesting;

/****************************************************************
 * DFSInputStream provides bytes from a named file.  It handles 
 * negotiation of the namenode and various datanodes as necessary.
 ****************************************************************/
@InterfaceAudience.Private
public class DFSMetaDataInputStream extends FSInputStream
implements ByteBufferReadable, CanSetDropBehind, CanSetReadahead,
    HasEnhancedByteBufferAccess {
    	/**
    	 * Close it down!
    	 */
    	@Override
    	public synchronized void close() throws IOException {
    	  if (closed) {
    	    return;
    	  }
    	  dfsClient.checkOpen();

    	  if (!extendedReadBuffers.isEmpty()) {
    	    final StringBuilder builder = new StringBuilder();
    	    extendedReadBuffers.visitAll(new IdentityHashStore.Visitor<ByteBuffer, Object>() {
    	      private String prefix = "";
    	      @Override
    	      public void accept(ByteBuffer k, Object v) {
    	        builder.append(prefix).append(k);
    	        prefix = ", ";
    	      }
    	    });
    	    DFSClient.LOG.warn("closing file " + src + ", but there are still " +
    	        "unreleased ByteBuffers allocated by read().  " +
    	        "Please release " + builder.toString() + ".");
    	  }
    	  if (blockReader != null) {
    	    blockReader.close();
    	    blockReader = null;
    	  }
    	  super.close();
    	  closed = true;
    	}

    	@Override
    	public synchronized int read() throws IOException {
    	  int ret = read( oneByteBuf, 0, 1 );
    	  return ( ret <= 0 ) ? -1 : (oneByteBuf[0] & 0xff);
    	}

    	@Override
    	public int doRead(BlockReader blockReader, int off, int len,
    	        ReadStatistics readStatistics) throws ChecksumException, IOException {
    	    int nRead = blockReader.read(buf, off, len);
    	    updateReadStatistics(readStatistics, nRead, blockReader);
    	    return nRead;
    	}
    }