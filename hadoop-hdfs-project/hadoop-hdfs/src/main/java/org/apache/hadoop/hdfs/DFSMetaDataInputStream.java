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
public class DFSMetaDataInputStream extends DFSInputStream {
	@VisibleForTesting
	static boolean tcpReadsDisabledForTesting = false;
	private final DFSClient dfsClient;
	private boolean closed = false;
	private final String src;
	private BlockReader blockReader = null;
	private final boolean verifyChecksum;
	private LocatedBlocks locatedBlocks = null;
	private long lastBlockBeingWrittenLength = 0;
	private DatanodeInfo currentNode = null;
	private LocatedBlock currentLocatedBlock = null;
	private long pos = 0;
	private long blockEnd = -1;
	private CachingStrategy cachingStrategy;
	private final ReadStatistics readStatistics = new ReadStatistics();
	
	DFSMetaDataInputStream(DFSClient dfsClient, String src, int buffersize, boolean verifyChecksum
		) throws IOException, UnresolvedLinkException {
		this.dfsClient = dfsClient;
		this.verifyChecksum = verifyChecksum;
		this.buffersize = buffersize;
		this.src = src;
		this.cachingStrategy =
		dfsClient.getDefaultReadCachingStrategy();
		openInfo();
	}
}