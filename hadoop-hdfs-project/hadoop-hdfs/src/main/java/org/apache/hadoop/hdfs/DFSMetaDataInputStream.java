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
import org.apache.hadoop.hdfs.protocol.proto.FileComposeProtos.MetaDataInputProto;
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
 * DFSMetaDataInputStream provides bytes from a named file.  It handles 
 * negotiation of the namenode and various datanodes as necessary.
 ****************************************************************/
@InterfaceAudience.Private
public class DFSMetaDataInputStream extends DFSInputStream {
	private long pos = 0;
	private final DFSClient dfsClient;
	private final String src;

	DFSMetaDataInputStream(DFSClient dfsClient, String src, int buffersize, boolean verifyChecksum
		) throws IOException, UnresolvedLinkException {
		super(dfsClient, src, buffersize, verifyChecksum);
		this.dfsClient = dfsClient;
		this.src = src;

		DFSClient.LOG.info("[compose] Opening DFSMetaDataInputStream " + src);
	}

	/**
	 * Read the entire buffer.
	 */
	@Override
	public synchronized int read(final byte buf[], int off, int len) throws IOException {
		// DFSClient.LOG.info("[compose] Reading from DFSMetaDataInputStream " + off + "," 
		// 	+ len + ". File length is " + getFileLength() + ", position is " + pos);
		if (pos < 0 || pos >= getFileLength()) {
			return -1;
		}

		LocatedBlocks blockLocations = 
			dfsClient.getLocatedBlocks(src, pos, len);

		DFSClient.LOG.info("[compose] DFSMetaDataInputStream read() from " + pos 
			+ "-" + len + ", returned " + 
			blockLocations.locatedBlockCount() + " blocks");

		MetaDataInputProto.Builder mdiBld = MetaDataInputProto.newBuilder();

		for (LocatedBlock lb : blockLocations.getLocatedBlocks()) {
			MetaDataInputProto.LocatedBlockProto.Builder lbBld 
				= MetaDataInputProto.LocatedBlockProto.newBuilder();
			lbBld.setStartOffset(pos);
			DFSClient.LOG.info("[compose] In DFSMetaDataInputStream, block offset: " + 
				lb.getStartOffset() + ", block path is " + 
				lb.getBlock().getBlockPoolId() + "/" + lb.getBlock().getBlockName() +
				", location: ");
			for (DatanodeInfo di : lb.getLocations()) {
				MetaDataInputProto.LocatedBlockProto.DatanodeInfoProto.Builder diBld 
					= MetaDataInputProto.LocatedBlockProto.DatanodeInfoProto.newBuilder();
				diBld.setIpAddr(di.getIpAddr());
				diBld.setHostName(di.getHostName());
				if (di.getPeerHostName() != null) {
					diBld.setPeerHostName(di.getPeerHostName());
				}
				diBld.setXferPort(di.getXferPort());
				diBld.setInfoPort(di.getInfoPort());
				diBld.setInfoSecurePort(di.getInfoSecurePort());
				diBld.setIpcPort(di.getIpcPort());
				diBld.setDatanodeUuid(di.getDatanodeUuid());
				
				DFSClient.LOG.info("  [compose] location: " + di);
				lbBld.addDi(diBld.build());
			}
			lbBld.setLocalFileName("/tmp/hadoop-root/dfs/data/current/" + 
				lb.getBlock().getBlockPoolId() + 
				"/current/finalized/" + 
				lb.getBlock().getBlockName());
			lbBld.setNumBytes(lb.getBlock().getNumBytes());
			mdiBld.addLb(lbBld.build());
		}

		byte[] metaDataInput = mdiBld.build().toByteArray();
		DFSClient.LOG.info("[compose] metaDataInput size = " + metaDataInput.length);
		System.arraycopy(metaDataInput, 0, 
			buf, 0, 
			metaDataInput.length);

		if (pos + len < getFileLength()) {
			pos += len;
		} else {
			pos = getFileLength();
		}

		return metaDataInput.length;
	}
}