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

		MetaDataInputProto.Builder mdiBld = MetaDataInputProto.newBuilder();

		for (LocatedBlock lb : blockLocations.getLocatedBlocks()) {
			MetaDataInputProto.LocatedBlockProto.Builder lbBld 
				= MetaDataInputProto.LocatedBlockProto.newBuilder();
			lbBld.setStartOffset(pos);
			DFSClient.LOG.info("[compose] block offset: " + lb.getStartOffset() + ", location: ");
			for (DatanodeInfo di : lb.getLocations()) {
				MetaDataInputProto.LocatedBlockProto.DatanodeInfoProto.Builder diBld 
					= MetaDataInputProto.LocatedBlockProto.DatanodeInfoProto.newBuilder();
				diBld.setIpAddress(di.getIpAddr());
				diBld.setXferPort(di.getXferPort());
				diBld.setHostName(di.getHostName());
				DFSClient.LOG.info("  [compose] location: " + di);
				lbBld.addDi(diBld.build());
			}
			mdiBld.addLb(lbBld.build());
		}

		byte[] metaDataInput = mdiBld.build().toByteArray();
		System.arraycopy(metaDataInput, 0, 
			target, 0, 
			metaDataInput.length);
		
		if (pos + len < getFileLength()) {
			pos += len;
			return len;
		} else {
			pos = getFileLength();
			return (int)(getFileLength() - pos);
		}
		// dfsClient.checkOpen();
		// if (closed) {
		// 	throw new IOException("Stream closed");
		// }
		// Map<ExtendedBlock,Set<DatanodeInfo>> corruptedBlockMap 
		// = new HashMap<ExtendedBlock, Set<DatanodeInfo>>();
		// failures = 0;
		// if (pos < getFileLength()) {
		// 	int retries = 2;
		// 	while (retries > 0) {
		// 		try {
		//       // currentNode can be left as null if previous read had a checksum
		//       // error on the same block. See HDFS-3067
		// 			if (pos > blockEnd || currentNode == null) {
		// 				currentNode = blockSeekTo(pos);
		// 			}
		// 			int realLen = (int) Math.min(len, (blockEnd - pos + 1L));
		// 			if (locatedBlocks.isLastBlockComplete()) {
		// 				realLen = (int) Math.min(realLen, locatedBlocks.getFileLength());
		// 			}
		// 			int result = readBuffer(strategy, off, realLen, corruptedBlockMap);

		// 			if (result >= 0) {
		// 				pos += result;
		// 			} else {
		//         // got a EOS from reader though we expect more data on it.
		// 				throw new IOException("Unexpected EOS from the reader");
		// 			}
		// 			if (dfsClient.stats != null && result != -1) {
		// 				dfsClient.stats.incrementBytesRead(result);
		// 			}
		// 			return result;
		// 		} catch (ChecksumException ce) {
		// 			throw ce;            
		// 		} catch (IOException e) {
		// 			if (retries == 1) {
		// 				DFSClient.LOG.warn("DFS Read", e);
		// 			}
		// 			blockEnd = -1;
		// 			if (currentNode != null) { addToDeadNodes(currentNode); }
		// 			if (--retries == 0) {
		// 				throw e;
		// 			}
		// 		} finally {
		//       // Check if need to report block replicas corruption either read
		//       // was successful or ChecksumException occured.
		// 			reportCheckSumFailure(corruptedBlockMap, 
		// 				currentLocatedBlock.getLocations().length);
		// 		}
		// 	}
		// }
		// return -1;
	}
}