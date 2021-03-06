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

import static org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status.SUCCESS;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.BufferOverflowException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferEncryptor;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocol.proto.FileComposeProtos.MetaDataInputProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;


/****************************************************************
 * DFSOutputStream creates files from a stream of bytes.
 *
 * The client application writes data that is cached internally by
 * this stream. Data is broken up into packets, each packet is
 * typically 64K in size. A packet comprises of chunks. Each chunk
 * is typically 512 bytes and has an associated checksum with it.
 *
 * When a client application fills up the currentPacket, it is
 * enqueued into dataQueue.  The DataStreamer thread picks up
 * packets from the dataQueue, sends it to the first datanode in
 * the pipeline and moves it from the dataQueue to the ackQueue.
 * The ResponseProcessor receives acks from the datanodes. When an
 * successful ack for a packet is received from all datanodes, the
 * ResponseProcessor removes the corresponding packet from the
 * ackQueue.
 *
 * In case of error, all outstanding packets and moved from
 * ackQueue. A new pipeline is setup by eliminating the bad
 * datanode from the original pipeline. The DataStreamer now
 * starts sending packets from the dataQueue.
****************************************************************/
@InterfaceAudience.Private
public class DFSMetaDataOutputStream extends DFSOutputStream
implements Syncable, CanSetDropBehind {
	private DFSClient dfsClient;
	private String src;
	private final long fileId;
	private ExtendedBlock block = null;
	private Socket s;

	/** Construct a new output stream for creating a file. */
	public DFSMetaDataOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat,
		EnumSet<CreateFlag> flag, Progressable progress,
		DataChecksum checksum, String[] favoredNodes) throws IOException {
		super(dfsClient, src, stat, flag, progress, checksum, favoredNodes);
		this.dfsClient = dfsClient;
		this.src = src;
		this.fileId = stat.getFileId();
	}

	synchronized void start() {
		DFSClient.LOG.info("[compose] Starting DFSMetaDataOutputStream");
	}

	@Override
	public synchronized void write(byte b[], int off, int len)
	throws IOException {
		DFSClient.LOG.info("[compose] DFSMetaDataOutputStream writing a chunk with size " + 
			b.length);
		MetaDataInputProto mdiProto = MetaDataInputProto.parseFrom(b);

		for (MetaDataInputProto.LocatedBlockProto lbProto : mdiProto.getLbList()) {
			String[] favoredNodes = new String[lbProto.getDiList().size()];
			DFSClient.LOG.info("[compose] DFSMetaDataOutputStream received LocatedBlock " + lbProto.getStartOffset() + 
				", " + favoredNodes.length + " nodes.");
			int counter = 0;
			String localFileName = lbProto.getLocalFileName();
			for (MetaDataInputProto.LocatedBlockProto.DatanodeInfoProto diProto : lbProto.getDiList()) {
				favoredNodes[counter++] = diProto.getIpAddr() + ":" + diProto.getXferPort();
			}
			if (block != null) {
				block.setNumBytes(67108864);
			}
			LocatedBlock lb = dfsClient.namenode.addBlock(src, dfsClient.clientName,
				block, null, fileId, favoredNodes);
			DFSClient.LOG.info("[compose] DFSMetaDataOutputStream block starting offset: " + lbProto.getStartOffset() + 
				", returned block ID: " + lb.getBlock().getBlockId() + " at pool " + 
				lb.getBlock().getBlockPoolId());
			block = lb.getBlock();
			for (MetaDataInputProto.LocatedBlockProto.DatanodeInfoProto diProto : lbProto.getDiList()) {
				DatanodeInfo chosenNode = new DatanodeInfo(
					new DatanodeID(diProto.getIpAddr(),
						diProto.getHostName(),
						diProto.getDatanodeUuid(),
						diProto.getXferPort(),
						diProto.getInfoPort(),
						diProto.getInfoSecurePort(),
						diProto.getIpcPort()));
				s = createSocketForPipeline(chosenNode, 1, dfsClient);
				long writeTimeout = dfsClient.getDatanodeWriteTimeout(1);
				InputStream unbufIn = NetUtils.getInputStream(s);
				DataInputStream blockReplyStream = new DataInputStream(unbufIn);
				OutputStream unbufOut = NetUtils.getOutputStream(s, writeTimeout);
				DataOutputStream out = new DataOutputStream(new BufferedOutputStream(unbufOut,
					HdfsConstants.SMALL_BUFFER_SIZE));
				DatanodeInfo[] targets = {chosenNode};
				DFSClient.LOG.info("[compose] Connecting to DN " + chosenNode);
				new Sender(out).touchBlock(block, lb.getBlockToken(), dfsClient.clientName,
					targets, null, localFileName, null, 
					1, 0, 0, 0, null, null);
				BlockOpResponseProto resp = BlockOpResponseProto.parseFrom(
					PBHelper.vintPrefixed(blockReplyStream));
			}
			block.setNumBytes(lbProto.getNumBytes());
		}
		// MetaDataInputProto.LocatedBlockProto lb = 
	}

	/**
	 * Closes this output stream and releases any system 
	 * resources associated with this stream.
	 */
	@Override
	public synchronized void close() throws IOException {
		ExtendedBlock lastBlock = block;
		dfsClient.namenode.complete(src, dfsClient.clientName, lastBlock, fileId);
		dfsClient.endFileLease(src);
	}
}