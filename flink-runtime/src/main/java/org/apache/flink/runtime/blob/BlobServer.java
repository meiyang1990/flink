/*
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

package org.apache.flink.runtime.blob;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.dispatcher.cleanup.GloballyCleanableApplicationResource;
import org.apache.flink.runtime.dispatcher.cleanup.GloballyCleanableResource;
import org.apache.flink.runtime.dispatcher.cleanup.LocallyCleanableResource;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Reference;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.net.ServerSocketFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerProtocol.BUFFER_SIZE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class implements the BLOB server. The BLOB server is responsible for listening for incoming
 * requests and spawning threads to handle these requests. Furthermore, it takes care of creating
 * the directory structure to store the BLOBs or temporarily cache them.
 *
 * <p>【学习型注释】BLOB（Binary Large Object）服务器，负责大文件的存储和分发。
 *
 * <h2>核心职责</h2>
 * <ul>
 *   <li>接收并存储 Job JAR 包、用户代码等大文件</li>
 *   <li>为 TaskManager 提供文件下载服务</li>
 *   <li>支持 HA 模式下的持久化存储（通过 BlobStore）</li>
 *   <li>管理临时 BLOB（TransientBlob）的生命周期和清理</li>
 * </ul>
 *
 * <h2>BLOB 类型</h2>
 * <ul>
 *   <li><b>PermanentBlob</b>：永久 BLOB，存储在 HA 存储（如 HDFS/S3）中，Job 级别持久化</li>
 *   <li><b>TransientBlob</b>：临时 BLOB，仅存储在本地，有 TTL 过期清理机制</li>
 * </ul>
 *
 * <h2>网络协议</h2>
 * <ul>
 *   <li>基于 TCP Socket 的自定义二进制协议</li>
 *   <li>支持 GET（下载）和 PUT（上传）操作</li>
 *   <li>支持 SSL/TLS 加密传输</li>
 * </ul>
 *
 * <h2>存储结构</h2>
 * <pre>
 * storageDir/
 * ├── incoming/              # 上传中的临时文件
 * ├── no_job/                # Job 无关的 BLOB
 * ├── job_{jobId}/           # Job 相关的 BLOB（按 JobID 组织）
 * └── application_{appId}/   # Application 相关的 BLOB
 * </pre>
 *
 * <h2>并发安全</h2>
 * 使用 ReadWriteLock 保护文件操作，读操作可并发，写操作互斥。
 */
public class BlobServer extends Thread
        implements BlobService,
                BlobWriter,
                PermanentBlobService,
                TransientBlobService,
                LocallyCleanableResource,
                GloballyCleanableResource,
                GloballyCleanableApplicationResource {

    /** The log object used for debugging. */
    private static final Logger LOG = LoggerFactory.getLogger(BlobServer.class);

    /**
     * 临时文件计数器，用于生成唯一的临时文件名（temp-00000001、temp-00000002 等）。
     */
    private final AtomicLong tempFileCounter = new AtomicLong(0);

    /**
     * 服务端 Socket，监听客户端连接请求。
     * 如果 BlobServer 在构造函数完成前关闭，可能为 null。
     */
    // can be null if BlobServer is shut down before constructor completion
    @Nullable private final ServerSocket serverSocket;

    /**
     * BLOB 服务配置，包含端口范围、SSL 配置、连接数限制等。
     */
    private final Configuration blobServiceConfiguration;

    /**
     * 关闭请求标志，使用 AtomicBoolean 保证线程安全的状态检查。
     */
    private final AtomicBoolean shutdownRequested = new AtomicBoolean();

    /**
     * 本地存储根目录，所有 BLOB 文件都存储在此目录下。
     * 使用 Reference 封装，支持所有权转移（owned 时负责清理）。
     */
    private final Reference<File> storageDir;

    /**
     * 分布式 BLOB 存储，用于 HA 模式下的持久化。
     * 可以是 HDFS、S3 或其他分布式文件系统的实现。
     */
    private final BlobStore blobStore;

    /**
     * 当前活跃的客户端连接集合。
     * 每个连接由一个 BlobServerConnection 线程处理。
     */
    private final Set<BlobServerConnection> activeConnections = new HashSet<>();

    /**
     * 最大并发连接数，超过此限制的新连接需要等待。
     */
    private final int maxConnections;

    /**
     * 读写锁，保护文件操作的并发安全。
     * 读操作（getFile）可以并发执行，写操作（putBuffer/delete）需要互斥。
     */
    private final ReadWriteLock readWriteLock;

    /**
     * JVM 关闭钩子，确保 BlobServer 关闭时清理本地存储目录。
     */
    private final Thread shutdownHook;

    // --------------------------------------------------------------------------------------------

    /**
     * TransientBlob 的过期时间映射：(JobID, BlobKey) → 过期时间戳。
     * 每次访问 TransientBlob 时更新其 TTL，过期后由清理任务删除。
     */
    private final ConcurrentHashMap<Tuple2<JobID, TransientBlobKey>, Long> blobExpiryTimes =
            new ConcurrentHashMap<>();

    /**
     * 清理任务的执行间隔（毫秒），同时也作为 TransientBlob 的默认 TTL。
     */
    private final long cleanupInterval;

    /**
     * 定时清理任务，周期性检查并删除过期的 TransientBlob。
     */
    private final Timer cleanupTimer;

    @VisibleForTesting
    public BlobServer(Configuration config, File storageDir, BlobStore blobStore)
            throws IOException {
        this(config, Reference.owned(storageDir), blobStore);
    }

    /**
     * Instantiates a new BLOB server and binds it to a free network port.
     *
     * @param config Configuration to be used to instantiate the BlobServer
     * @param storageDir storage directory for the blobs
     * @param blobStore BlobStore to store blobs persistently
     * @throws IOException thrown if the BLOB server cannot bind to a free network port or if the
     *     (local or distributed) file storage cannot be created or is not usable
     */
    public BlobServer(Configuration config, Reference<File> storageDir, BlobStore blobStore)
            throws IOException {
        this.blobServiceConfiguration = checkNotNull(config);
        this.blobStore = checkNotNull(blobStore);
        this.readWriteLock = new ReentrantReadWriteLock();

        // configure and create the storage directory
        this.storageDir = storageDir;
        LOG.info("Created BLOB server storage directory {}", storageDir);

        // configure the maximum number of concurrent connections
        final int maxConnections = config.get(BlobServerOptions.FETCH_CONCURRENT);
        if (maxConnections >= 1) {
            this.maxConnections = maxConnections;
        } else {
            LOG.warn(
                    "Invalid value for maximum connections in BLOB server: {}. Using default value of {}",
                    maxConnections,
                    BlobServerOptions.FETCH_CONCURRENT.defaultValue());
            this.maxConnections = BlobServerOptions.FETCH_CONCURRENT.defaultValue();
        }

        // configure the backlog of connections
        int backlog = config.get(BlobServerOptions.FETCH_BACKLOG);
        if (backlog < 1) {
            LOG.warn(
                    "Invalid value for BLOB connection backlog: {}. Using default value of {}",
                    backlog,
                    BlobServerOptions.FETCH_BACKLOG.defaultValue());
            backlog = BlobServerOptions.FETCH_BACKLOG.defaultValue();
        }

        // Initializing the clean up task
        this.cleanupTimer = new Timer(true);

        this.cleanupInterval = config.get(BlobServerOptions.CLEANUP_INTERVAL) * 1000;
        this.cleanupTimer.schedule(
                new TransientBlobCleanupTask(blobExpiryTimes, this::deleteInternal, LOG),
                cleanupInterval,
                cleanupInterval);

        this.shutdownHook = ShutdownHookUtil.addShutdownHook(this, getClass().getSimpleName(), LOG);

        //  ----------------------- start the server -------------------

        final String serverPortRange = config.get(BlobServerOptions.PORT);
        final Iterator<Integer> ports = NetUtils.getPortRangeFromString(serverPortRange);

        final ServerSocketFactory socketFactory;
        if (SecurityOptions.isInternalSSLEnabled(config)
                && config.get(BlobServerOptions.SSL_ENABLED)) {
            try {
                socketFactory = SSLUtils.createSSLServerSocketFactory(config);
            } catch (Exception e) {
                throw new IOException("Failed to initialize SSL for the blob server", e);
            }
        } else {
            socketFactory = ServerSocketFactory.getDefault();
        }

        final int finalBacklog = backlog;
        final String bindHost =
                config.getOptional(JobManagerOptions.BIND_HOST)
                        .orElseGet(NetUtils::getWildcardIPAddress);

        this.serverSocket =
                NetUtils.createSocketFromPorts(
                        ports,
                        (port) ->
                                socketFactory.createServerSocket(
                                        port, finalBacklog, InetAddress.getByName(bindHost)));

        if (serverSocket == null) {
            throw new IOException(
                    "Unable to open BLOB Server in specified port range: " + serverPortRange);
        }

        // start the server thread
        setName("BLOB Server listener at " + getPort());
        setDaemon(true);

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Started BLOB server at {}:{} - max concurrent requests: {} - max backlog: {}",
                    serverSocket.getInetAddress().getHostAddress(),
                    getPort(),
                    maxConnections,
                    backlog);
        }

        checkStoredBlobsForCorruption();
        registerBlobExpiryTimes();
    }

    private void registerBlobExpiryTimes() throws IOException {
        if (storageDir.deref().exists()) {
            final Collection<BlobUtils.TransientBlob> transientBlobs =
                    BlobUtils.listTransientBlobsInDirectory(storageDir.deref().toPath());

            final long expiryTime = System.currentTimeMillis() + cleanupInterval;

            for (BlobUtils.TransientBlob transientBlob : transientBlobs) {
                blobExpiryTimes.put(
                        Tuple2.of(transientBlob.getJobId(), transientBlob.getBlobKey()),
                        expiryTime);
            }
        }
    }

    private void checkStoredBlobsForCorruption() throws IOException {
        if (storageDir.deref().exists()) {
            BlobUtils.checkAndDeleteCorruptedBlobs(storageDir.deref().toPath(), LOG);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Path Accessors
    // --------------------------------------------------------------------------------------------

    public File getStorageDir() {
        return storageDir.deref();
    }

    /**
     * Returns a file handle to the file associated with the given blob key on the blob server.
     *
     * <p><strong>This is only called from {@link BlobServerConnection} or unit tests.</strong>
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param key identifying the file
     * @return file handle to the file
     * @throws IOException if creating the directory fails
     */
    @VisibleForTesting
    public File getStorageLocation(@Nullable JobID jobId, BlobKey key) throws IOException {
        return BlobUtils.getStorageLocation(storageDir.deref(), jobId, key);
    }

    /**
     * Returns a file handle to the file associated with the given blob key on the blob server.
     *
     * <p><strong>This is only called from unit tests.</strong>
     *
     * @param applicationId ID of the application this blob belongs to
     * @param key identifying the file
     * @return file handle to the file
     * @throws IOException if creating the directory fails
     */
    @VisibleForTesting
    public File getStorageLocation(ApplicationID applicationId, BlobKey key) throws IOException {
        return BlobUtils.getStorageLocation(storageDir.deref(), applicationId, key);
    }

    /**
     * Returns a temporary file inside the BLOB server's incoming directory.
     *
     * @return a temporary file inside the BLOB server's incoming directory
     * @throws IOException if creating the directory fails
     */
    File createTemporaryFilename() throws IOException {
        return new File(
                BlobUtils.getIncomingDirectory(storageDir.deref()),
                String.format("temp-%08d", tempFileCounter.getAndIncrement()));
    }

    /** Returns the lock used to guard file accesses. */
    ReadWriteLock getReadWriteLock() {
        return readWriteLock;
    }

    /**
     * BlobServer 的主循环，持续监听并处理客户端连接。
     *
     * <p>【学习型注释】连接处理流程：
     * <ol>
     *   <li>通过 serverSocket.accept() 阻塞等待新连接</li>
     *   <li>检查当前活跃连接数是否达到上限，达到则等待</li>
     *   <li>为新连接创建 BlobServerConnection 线程处理请求</li>
     *   <li>连接完成后从 activeConnections 中移除</li>
     * </ol>
     * 当 shutdownRequested 为 true 或发生致命错误时退出循环。
     */
    @Override
    public void run() {
        try {
            while (!this.shutdownRequested.get()) {
                BlobServerConnection conn =
                        new BlobServerConnection(NetUtils.acceptWithoutTimeout(serverSocket), this);
                try {
                    synchronized (activeConnections) {
                        while (activeConnections.size() >= maxConnections) {
                            activeConnections.wait(2000);
                        }
                        activeConnections.add(conn);
                    }

                    conn.start();
                    conn = null;
                } finally {
                    if (conn != null) {
                        conn.close();
                        synchronized (activeConnections) {
                            activeConnections.remove(conn);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            if (!this.shutdownRequested.get()) {
                LOG.error("BLOB server stopped working. Shutting down", t);

                try {
                    close();
                } catch (Throwable closeThrowable) {
                    LOG.error("Could not properly close the BlobServer.", closeThrowable);
                }
            }
        }
    }

    /** Shuts down the BLOB server. */
    @Override
    public void close() throws IOException {
        cleanupTimer.cancel();

        if (shutdownRequested.compareAndSet(false, true)) {
            Exception exception = null;

            if (serverSocket != null) {
                try {
                    this.serverSocket.close();
                } catch (IOException ioe) {
                    exception = ioe;
                }
            }

            // wake the thread up, in case it is waiting on some operation
            interrupt();

            try {
                join();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();

                LOG.debug("Error while waiting for this thread to die.", ie);
            }

            synchronized (activeConnections) {
                if (!activeConnections.isEmpty()) {
                    for (BlobServerConnection conn : activeConnections) {
                        LOG.debug("Shutting down connection {}.", conn.getName());
                        conn.close();
                    }
                    activeConnections.clear();
                }
            }

            // Clean up the storage directory if it is owned
            try {
                storageDir
                        .owned()
                        .ifPresent(FunctionUtils.uncheckedConsumer(FileUtils::deleteDirectory));
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            // Remove shutdown hook to prevent resource leaks
            ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);

            if (LOG.isInfoEnabled()) {
                if (serverSocket != null) {
                    LOG.info(
                            "Stopped BLOB server at {}:{}",
                            serverSocket.getInetAddress().getHostAddress(),
                            getPort());
                } else {
                    LOG.info("Stopped BLOB server before initializing the socket");
                }
            }

            ExceptionUtils.tryRethrowIOException(exception);
        }
    }

    protected BlobClient createClient() throws IOException {
        return new BlobClient(
                new InetSocketAddress(serverSocket.getInetAddress(), getPort()),
                blobServiceConfiguration);
    }

    /**
     * Retrieves the local path of a (job-unrelated) file associated with a job and a blob key.
     *
     * <p>The blob server looks the blob key up in its local storage. If the file exists, it is
     * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
     * or a {@link FileNotFoundException} is thrown.
     *
     * @param key blob key associated with the requested file
     * @return file referring to the local storage location of the BLOB
     * @throws IOException Thrown if the file retrieval failed.
     */
    @Override
    public File getFile(TransientBlobKey key) throws IOException {
        return getFileInternalWithReadLock((JobID) null, key);
    }

    /**
     * Retrieves the local path of a file associated with a job and a blob key.
     *
     * <p>The blob server looks the blob key up in its local storage. If the file exists, it is
     * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
     * or a {@link FileNotFoundException} is thrown.
     *
     * @param jobId ID of the job this blob belongs to
     * @param key blob key associated with the requested file
     * @return file referring to the local storage location of the BLOB
     * @throws IOException Thrown if the file retrieval failed.
     */
    @Override
    public File getFile(JobID jobId, TransientBlobKey key) throws IOException {
        checkNotNull(jobId);
        return getFileInternalWithReadLock(jobId, key);
    }

    /**
     * Returns the path to a local copy of the file associated with the provided job ID and blob
     * key.
     *
     * <p>We will first attempt to serve the BLOB from the local storage. If the BLOB is not in
     * there, we will try to download it from the HA store.
     *
     * @param jobId ID of the job this blob belongs to
     * @param key blob key associated with the requested file
     * @return The path to the file.
     * @throws java.io.FileNotFoundException if the BLOB does not exist;
     * @throws IOException if any other error occurs when retrieving the file
     */
    @Override
    public File getFile(JobID jobId, PermanentBlobKey key) throws IOException {
        checkNotNull(jobId);
        return getFileInternalWithReadLock(jobId, key);
    }

    /**
     * Retrieves the local path of a file associated with a job and a blob key.
     *
     * <p>The blob server looks the blob key up in its local storage. If the file exists, it is
     * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
     * or a {@link FileNotFoundException} is thrown.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param blobKey blob key associated with the requested file
     * @return file referring to the local storage location of the BLOB
     * @throws IOException Thrown if the file retrieval failed.
     */
    private File getFileInternalWithReadLock(@Nullable JobID jobId, BlobKey blobKey)
            throws IOException {
        checkArgument(blobKey != null, "BLOB key cannot be null.");
        readWriteLock.readLock().lock();

        try {
            return getFileInternal(jobId, blobKey);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * Helper to retrieve the local path of a file associated with a job and a blob key.
     *
     * <p>The blob server looks the blob key up in its local storage. If the file exists, it is
     * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
     * or a {@link FileNotFoundException} is thrown.
     *
     * <p><strong>Assumes the read lock has already been acquired.</strong>
     *
     * <p>【学习型注释】BLOB 获取流程：
     * <ol>
     *   <li>首先检查本地存储是否存在该文件</li>
     *   <li>如果本地存在：对于 TransientBlob，更新 TTL 时间戳；直接返回文件</li>
     *   <li>如果本地不存在且是 PermanentBlob：尝试从 HA 存储下载</li>
     *   <li>下载时需要释放读锁、获取写锁，完成后重新获取读锁</li>
     *   <li>如果文件不存在且无法从 HA 存储获取，抛出 FileNotFoundException</li>
     * </ol>
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param blobKey blob key associated with the requested file
     * @throws IOException Thrown if the file retrieval failed.
     * @return the retrieved local blob file
     */
    File getFileInternal(@Nullable JobID jobId, BlobKey blobKey) throws IOException {
        // assume readWriteLock.readLock() was already locked (cannot really check that)

        final File localFile = BlobUtils.getStorageLocation(storageDir.deref(), jobId, blobKey);

        if (localFile.exists()) {
            // update TTL for transient BLOBs:
            if (blobKey instanceof TransientBlobKey) {
                // regarding concurrent operations, it is not really important which timestamp makes
                // it into the map as they are close to each other anyway, also we can simply
                // overwrite old values as long as we are in the read (or write) lock
                blobExpiryTimes.put(
                        Tuple2.of(jobId, (TransientBlobKey) blobKey),
                        System.currentTimeMillis() + cleanupInterval);
            }
            return localFile;
        } else if (blobKey instanceof PermanentBlobKey) {
            // Try the HA blob store
            // first we have to release the read lock in order to acquire the write lock
            readWriteLock.readLock().unlock();

            // use a temporary file (thread-safe without locking)
            File incomingFile = null;
            try {
                incomingFile = createTemporaryFilename();
                blobStore.get(jobId, blobKey, incomingFile);

                readWriteLock.writeLock().lock();
                try {
                    BlobUtils.moveTempFileToStore(
                            incomingFile, jobId, blobKey, localFile, LOG, null);
                } finally {
                    readWriteLock.writeLock().unlock();
                }

                return localFile;
            } finally {
                // delete incomingFile from a failed download
                if (incomingFile != null && !incomingFile.delete() && incomingFile.exists()) {
                    LOG.warn(
                            "Could not delete the staging file {} for blob key {} and job {}.",
                            incomingFile,
                            blobKey,
                            jobId);
                }

                // re-acquire lock so that it can be unlocked again outside
                readWriteLock.readLock().lock();
            }
        }

        throw new FileNotFoundException(
                "Local file "
                        + localFile
                        + " does not exist "
                        + "and failed to copy from blob store.");
    }

    /**
     * Returns the path to a local copy of the file associated with the provided application ID and
     * blob key.
     *
     * <p>We will first attempt to serve the BLOB from the local storage. If the BLOB is not in
     * there, we will try to download it from the HA store.
     *
     * @param applicationId ID of the application this blob belongs to
     * @param key BLOB key associated with the requested file
     * @return The path to the file.
     * @throws java.io.FileNotFoundException if the BLOB does not exist;
     * @throws IOException if any other error occurs when retrieving the file
     */
    @Override
    public File getFile(ApplicationID applicationId, PermanentBlobKey key) throws IOException {
        checkNotNull(applicationId);
        return getFileInternalWithReadLock(applicationId, key);
    }

    /**
     * Retrieves the local path of a file associated with an application and a blob key.
     *
     * <p>The blob server looks the blob key up in its local storage. If the file exists, it is
     * returned. If the file does not exist, it is retrieved from the HA blob store (if available)
     * or a {@link FileNotFoundException} is thrown.
     *
     * @param applicationId ID of the application this blob belongs to
     * @param blobKey blob key associated with the requested file
     * @return file referring to the local storage location of the BLOB
     * @throws IOException Thrown if the file retrieval failed.
     */
    private File getFileInternalWithReadLock(ApplicationID applicationId, BlobKey blobKey)
            throws IOException {
        checkArgument(blobKey instanceof PermanentBlobKey, "BLOB must be permanent.");
        readWriteLock.readLock().lock();

        try {
            return getFileInternal(applicationId, blobKey);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    File getFileInternal(ApplicationID applicationId, BlobKey blobKey) throws IOException {
        // assume readWriteLock.readLock() was already locked (cannot really check that)

        final File localFile =
                BlobUtils.getStorageLocation(storageDir.deref(), applicationId, blobKey);

        if (localFile.exists()) {
            return localFile;
        }

        // Try the HA blob store
        // first we have to release the read lock in order to acquire the write lock
        readWriteLock.readLock().unlock();

        // use a temporary file (thread-safe without locking)
        File incomingFile = null;
        try {
            incomingFile = createTemporaryFilename();
            blobStore.get(applicationId, blobKey, incomingFile);

            readWriteLock.writeLock().lock();
            try {
                BlobUtils.moveTempFileToStore(
                        incomingFile, applicationId, blobKey, localFile, LOG, null);
            } finally {
                readWriteLock.writeLock().unlock();
            }

            return localFile;
        } finally {
            // delete incomingFile from a failed download
            if (incomingFile != null && !incomingFile.delete() && incomingFile.exists()) {
                LOG.warn(
                        "Could not delete the staging file {} for blob key {} and application {}.",
                        incomingFile,
                        blobKey,
                        applicationId);
            }

            // re-acquire lock so that it can be unlocked again outside
            readWriteLock.readLock().lock();
        }
    }

    @Override
    public TransientBlobKey putTransient(byte[] value) throws IOException {
        return (TransientBlobKey) putBuffer((JobID) null, value, TRANSIENT_BLOB);
    }

    @Override
    public TransientBlobKey putTransient(JobID jobId, byte[] value) throws IOException {
        checkNotNull(jobId);
        return (TransientBlobKey) putBuffer(jobId, value, TRANSIENT_BLOB);
    }

    @Override
    public TransientBlobKey putTransient(InputStream inputStream) throws IOException {
        return (TransientBlobKey) putInputStream((JobID) null, inputStream, TRANSIENT_BLOB);
    }

    @Override
    public TransientBlobKey putTransient(JobID jobId, InputStream inputStream) throws IOException {
        checkNotNull(jobId);
        return (TransientBlobKey) putInputStream(jobId, inputStream, TRANSIENT_BLOB);
    }

    @Override
    public PermanentBlobKey putPermanent(JobID jobId, byte[] value) throws IOException {
        checkNotNull(jobId);
        return (PermanentBlobKey) putBuffer(jobId, value, PERMANENT_BLOB);
    }

    @Override
    public PermanentBlobKey putPermanent(JobID jobId, InputStream inputStream) throws IOException {
        checkNotNull(jobId);
        return (PermanentBlobKey) putInputStream(jobId, inputStream, PERMANENT_BLOB);
    }

    /**
     * Uploads the data of the given byte array for the given application to the BLOB server.
     *
     * @param applicationId the ID of the application the BLOB belongs to
     * @param value the buffer to upload
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while writing it to a local file, or
     *     uploading it to the HA store
     */
    @VisibleForTesting
    public PermanentBlobKey putPermanent(ApplicationID applicationId, byte[] value)
            throws IOException {
        checkNotNull(applicationId);
        return (PermanentBlobKey) putBuffer(applicationId, value, PERMANENT_BLOB);
    }

    /**
     * Uploads the data from the given input stream for the given application to the BLOB server.
     *
     * @param applicationId the ID of the application the BLOB belongs to
     * @param inputStream the input stream to read the data from
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while reading the data from the input
     *     stream, writing it to a local file, or uploading it to the HA store
     */
    @VisibleForTesting
    public PermanentBlobKey putPermanent(ApplicationID applicationId, InputStream inputStream)
            throws IOException {
        checkNotNull(applicationId);
        return (PermanentBlobKey) putInputStream(applicationId, inputStream, PERMANENT_BLOB);
    }

    /**
     * Uploads the data of the given byte array for the given job to the BLOB server.
     *
     * @param jobId the ID of the job the BLOB belongs to
     * @param value the buffer to upload
     * @param blobType whether to make the data permanent or transient
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while writing it to a local file, or
     *     uploading it to the HA store
     */
    private BlobKey putBuffer(@Nullable JobID jobId, byte[] value, BlobKey.BlobType blobType)
            throws IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Received PUT call for BLOB of job {}.", jobId);
        }

        File incomingFile = createTemporaryFilename();
        MessageDigest md = BlobUtils.createMessageDigest();
        BlobKey blobKey = null;
        try (FileOutputStream fos = new FileOutputStream(incomingFile)) {
            md.update(value);
            fos.write(value);
        } catch (IOException ioe) {
            // delete incomingFile from a failed download
            if (!incomingFile.delete() && incomingFile.exists()) {
                LOG.warn("Could not delete the staging file {} for job {}.", incomingFile, jobId);
            }
            throw ioe;
        }

        try {
            // persist file
            blobKey = moveTempFileToStore(incomingFile, jobId, md.digest(), blobType);

            return blobKey;
        } finally {
            // delete incomingFile from a failed download
            if (!incomingFile.delete() && incomingFile.exists()) {
                LOG.warn(
                        "Could not delete the staging file {} for blob key {} and job {}.",
                        incomingFile,
                        blobKey,
                        jobId);
            }
        }
    }

    /**
     * Uploads the data of the given byte array for the given application to the BLOB server.
     *
     * @param applicationId the ID of the application the BLOB belongs to
     * @param value the buffer to upload
     * @param blobType whether to make the data permanent or transient
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while writing it to a local file, or
     *     uploading it to the HA store
     */
    private BlobKey putBuffer(ApplicationID applicationId, byte[] value, BlobKey.BlobType blobType)
            throws IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Received PUT call for BLOB of application {}.", applicationId);
        }

        File incomingFile = createTemporaryFilename();
        MessageDigest md = BlobUtils.createMessageDigest();
        BlobKey blobKey = null;
        try (FileOutputStream fos = new FileOutputStream(incomingFile)) {
            md.update(value);
            fos.write(value);
        } catch (IOException ioe) {
            // delete incomingFile from a failed download
            if (!incomingFile.delete() && incomingFile.exists()) {
                LOG.warn(
                        "Could not delete the staging file {} for application {}.",
                        incomingFile,
                        applicationId);
            }
            throw ioe;
        }

        try {
            // persist file
            blobKey = moveTempFileToStore(incomingFile, applicationId, md.digest(), blobType);

            return blobKey;
        } finally {
            // delete incomingFile from a failed download
            if (!incomingFile.delete() && incomingFile.exists()) {
                LOG.warn(
                        "Could not delete the staging file {} for blob key {} and application {}.",
                        incomingFile,
                        blobKey,
                        applicationId);
            }
        }
    }

    /**
     * Uploads the data from the given input stream for the given job to the BLOB server.
     *
     * @param jobId the ID of the job the BLOB belongs to
     * @param inputStream the input stream to read the data from
     * @param blobType whether to make the data permanent or transient
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while reading the data from the input
     *     stream, writing it to a local file, or uploading it to the HA store
     */
    private BlobKey putInputStream(
            @Nullable JobID jobId, InputStream inputStream, BlobKey.BlobType blobType)
            throws IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Received PUT call for BLOB of job {}.", jobId);
        }

        File incomingFile = createTemporaryFilename();
        BlobKey blobKey = null;
        try {
            MessageDigest md = writeStreamToFileAndCreateDigest(inputStream, incomingFile);

            // persist file
            blobKey = moveTempFileToStore(incomingFile, jobId, md.digest(), blobType);

            return blobKey;
        } finally {
            // delete incomingFile from a failed download
            if (!incomingFile.delete() && incomingFile.exists()) {
                LOG.warn(
                        "Could not delete the staging file {} for blob key {} and job {}.",
                        incomingFile,
                        blobKey,
                        jobId);
            }
        }
    }

    /**
     * Uploads the data from the given input stream for the given application to the BLOB server.
     *
     * @param applicationId the ID of the application the BLOB belongs to
     * @param inputStream the input stream to read the data from
     * @param blobType whether to make the data permanent or transient
     * @return the computed BLOB key identifying the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while reading the data from the input
     *     stream, writing it to a local file, or uploading it to the HA store
     */
    private BlobKey putInputStream(
            ApplicationID applicationId, InputStream inputStream, BlobKey.BlobType blobType)
            throws IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Received PUT call for BLOB of application {}.", applicationId);
        }

        File incomingFile = createTemporaryFilename();
        BlobKey blobKey = null;
        try {
            MessageDigest md = writeStreamToFileAndCreateDigest(inputStream, incomingFile);

            // persist file
            blobKey = moveTempFileToStore(incomingFile, applicationId, md.digest(), blobType);

            return blobKey;
        } finally {
            // delete incomingFile from a failed download
            if (!incomingFile.delete() && incomingFile.exists()) {
                LOG.warn(
                        "Could not delete the staging file {} for blob key {} and application {}.",
                        incomingFile,
                        blobKey,
                        applicationId);
            }
        }
    }

    private static MessageDigest writeStreamToFileAndCreateDigest(
            InputStream inputStream, File file) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            MessageDigest md = BlobUtils.createMessageDigest();
            // read stream
            byte[] buf = new byte[BUFFER_SIZE];
            while (true) {
                final int bytesRead = inputStream.read(buf);
                if (bytesRead == -1) {
                    // done
                    break;
                }
                fos.write(buf, 0, bytesRead);
                md.update(buf, 0, bytesRead);
            }
            return md;
        }
    }

    /**
     * Moves the temporary <tt>incomingFile</tt> to its permanent location where it is available for
     * use.
     *
     * <p>【学习型注释】BLOB 上传存储流程：
     * <ol>
     *   <li>根据内容摘要（hash）生成 BlobKey</li>
     *   <li>获取写锁，检查目标文件是否已存在（hash 冲突检测）</li>
     *   <li>如果文件不存在：移动临时文件到目标位置</li>
     *   <li>对于 PermanentBlob：同步上传到 HA 存储</li>
     *   <li>对于 TransientBlob：记录 TTL 过期时间</li>
     *   <li>如果 hash 冲突（文件已存在），重新生成 BlobKey 并重试（最多 10 次）</li>
     * </ol>
     *
     * @param incomingFile temporary file created during transfer
     * @param jobId ID of the job this blob belongs to or <tt>null</tt> if job-unrelated
     * @param digest BLOB content digest, i.e. hash
     * @param blobType whether this file is a permanent or transient BLOB
     * @return unique BLOB key that identifies the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while moving the file or uploading it to
     *     the HA store
     */
    BlobKey moveTempFileToStore(
            File incomingFile, @Nullable JobID jobId, byte[] digest, BlobKey.BlobType blobType)
            throws IOException {

        int retries = 10;

        int attempt = 0;
        while (true) {
            // add unique component independent of the BLOB content
            BlobKey blobKey = BlobKey.createKey(blobType, digest);
            File storageFile = BlobUtils.getStorageLocation(storageDir.deref(), jobId, blobKey);

            // try again until the key is unique (put the existence check into the lock!)
            readWriteLock.writeLock().lock();
            try {
                if (!storageFile.exists()) {
                    BlobUtils.moveTempFileToStore(
                            incomingFile,
                            jobId,
                            blobKey,
                            storageFile,
                            LOG,
                            blobKey instanceof PermanentBlobKey ? blobStore : null);
                    // add TTL for transient BLOBs:
                    if (blobKey instanceof TransientBlobKey) {
                        // must be inside read or write lock to add a TTL
                        blobExpiryTimes.put(
                                Tuple2.of(jobId, (TransientBlobKey) blobKey),
                                System.currentTimeMillis() + cleanupInterval);
                    }
                    return blobKey;
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }

            ++attempt;
            if (attempt >= retries) {
                String message =
                        "Failed to find a unique key for BLOB of job "
                                + jobId
                                + " (last tried "
                                + storageFile.getAbsolutePath()
                                + ".";
                LOG.error(message + " No retries left.");
                throw new IOException(message);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Trying to find a unique key for BLOB of job {} (retry {}, last tried {})",
                            jobId,
                            attempt,
                            storageFile.getAbsolutePath());
                }
            }
        }
    }

    /**
     * Moves the temporary <tt>incomingFile</tt> to its permanent location where it is available for
     * use.
     *
     * @param incomingFile temporary file created during transfer
     * @param applicationId ID of the application this blob belongs to
     * @param digest BLOB content digest, i.e. hash
     * @param blobType whether this file is a permanent or transient BLOB
     * @return unique BLOB key that identifies the BLOB on the server
     * @throws IOException thrown if an I/O error occurs while moving the file or uploading it to
     *     the HA store
     */
    BlobKey moveTempFileToStore(
            File incomingFile,
            ApplicationID applicationId,
            byte[] digest,
            BlobKey.BlobType blobType)
            throws IOException {

        int retries = 10;

        int attempt = 0;
        while (true) {
            // add unique component independent of the BLOB content
            BlobKey blobKey = BlobKey.createKey(blobType, digest);
            File storageFile =
                    BlobUtils.getStorageLocation(storageDir.deref(), applicationId, blobKey);

            // try again until the key is unique (put the existence check into the lock!)
            readWriteLock.writeLock().lock();
            try {
                if (!storageFile.exists()) {
                    BlobUtils.moveTempFileToStore(
                            incomingFile, applicationId, blobKey, storageFile, LOG, blobStore);
                    return blobKey;
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }

            ++attempt;
            if (attempt >= retries) {
                String message =
                        "Failed to find a unique key for BLOB of application "
                                + applicationId
                                + " (last tried "
                                + storageFile.getAbsolutePath()
                                + ".";
                LOG.error(message + " No retries left.");
                throw new IOException(message);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Trying to find a unique key for BLOB of application {} (retry {}, last tried {})",
                            applicationId,
                            attempt,
                            storageFile.getAbsolutePath());
                }
            }
        }
    }

    /**
     * Deletes the (job-unrelated) file associated with the blob key in the local storage of the
     * blob server.
     *
     * @param key blob key associated with the file to be deleted
     * @return <tt>true</tt> if the given blob is successfully deleted or non-existing;
     *     <tt>false</tt> otherwise
     */
    @Override
    public boolean deleteFromCache(TransientBlobKey key) {
        return deleteInternal(null, key);
    }

    /**
     * Deletes the file associated with the blob key in the local storage of the blob server.
     *
     * @param jobId ID of the job this blob belongs to
     * @param key blob key associated with the file to be deleted
     * @return <tt>true</tt> if the given blob is successfully deleted or non-existing;
     *     <tt>false</tt> otherwise
     */
    @Override
    public boolean deleteFromCache(JobID jobId, TransientBlobKey key) {
        checkNotNull(jobId);
        return deleteInternal(jobId, key);
    }

    /**
     * Deletes the file associated with the blob key in the local storage of the blob server.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param key blob key associated with the file to be deleted
     * @return <tt>true</tt> if the given blob is successfully deleted or non-existing;
     *     <tt>false</tt> otherwise
     */
    boolean deleteInternal(@Nullable JobID jobId, TransientBlobKey key) {
        final File localFile =
                new File(
                        BlobUtils.getStorageLocationPath(
                                storageDir.deref().getAbsolutePath(), jobId, key));

        readWriteLock.writeLock().lock();

        try {
            if (!localFile.delete() && localFile.exists()) {
                LOG.warn(
                        "Failed to locally delete BLOB "
                                + key
                                + " at "
                                + localFile.getAbsolutePath());
                return false;
            }
            // this needs to happen inside the write lock in case of concurrent getFile() calls
            blobExpiryTimes.remove(Tuple2.of(jobId, key));
            return true;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Deletes the file associated with the blob key in the local storage of the blob server.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param key blob key associated with the file to be deleted
     * @return <tt>true</tt> if the given blob is successfully deleted or non-existing;
     *     <tt>false</tt> otherwise
     */
    private boolean deleteInternal(JobID jobId, PermanentBlobKey key) {
        final File localFile =
                new File(
                        BlobUtils.getStorageLocationPath(
                                storageDir.deref().getAbsolutePath(), jobId, key));

        readWriteLock.writeLock().lock();

        try {
            boolean deleteLocally = true;
            if (!localFile.delete() && localFile.exists()) {
                LOG.warn(
                        "Failed to locally delete BLOB "
                                + key
                                + " at "
                                + localFile.getAbsolutePath());
                deleteLocally = false;
            }
            // this needs to happen inside the write lock in case of concurrent getFile() calls
            boolean deleteHA = blobStore.delete(jobId, key);
            return deleteLocally && deleteHA;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Delete the uploaded data with the given {@link JobID} and {@link PermanentBlobKey}.
     *
     * @param jobId ID of the job this blob belongs to
     * @param key the key of this blob
     */
    @Override
    public boolean deletePermanent(JobID jobId, PermanentBlobKey key) {
        return deleteInternal(jobId, key);
    }

    /**
     * Deletes locally stored artifacts for the job represented by the given {@link JobID}. This
     * doesn't touch the job's entry in the {@link BlobStore} to enable recovering.
     *
     * @param jobId The {@code JobID} of the job that is subject to cleanup.
     */
    @Override
    public CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor cleanupExecutor) {
        checkNotNull(jobId);

        return runAsyncWithWriteLock(() -> internalLocalCleanup(jobId), cleanupExecutor);
    }

    @GuardedBy("readWriteLock")
    private void internalLocalCleanup(JobID jobId) throws IOException {
        final File jobDir =
                new File(
                        BlobUtils.getStorageLocationPath(
                                storageDir.deref().getAbsolutePath(), jobId));
        FileUtils.deleteDirectory(jobDir);

        // NOTE on why blobExpiryTimes are not cleaned up: Instead of going through
        // blobExpiryTimes, keep lingering entries. They will be cleaned up by the timer
        // task which tolerate non-existing files. If inserted again with the same IDs
        // (via put()), the TTL will be updated again.
    }

    /**
     * Removes all BLOBs from local and HA store belonging to the given {@link JobID}.
     *
     * @param jobId ID of the job this blob belongs to
     */
    @Override
    public CompletableFuture<Void> globalCleanupAsync(JobID jobId, Executor executor) {
        checkNotNull(jobId);

        return runAsyncWithWriteLock(
                () -> {
                    IOException exception = null;

                    try {
                        internalLocalCleanup(jobId);
                    } catch (IOException e) {
                        exception = e;
                    }

                    if (!blobStore.deleteAll(jobId)) {
                        exception =
                                ExceptionUtils.firstOrSuppressed(
                                        new IOException(
                                                "Error while cleaning up the BlobStore for job "
                                                        + jobId),
                                        exception);
                    }

                    if (exception != null) {
                        throw new IOException(exception);
                    }
                },
                executor);
    }

    @GuardedBy("readWriteLock")
    private void internalLocalCleanup(ApplicationID applicationId) throws IOException {
        final File applicationDir =
                new File(
                        BlobUtils.getStorageLocationPath(
                                storageDir.deref().getAbsolutePath(), applicationId));
        FileUtils.deleteDirectory(applicationDir);
    }

    /**
     * Removes all BLOBs from local and HA store belonging to the given {@link ApplicationID}.
     *
     * @param applicationId ID of the application this blob belongs to
     */
    @Override
    public CompletableFuture<Void> globalCleanupAsync(
            ApplicationID applicationId, Executor executor) {
        checkNotNull(applicationId);

        return runAsyncWithWriteLock(
                () -> {
                    IOException exception = null;

                    try {
                        internalLocalCleanup(applicationId);
                    } catch (IOException e) {
                        exception = e;
                    }

                    if (!blobStore.deleteAll(applicationId)) {
                        exception =
                                ExceptionUtils.firstOrSuppressed(
                                        new IOException(
                                                "Error while cleaning up the BlobStore for application "
                                                        + applicationId),
                                        exception);
                    }

                    if (exception != null) {
                        throw new IOException(exception);
                    }
                },
                executor);
    }

    private CompletableFuture<Void> runAsyncWithWriteLock(
            ThrowingRunnable<IOException> runnable, Executor executor) {
        return CompletableFuture.runAsync(
                () -> {
                    readWriteLock.writeLock().lock();
                    try {
                        runnable.run();
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    } finally {
                        readWriteLock.writeLock().unlock();
                    }
                },
                executor);
    }

    public void retainJobs(Collection<JobID> jobsToRetain, Executor ioExecutor) throws IOException {
        if (storageDir.deref().exists()) {
            final Set<JobID> jobsToRemove = BlobUtils.listExistingJobs(storageDir.deref().toPath());

            jobsToRemove.removeAll(jobsToRetain);

            final Collection<CompletableFuture<Void>> cleanupResultFutures =
                    new ArrayList<>(jobsToRemove.size());
            for (JobID jobToRemove : jobsToRemove) {
                cleanupResultFutures.add(globalCleanupAsync(jobToRemove, ioExecutor));
            }

            try {
                FutureUtils.completeAll(cleanupResultFutures).get();
            } catch (InterruptedException | ExecutionException e) {
                ExceptionUtils.rethrowIOException(e);
            }
        }
    }

    @Override
    public PermanentBlobService getPermanentBlobService() {
        return this;
    }

    @Override
    public TransientBlobService getTransientBlobService() {
        return this;
    }

    /**
     * Returns the configuration used by the BLOB server.
     *
     * @return configuration
     */
    @Override
    public final int getMinOffloadingSize() {
        return blobServiceConfiguration.get(BlobServerOptions.OFFLOAD_MINSIZE);
    }

    /**
     * Returns the port on which the server is listening.
     *
     * @return port on which the server is listening
     */
    @Override
    public int getPort() {
        return this.serverSocket.getLocalPort();
    }

    /**
     * Returns the address on which the server is listening.
     *
     * @return address on which the server is listening
     */
    @Override
    public InetAddress getAddress() {
        InetAddress bindAddr = serverSocket.getInetAddress();
        if (bindAddr.getHostAddress().equals(NetUtils.getWildcardIPAddress())) {
            try {
                return InetAddress.getLocalHost();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
        return bindAddr;
    }

    /**
     * Returns the blob expiry times - for testing purposes only!
     *
     * @return blob expiry times (internal state!)
     */
    @VisibleForTesting
    ConcurrentMap<Tuple2<JobID, TransientBlobKey>, Long> getBlobExpiryTimes() {
        return blobExpiryTimes;
    }

    /**
     * Tests whether the BLOB server has been requested to shut down.
     *
     * @return True, if the server has been requested to shut down, false otherwise.
     */
    public boolean isShutdown() {
        return this.shutdownRequested.get();
    }

    /** Access to the server socket, for testing. */
    ServerSocket getServerSocket() {
        return this.serverSocket;
    }

    void unregisterConnection(BlobServerConnection conn) {
        synchronized (activeConnections) {
            activeConnections.remove(conn);
            activeConnections.notifyAll();
        }
    }

    /**
     * Returns all the current active connections in the BlobServer.
     *
     * @return the list of all the active in current BlobServer
     */
    List<BlobServerConnection> getCurrentActiveConnections() {
        synchronized (activeConnections) {
            return new ArrayList<>(activeConnections);
        }
    }
}
