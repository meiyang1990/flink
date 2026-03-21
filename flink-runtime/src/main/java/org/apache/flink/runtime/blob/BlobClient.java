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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerProtocol.APPLICATION_RELATED_CONTENT;
import static org.apache.flink.runtime.blob.BlobServerProtocol.BUFFER_SIZE;
import static org.apache.flink.runtime.blob.BlobServerProtocol.GET_OPERATION;
import static org.apache.flink.runtime.blob.BlobServerProtocol.JOB_RELATED_CONTENT;
import static org.apache.flink.runtime.blob.BlobServerProtocol.JOB_UNRELATED_CONTENT;
import static org.apache.flink.runtime.blob.BlobServerProtocol.RETURN_ERROR;
import static org.apache.flink.runtime.blob.BlobServerProtocol.RETURN_OKAY;
import static org.apache.flink.runtime.blob.BlobUtils.readExceptionFromStream;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The BLOB client can communicate with the BLOB server and either upload (PUT), download (GET), or
 * delete (DELETE) BLOBs.
 *
 * <p>【学习型注释】BLOB 客户端，用于与 BlobServer 通信，执行文件的上传和下载操作。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li><b>PUT</b>：上传 BLOB（字节数组、InputStream、文件）</li>
 *   <li><b>GET</b>：下载 BLOB 到本地文件或 InputStream</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>JobManager 上传 Job JAR 包到 BlobServer</li>
 *   <li>TaskManager 从 BlobServer 下载 Job JAR 包</li>
 *   <li>Client 提交作业时上传用户代码</li>
 * </ul>
 *
 * <h2>连接特性</h2>
 * <ul>
 *   <li>每个 BlobClient 实例对应一个 TCP 连接</li>
 *   <li>支持 SSL/TLS 加密连接</li>
 *   <li>连接超时和读取超时可配置</li>
 *   <li>下载失败支持自动重试</li>
 * </ul>
 *
 * <h2>典型用法</h2>
 * <pre>{@code
 * try (BlobClient client = new BlobClient(serverAddress, config)) {
 *     PermanentBlobKey key = client.uploadFile(jobId, jarPath);
 *     // 或者
 *     InputStream is = client.getInternal(jobId, blobKey);
 * }
 * }</pre>
 */
public final class BlobClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(BlobClient.class);

    /**
     * 与 BlobServer 的 TCP Socket 连接。
     * 支持普通 Socket 和 SSL Socket。
     */
    private final Socket socket;

    /**
     * 创建 BlobClient 实例并建立与 BlobServer 的连接。
     *
     * <p>【学习型注释】连接建立流程：
     * <ol>
     *   <li>根据配置决定是否使用 SSL 连接</li>
     *   <li>使用 getHostString() 而非 getHostName() 避免不必要的 DNS 查询</li>
     *   <li>设置连接超时和读取超时</li>
     * </ol>
     *
     * @param serverAddress the network address of the BLOB server
     * @param clientConfig additional configuration like SSL parameters required to connect to the
     *     blob server
     * @throws IOException thrown if the connection to the BLOB server could not be established
     */
    public BlobClient(InetSocketAddress serverAddress, Configuration clientConfig)
            throws IOException {
        Socket socket = null;

        try {
            // create an SSL socket if configured
            if (SecurityOptions.isInternalSSLEnabled(clientConfig)
                    && clientConfig.get(BlobServerOptions.SSL_ENABLED)) {
                LOG.info("Using ssl connection to the blob server");

                socket = SSLUtils.createSSLClientSocketFactory(clientConfig).createSocket();
            } else {
                socket = new Socket();
            }

            // Establish the socket using the hostname and port. This avoids a potential issue where
            // the InetSocketAddress can cache a failure in hostname resolution forever.
            // Use getHostString() instead of getHostName() to avoid unnecessary reverse DNS and DNS
            // lookups when addresses are specified as IP literals. This improves reliability and
            // reduces latency for each blob transfer.
            socket.connect(
                    new InetSocketAddress(serverAddress.getHostString(), serverAddress.getPort()),
                    clientConfig.get(BlobServerOptions.CONNECT_TIMEOUT));
            socket.setSoTimeout(clientConfig.get(BlobServerOptions.SO_TIMEOUT));
        } catch (Exception e) {
            BlobUtils.closeSilently(socket, LOG);
            throw new IOException("Could not connect to BlobServer at address " + serverAddress, e);
        }

        this.socket = socket;
    }

    /**
     * Downloads the given BLOB from the given server and stores its contents to a (local) file.
     *
     * <p>Transient BLOB files are deleted after a successful copy of the server's data into the
     * given <tt>localJarFile</tt>.
     *
     * <p>【学习型注释】带重试机制的 BLOB 下载方法。
     * <ul>
     *   <li>每次重试都会新建 BlobClient 连接</li>
     *   <li>使用 try-with-resources 确保资源正确释放</li>
     *   <li>下载数据通过 BUFFER_SIZE（8KB）缓冲区逐块写入本地文件</li>
     *   <li>重试次数用完后抛出 IOException</li>
     * </ul>
     *
     * @param jobId job ID the BLOB belongs to or <tt>null</tt> if job-unrelated
     * @param blobKey BLOB key
     * @param localJarFile the local file to write to
     * @param serverAddress address of the server to download from
     * @param blobClientConfig client configuration for the connection
     * @param numFetchRetries number of retries before failing
     * @throws IOException if an I/O error occurs during the download
     */
    static void downloadFromBlobServer(
            @Nullable JobID jobId,
            BlobKey blobKey,
            File localJarFile,
            InetSocketAddress serverAddress,
            Configuration blobClientConfig,
            int numFetchRetries)
            throws IOException {

        final byte[] buf = new byte[BUFFER_SIZE];
        LOG.info("Downloading {}/{} from {}", jobId, blobKey, serverAddress);

        // loop over retries
        int attempt = 0;
        while (true) {
            try (final BlobClient bc = new BlobClient(serverAddress, blobClientConfig);
                    final InputStream is = bc.getInternal(jobId, blobKey);
                    final OutputStream os = new FileOutputStream(localJarFile)) {
                while (true) {
                    final int read = is.read(buf);
                    if (read < 0) {
                        break;
                    }
                    os.write(buf, 0, read);
                }

                return;
            } catch (Throwable t) {
                String message =
                        "Failed to fetch BLOB "
                                + jobId
                                + "/"
                                + blobKey
                                + " from "
                                + serverAddress
                                + " and store it under "
                                + localJarFile.getAbsolutePath();
                if (attempt < numFetchRetries) {
                    if (LOG.isDebugEnabled()) {
                        LOG.error(message + " Retrying...", t);
                    } else {
                        LOG.error(message + " Retrying...");
                    }
                } else {
                    LOG.error(message + " No retries left.", t);
                    throw new IOException(message, t);
                }

                // retry
                ++attempt;
                LOG.info(
                        "Downloading {}/{} from {} (retry {})",
                        jobId,
                        blobKey,
                        serverAddress,
                        attempt);
            }
        } // end loop over retries
    }

    /**
     * Downloads a BLOB (binary large object) from the {@link BlobServer} to a local file.
     *
     * @param applicationId ID of the application this blob belongs to
     * @param blobKey blob key identifying the file to be downloaded
     * @param localJarFile the local file to write to
     * @param serverAddress address of the server to download from
     * @param blobClientConfig client configuration for the connection
     * @param numFetchRetries number of retries before failing
     * @throws IOException if an I/O error occurs during the download
     */
    static void downloadFromBlobServer(
            ApplicationID applicationId,
            BlobKey blobKey,
            File localJarFile,
            InetSocketAddress serverAddress,
            Configuration blobClientConfig,
            int numFetchRetries)
            throws IOException {

        final byte[] buf = new byte[BUFFER_SIZE];
        LOG.info(
                "Downloading {} for application {} from {}", blobKey, applicationId, serverAddress);

        // loop over retries
        int attempt = 0;
        while (true) {
            try (final BlobClient bc = new BlobClient(serverAddress, blobClientConfig);
                    final InputStream is = bc.getInternal(applicationId, blobKey);
                    final OutputStream os = new FileOutputStream(localJarFile)) {
                while (true) {
                    final int read = is.read(buf);
                    if (read < 0) {
                        break;
                    }
                    os.write(buf, 0, read);
                }

                return;
            } catch (Throwable t) {
                String message =
                        "Failed to fetch BLOB "
                                + blobKey
                                + " for application "
                                + applicationId
                                + " from "
                                + serverAddress
                                + " and store it under "
                                + localJarFile.getAbsolutePath();
                if (attempt < numFetchRetries) {
                    if (LOG.isDebugEnabled()) {
                        LOG.error(message + " Retrying...", t);
                    } else {
                        LOG.error(message + " Retrying...");
                    }
                } else {
                    LOG.error(message + " No retries left.", t);
                    throw new IOException(message, t);
                }

                // retry
                ++attempt;
                LOG.info(
                        "Downloading {} for application {} from {} (retry {})",
                        blobKey,
                        applicationId,
                        serverAddress,
                        attempt);
            }
        } // end loop over retries
    }

    @Override
    public void close() throws IOException {
        this.socket.close();
    }

    public boolean isClosed() {
        return this.socket.isClosed();
    }

    public boolean isConnected() {
        return socket.isConnected();
    }

    // --------------------------------------------------------------------------------------------
    //  GET
    // --------------------------------------------------------------------------------------------

    /**
     * Downloads the BLOB identified by the given BLOB key from the BLOB server.
     *
     * <p>【学习型注释】GET 操作的协议流程：
     * <ol>
     *   <li>发送 GET 请求头：操作类型 + JobID/ApplicationID + BlobKey</li>
     *   <li>接收并校验响应：RETURN_OKAY 表示成功，RETURN_ERROR 表示失败</li>
     *   <li>返回 BlobInputStream 供调用方读取数据</li>
     * </ol>
     * 如果发生异常，会自动关闭 Socket 连接。
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param blobKey blob key associated with the requested file
     * @return an input stream to read the retrieved data from
     * @throws FileNotFoundException if there is no such file;
     * @throws IOException if an I/O error occurs during the download
     */
    InputStream getInternal(@Nullable JobID jobId, BlobKey blobKey) throws IOException {

        if (this.socket.isClosed()) {
            throw new IllegalStateException(
                    "BLOB Client is not connected. "
                            + "Client has been shut down or encountered an error before.");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("GET BLOB {}/{} from {}.", jobId, blobKey, socket.getLocalSocketAddress());
        }

        try {
            OutputStream os = this.socket.getOutputStream();
            InputStream is = this.socket.getInputStream();

            // Send GET header
            sendGetHeader(os, jobId, blobKey);
            receiveAndCheckGetResponse(is);

            return new BlobInputStream(is, blobKey, os);
        } catch (Throwable t) {
            BlobUtils.closeSilently(socket, LOG);
            throw new IOException("GET operation failed: " + t.getMessage(), t);
        }
    }

    /**
     * Downloads the BLOB identified by the given BLOB key from the BLOB server.
     *
     * @param applicationId ID of the application this blob belongs to
     * @param blobKey blob key associated with the requested file
     * @return an input stream to read the retrieved data from
     * @throws FileNotFoundException if there is no such file;
     * @throws IOException if an I/O error occurs during the download
     */
    InputStream getInternal(ApplicationID applicationId, BlobKey blobKey) throws IOException {

        if (this.socket.isClosed()) {
            throw new IllegalStateException(
                    "BLOB Client is not connected. "
                            + "Client has been shut down or encountered an error before.");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "GET BLOB {} for application {} from {}.",
                    blobKey,
                    applicationId,
                    socket.getLocalSocketAddress());
        }

        try {
            OutputStream os = this.socket.getOutputStream();
            InputStream is = this.socket.getInputStream();

            // Send GET header
            sendGetHeader(os, applicationId, blobKey);
            receiveAndCheckGetResponse(is);

            return new BlobInputStream(is, blobKey, os);
        } catch (Throwable t) {
            BlobUtils.closeSilently(socket, LOG);
            throw new IOException("GET operation failed: " + t.getMessage(), t);
        }
    }

    /**
     * Constructs and writes the header data for a GET operation to the given output stream.
     *
     * @param outputStream the output stream to write the header data to
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param blobKey blob key associated with the requested file
     * @throws IOException thrown if an I/O error occurs while writing the header data to the output
     *     stream
     */
    private static void sendGetHeader(
            OutputStream outputStream, @Nullable JobID jobId, BlobKey blobKey) throws IOException {
        checkNotNull(blobKey);
        checkArgument(
                jobId != null || blobKey instanceof TransientBlobKey,
                "permanent BLOBs must be job-related");

        // Signal type of operation
        outputStream.write(GET_OPERATION);

        // Send job ID and key
        if (jobId == null) {
            outputStream.write(JOB_UNRELATED_CONTENT);
        } else {
            outputStream.write(JOB_RELATED_CONTENT);
            outputStream.write(jobId.getBytes());
        }
        blobKey.writeToOutputStream(outputStream);
    }

    /**
     * Constructs and writes the header data for a GET operation to the given output stream.
     *
     * @param outputStream the output stream to write the header data to
     * @param applicationId ID of the application this blob belongs to
     * @param blobKey blob key associated with the requested file
     * @throws IOException thrown if an I/O error occurs while writing the header data to the output
     *     stream
     */
    private static void sendGetHeader(
            OutputStream outputStream, ApplicationID applicationId, BlobKey blobKey)
            throws IOException {
        checkNotNull(blobKey);
        checkNotNull(applicationId);

        // Signal type of operation
        outputStream.write(GET_OPERATION);

        // Send application ID and key
        outputStream.write(APPLICATION_RELATED_CONTENT);
        outputStream.write(applicationId.getBytes());
        blobKey.writeToOutputStream(outputStream);
    }

    /**
     * Reads the response from the input stream and throws in case of errors.
     *
     * @param is stream to read from
     * @throws IOException if the response is an error or reading the response failed
     */
    private static void receiveAndCheckGetResponse(InputStream is) throws IOException {
        int response = is.read();
        if (response < 0) {
            throw new EOFException("Premature end of response");
        }
        if (response == RETURN_ERROR) {
            Throwable cause = readExceptionFromStream(is);
            throw new IOException("Server side error: " + cause.getMessage(), cause);
        } else if (response != RETURN_OKAY) {
            throw new IOException("Unrecognized response");
        }
    }

    // --------------------------------------------------------------------------------------------
    //  PUT
    // --------------------------------------------------------------------------------------------

    /**
     * Uploads data from the given byte buffer to the BLOB server.
     *
     * <p>【学习型注释】PUT 操作的协议流程：
     * <ol>
     *   <li>创建 BlobOutputStream 封装上传逻辑</li>
     *   <li>发送 PUT 请求头：操作类型 + JobID + BlobType</li>
     *   <li>分块发送数据（每块 BUFFER_SIZE 字节）</li>
     *   <li>发送结束标记并接收服务端返回的 BlobKey</li>
     *   <li>校验本地计算的 hash 与服务端返回的是否一致</li>
     * </ol>
     *
     * @param jobId the ID of the job the BLOB belongs to (or <tt>null</tt> if job-unrelated)
     * @param value the buffer to read the data from
     * @param offset the read offset within the buffer
     * @param len the number of bytes to read from the buffer
     * @param blobType whether the BLOB should become permanent or transient
     * @return the computed BLOB key of the uploaded BLOB
     * @throws IOException thrown if an I/O error occurs while uploading the data to the BLOB server
     */
    BlobKey putBuffer(
            @Nullable JobID jobId, byte[] value, int offset, int len, BlobKey.BlobType blobType)
            throws IOException {

        if (this.socket.isClosed()) {
            throw new IllegalStateException(
                    "BLOB Client is not connected. "
                            + "Client has been shut down or encountered an error before.");
        }
        checkNotNull(value);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "PUT BLOB buffer ("
                            + len
                            + " bytes) to "
                            + socket.getLocalSocketAddress()
                            + ".");
        }

        try (BlobOutputStream os = new BlobOutputStream(jobId, blobType, socket)) {
            os.write(value, offset, len);
            // Receive blob key and compare
            return os.finish();
        } catch (Throwable t) {
            BlobUtils.closeSilently(socket, LOG);
            throw new IOException("PUT operation failed: " + t.getMessage(), t);
        }
    }

    /**
     * Uploads data from the given input stream to the BLOB server.
     *
     * @param jobId the ID of the job the BLOB belongs to (or <tt>null</tt> if job-unrelated)
     * @param inputStream the input stream to read the data from
     * @param blobType whether the BLOB should become permanent or transient
     * @return the computed BLOB key of the uploaded BLOB
     * @throws IOException thrown if an I/O error occurs while uploading the data to the BLOB server
     */
    BlobKey putInputStream(
            @Nullable JobID jobId, InputStream inputStream, BlobKey.BlobType blobType)
            throws IOException {

        if (this.socket.isClosed()) {
            throw new IllegalStateException(
                    "BLOB Client is not connected. "
                            + "Client has been shut down or encountered an error before.");
        }
        checkNotNull(inputStream);

        if (LOG.isDebugEnabled()) {
            LOG.debug("PUT BLOB stream to {}.", socket.getLocalSocketAddress());
        }

        try (BlobOutputStream os = new BlobOutputStream(jobId, blobType, socket)) {
            IOUtils.copyBytes(inputStream, os, BUFFER_SIZE, false);
            return os.finish();
        } catch (Throwable t) {
            BlobUtils.closeSilently(socket, LOG);
            throw new IOException("PUT operation failed: " + t.getMessage(), t);
        }
    }

    /**
     * Uploads the JAR files to the {@link PermanentBlobService} of the {@link BlobServer} at the
     * given address with HA as configured.
     *
     * <p>【学习型注释】批量上传 JAR 文件的便捷方法。
     * <ul>
     *   <li>复用同一个 BlobClient 连接上传多个文件</li>
     *   <li>返回每个文件对应的 PermanentBlobKey 列表</li>
     *   <li>常用于提交作业时上传依赖 JAR</li>
     * </ul>
     *
     * @param serverAddress Server address of the {@link BlobServer}
     * @param clientConfig Any additional configuration for the blob client
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param files List of files to upload
     * @throws IOException if the upload fails
     */
    public static List<PermanentBlobKey> uploadFiles(
            InetSocketAddress serverAddress,
            Configuration clientConfig,
            JobID jobId,
            List<Path> files)
            throws IOException {

        checkNotNull(jobId);

        if (files.isEmpty()) {
            return Collections.emptyList();
        } else {
            List<PermanentBlobKey> blobKeys = new ArrayList<>();

            try (BlobClient blobClient = new BlobClient(serverAddress, clientConfig)) {
                for (final Path file : files) {
                    final PermanentBlobKey key = blobClient.uploadFile(jobId, file);
                    blobKeys.add(key);
                }
            }

            return blobKeys;
        }
    }

    /**
     * Uploads a single file to the {@link PermanentBlobService} of the given {@link BlobServer}.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param file file to upload
     * @throws IOException if the upload fails
     */
    public PermanentBlobKey uploadFile(JobID jobId, Path file) throws IOException {
        final FileSystem fs = file.getFileSystem();
        try (InputStream is = fs.open(file)) {
            return (PermanentBlobKey) putInputStream(jobId, is, PERMANENT_BLOB);
        }
    }

    /**
     * Uploads data from the given input stream to the BLOB server, associating it with an
     * application ID.
     *
     * @param applicationId the ID of the application the BLOB belongs to
     * @param inputStream the input stream to read the data from
     * @param blobType whether the BLOB should become permanent or transient
     * @return the computed BLOB key of the uploaded BLOB
     * @throws IOException thrown if an I/O error occurs while uploading the data to the BLOB server
     */
    BlobKey putInputStream(
            ApplicationID applicationId, InputStream inputStream, BlobKey.BlobType blobType)
            throws IOException {

        if (this.socket.isClosed()) {
            throw new IllegalStateException(
                    "BLOB Client is not connected. "
                            + "Client has been shut down or encountered an error before.");
        }
        checkNotNull(inputStream);
        checkNotNull(applicationId);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "PUT BLOB stream for application {} to {}.",
                    applicationId,
                    socket.getLocalSocketAddress());
        }

        try (BlobOutputStream os = new BlobOutputStream(applicationId, blobType, socket)) {
            IOUtils.copyBytes(inputStream, os, BUFFER_SIZE, false);
            return os.finish();
        } catch (Throwable t) {
            BlobUtils.closeSilently(socket, LOG);
            throw new IOException("PUT operation failed: " + t.getMessage(), t);
        }
    }

    /**
     * Uploads a single file to the {@link PermanentBlobService} of the given {@link BlobServer},
     * associating it with an application ID.
     *
     * @param applicationId ID of the application this blob belongs to
     * @param file file to upload
     * @throws IOException if the upload fails
     */
    public PermanentBlobKey uploadFile(ApplicationID applicationId, Path file) throws IOException {
        final FileSystem fs = file.getFileSystem();
        try (InputStream is = fs.open(file)) {
            return (PermanentBlobKey) putInputStream(applicationId, is, PERMANENT_BLOB);
        }
    }
}
