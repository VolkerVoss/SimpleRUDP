/*
 * Simple Reliable UDP (rudp)
 * Copyright (c) 2009, Adrian Granados (agranados@ihmc.us)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS AND CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package net.rudp;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;

import net.rudp.impl.ACKSegment;
import net.rudp.impl.DATSegment;
import net.rudp.impl.EAKSegment;
import net.rudp.impl.FINSegment;
import net.rudp.impl.NULSegment;
import net.rudp.impl.RSTSegment;
import net.rudp.impl.SYNSegment;
import net.rudp.impl.Segment;
import net.rudp.impl.Timer;

/**
 * This class implements client sockets that use
 * the Simple Reliable UDP (RUDP) protocol for
 * end-to-end communication between two machines.
 *
 * @author Adrian Granados
 * @see java.net.Socket
 */
public class ReliableSocket extends Socket {
    /**
     * Creates an unconnected RUDP socket with default RUDP parameters.
     *
     * @throws IOException
     *             if an I/O error occurs when
     *             creating the underlying UDP socket.
     */
    public ReliableSocket()
	    throws IOException {
	this(new ReliableSocketProfile());
    }

    /**
     * Creates an unconnected RUDP socket and uses the given RUDP parameters.
     *
     * @throws IOException
     *             if an I/O error occurs when
     *             creating the underlying UDP socket.
     */
    public ReliableSocket(final ReliableSocketProfile profile)
	    throws IOException {
	this(new DatagramSocket(), profile);
    }

    /**
     * Creates a RUDP socket and connects it to the specified port
     * number on the named host.
     * <p>
     * If the specified host is <tt>null</tt> it is the equivalent of
     * specifying the address as <tt>{@link java.net.InetAddress#getByName InetAddress.getByName}(null)</tt>.
     * In other words, it is equivalent to specifying an address of the
     * loopback interface.
     *
     * @param host
     *            the host name, or <code>null</code> for the loopback address.
     * @param port
     *            the port number.
     *
     * @throws UnknownHostException
     *             if the IP address of the host could not be determined.
     * @throws IOException
     *             if an I/O error occurs when creating the socket.
     * @throws IllegalArgumentException
     *             if the port parameter is outside the specified range
     *             of valid port values, which is between 0 and 65535, inclusive.
     * @see java.net.Socket#Socket(String, int)
     */
    public ReliableSocket(final String host, final int port)
	    throws UnknownHostException, IOException {
	this(new InetSocketAddress(host, port), null);
    }

    /**
     * Creates a RUDP socket and connects it to the specified remote address on
     * the specified remote port. The socket will also bind to the local address
     * and port supplied.
     * <p>
     * If the specified local address is <tt>null</tt> it is the equivalent of
     * specifying the address as the wildcard address
     * (see <tt>{@link java.net.InetAddress#isAnyLocalAddress InetAddress.isAnyLocalAddress}()</tt>).
     * <p>
     * A local port number of <code>zero</code> will let the system pick up a
     * free port in the <code>bind</code> operation.
     *
     * @param address
     *            the remote address.
     * @param port
     *            the remote port.
     * @param localAddr
     *            the local address the socket is bound to, or
     *            <code>null</code> for the wildcard address.
     * @param localPort
     *            the local port the socket is bound to, or
     *            <code>zero</code> for a system selected free port.
     * @throws IOException
     *             if an I/O error occurs when creating the socket.
     * @throws IllegalArgumentException
     *             if the port parameter is outside the specified range
     *             of valid port values, which is between 0 and 65535, inclusive.
     */
    public ReliableSocket(final InetAddress address, final int port, final InetAddress localAddr, final int localPort)
	    throws IOException {
	this(new InetSocketAddress(address, port),
		new InetSocketAddress(localAddr, localPort));
    }

    /**
     * Creates a RUDP socket and connects it to the specified remote host on
     * the specified remote port. The socket will also bind to the local address
     * and port supplied.
     * <p>
     * If the specified host is <tt>null</tt> it is the equivalent of
     * specifying the address as <tt>{@link java.net.InetAddress#getByName InetAddress.getByName}(null)</tt>.
     * In other words, it is equivalent to specifying an address of the
     * loopback interface.
     * <p>
     * A local port number of <code>zero</code> will let the system pick up a
     * free port in the <code>bind</code> operation.
     *
     * @param host
     *            the name of the remote host, or <code>null</code> for the loopback address.
     * @param port
     *            the remote port.
     * @param localAddr
     *            the local address the socket is bound to, or
     *            <code>null</code> for the wildcard address.
     * @param localPort
     *            the local port the socket is bound to, or
     *            <code>zero</code> for a system selected free port.
     * @throws IOException
     *             if an I/O error occurs when creating the socket.
     * @throws IllegalArgumentException
     *             if the port parameter is outside the specified range
     *             of valid port values, which is between 0 and 65535, inclusive.
     */
    public ReliableSocket(final String host, final int port, final InetAddress localAddr, final int localPort)
	    throws IOException {
	this(new InetSocketAddress(host, port),
		new InetSocketAddress(localAddr, localPort));
    }

    /**
     * Creates a RUDP socket and connects it to the specified remote address. The
     * socket will also bind to the local address supplied.
     *
     * @param inetAddr
     *            the remote address.
     * @param localAddr
     *            the local address.
     * @throws IOException
     *             if an I/O error occurs when creating the socket.
     */
    protected ReliableSocket(final InetSocketAddress inetAddr, final InetSocketAddress localAddr)
	    throws IOException {
	this(new DatagramSocket(localAddr), new ReliableSocketProfile());
	connect(inetAddr);
    }

    /**
     * Creates a RUDP socket and attaches it to the underlying datagram socket.
     *
     * @param sock
     *            the datagram socket.
     */
    protected ReliableSocket(final DatagramSocket sock) {
	this(sock, new ReliableSocketProfile());
    }

    /**
     * Creates a RUDP socket and attaches it to the underlying
     * datagram socket using the given RUDP parameters.
     *
     * @param sock
     *            the datagram socket.
     * @param profile
     *            the socket profile.
     */
    protected ReliableSocket(final DatagramSocket sock, final ReliableSocketProfile profile) {
	if (sock == null) {
	    throw new NullPointerException("sock");
	}

	init(sock, profile);
    }

    /**
     * Initializes socket and sets it up for receiving incoming traffic.
     *
     * @param sock
     *            the datagram socket.
     * @param profile
     *            the socket profile.
     */
    protected void init(final DatagramSocket sock, final ReliableSocketProfile profile) {
	this._sock = sock;
	this._profile = profile;
	this._shutdownHook = new ShutdownHook();

	this._sendBufferSize = (this._profile.maxSegmentSize() - Segment.RUDP_HEADER_LEN) * 32;
	this._recvBufferSize = (this._profile.maxSegmentSize() - Segment.RUDP_HEADER_LEN) * 32;

	/* Register shutdown hook */
	try {
	    Runtime.getRuntime().addShutdownHook(this._shutdownHook);
	} catch (final IllegalStateException xcp) {
	    if (ReliableSocket.DEBUG) {
		xcp.printStackTrace();
	    }
	}

	this._sockThread.start();
    }

    @Override
    public void bind(final SocketAddress bindpoint)
	    throws IOException {
	this._sock.bind(bindpoint);
    }

    @Override
    public void connect(final SocketAddress endpoint)
	    throws IOException {
	connect(endpoint, 0);
    }

    @Override
    public void connect(final SocketAddress endpoint, final int timeout)
	    throws IOException {
	if (endpoint == null) {
	    throw new IllegalArgumentException("connect: The address can't be null");
	}

	if (timeout < 0) {
	    throw new IllegalArgumentException("connect: timeout can't be negative");
	}

	if (isClosed()) {
	    throw new SocketException("Socket is closed");
	}

	if (isConnected()) {
	    throw new SocketException("already connected");
	}

	if (!(endpoint instanceof InetSocketAddress)) {
	    throw new IllegalArgumentException("Unsupported address type");
	}

	this._endpoint = endpoint;

	// Synchronize sequence numbers
	this._state = ReliableSocket.SYN_SENT;
	final Random rand = new Random(System.currentTimeMillis());
	final Segment syn = new SYNSegment(this._counters.setSequenceNumber(rand.nextInt(ReliableSocket.MAX_SEQUENCE_NUMBER)),
		this._profile.maxOutstandingSegs(),
		this._profile.maxSegmentSize(),
		this._profile.retransmissionTimeout(),
		this._profile.cumulativeAckTimeout(),
		this._profile.nullSegmentTimeout(),
		this._profile.maxRetrans(),
		this._profile.maxCumulativeAcks(),
		this._profile.maxOutOfSequence(),
		this._profile.maxAutoReset());

	sendAndQueueSegment(syn);

	// Wait for connection establishment (or timeout)
	boolean timedout = false;
	synchronized (this) {
	    if (!isConnected()) {
		try {
		    if (timeout == 0) {
			wait();
		    } else {
			final long startTime = System.currentTimeMillis();
			wait(timeout);
			if ((System.currentTimeMillis() - startTime) >= timeout) {
			    timedout = true;
			}
		    }
		} catch (final InterruptedException xcp) {
		    xcp.printStackTrace();
		}
	    }
	}

	if (this._state == ReliableSocket.ESTABLISHED) {
	    return;
	}

	synchronized (this._unackedSentQueue) {
	    this._unackedSentQueue.clear();
	    this._unackedSentQueue.notifyAll();
	}

	this._counters.reset();
	this._retransmissionTimer.cancel();

	switch (this._state) {
	case SYN_SENT:
	    connectionRefused();
	    this._state = ReliableSocket.CLOSED;
	    if (timedout) {
		throw new SocketTimeoutException();
	    }
	    throw new SocketException("Connection refused");
	case CLOSED:
	case CLOSE_WAIT:
	    this._state = ReliableSocket.CLOSED;
	    throw new SocketException("Socket closed");
	}
    }

    @Override
    public SocketChannel getChannel() {
	return null;
    }

    @Override
    public InetAddress getInetAddress() {
	if (!isConnected()) {
	    return null;
	}

	return ((InetSocketAddress) this._endpoint).getAddress();
    }

    @Override
    public int getPort() {
	if (!isConnected()) {
	    return 0;
	}

	return ((InetSocketAddress) this._endpoint).getPort();

    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
	if (!isConnected()) {
	    return null;
	}

	return new InetSocketAddress(getInetAddress(), getPort());
    }

    @Override
    public InetAddress getLocalAddress() {
	return this._sock.getLocalAddress();
    }

    @Override
    public int getLocalPort() {
	return this._sock.getLocalPort();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
	return this._sock.getLocalSocketAddress();
    }

    @Override
    public InputStream getInputStream()
	    throws IOException {
	if (isClosed()) {
	    throw new SocketException("Socket is closed");
	}

	if (!isConnected()) {
	    throw new SocketException("Socket is not connected");
	}

	if (isInputShutdown()) {
	    throw new SocketException("Socket input is shutdown");
	}

	return this._in;
    }

    @Override
    public OutputStream getOutputStream()
	    throws IOException {
	if (isClosed()) {
	    throw new SocketException("Socket is closed");
	}

	if (!isConnected()) {
	    throw new SocketException("Socket is not connected");
	}

	if (isOutputShutdown()) {
	    throw new SocketException("Socket output is shutdown");
	}

	return this._out;
    }

    @Override
    public synchronized void close()
	    throws IOException {
	synchronized (this._closeLock) {

	    if (isClosed()) {
		return;
	    }

	    try {
		Runtime.getRuntime().removeShutdownHook(this._shutdownHook);
	    } catch (final IllegalStateException xcp) {
		if (ReliableSocket.DEBUG) {
		    xcp.printStackTrace();
		}
	    }

	    switch (this._state) {
	    case SYN_SENT:
		synchronized (this) {
		    notify();
		}
		break;
	    case CLOSE_WAIT:
	    case SYN_RCVD:
	    case ESTABLISHED:
		sendSegment(new FINSegment(this._counters.nextSequenceNumber()));
		closeImpl();
		break;
	    case CLOSED:
		this._retransmissionTimer.destroy();
		this._cumulativeAckTimer.destroy();
		this._keepAliveTimer.destroy();
		this._nullSegmentTimer.destroy();
		this._sock.close();
		break;
	    }

	    this._closed = true;
	    this._state = ReliableSocket.CLOSED;

	    synchronized (this._unackedSentQueue) {
		this._unackedSentQueue.notify();
	    }

	    synchronized (this._inSeqRecvQueue) {
		this._inSeqRecvQueue.notify();
	    }
	}
    }

    @Override
    public boolean isBound() {
	return this._sock.isBound();
    }

    @Override
    public boolean isConnected() {
	return this._connected;
    }

    @Override
    public boolean isClosed() {
	synchronized (this._closeLock) {
	    return this._closed;
	}
    }

    @Override
    public void setSoTimeout(final int timeout)
	    throws SocketException {
	if (timeout < 0) {
	    throw new IllegalArgumentException("timeout < 0");
	}

	this._timeout = timeout;
    }

    @Override
    public synchronized void setSendBufferSize(final int size)
	    throws SocketException {
	if (!(size > 0)) {
	    throw new IllegalArgumentException("negative receive size");
	}

	if (isClosed()) {
	    throw new SocketException("Socket is closed");
	}

	if (isConnected()) {
	    return;
	}

	this._sendBufferSize = size;
    }

    @Override
    public synchronized int getSendBufferSize()
	    throws SocketException {
	if (isClosed()) {
	    throw new SocketException("Socket is closed");
	}

	return this._sendBufferSize;
    }

    @Override
    public synchronized void setReceiveBufferSize(final int size)
	    throws SocketException {
	if (!(size > 0)) {
	    throw new IllegalArgumentException("negative send size");
	}

	if (isClosed()) {
	    throw new SocketException("Socket is closed");
	}

	if (isConnected()) {
	    return;
	}

	this._recvBufferSize = size;
    }

    @Override
    public synchronized int getReceiveBufferSize()
	    throws SocketException {
	if (isClosed()) {
	    throw new SocketException("Socket is closed");
	}

	return this._recvBufferSize;
    }

    @Override
    public void setTcpNoDelay(final boolean on)
	    throws SocketException {
	throw new SocketException("Socket option not supported");
    }

    @Override
    public boolean getTcpNoDelay() {
	return false;
    }

    @Override
    public synchronized void setKeepAlive(final boolean on)
	    throws SocketException {
	if (isClosed()) {
	    throw new SocketException("Socket is closed");
	}

	if (!(this._keepAlive ^ on)) {
	    return;
	}

	this._keepAlive = on;

	if (isConnected()) {
	    if (this._keepAlive) {
		this._keepAliveTimer.schedule(this._profile.nullSegmentTimeout() * 6,
			this._profile.nullSegmentTimeout() * 6);
	    } else {
		this._keepAliveTimer.cancel();
	    }
	}
    }

    @Override
    public synchronized boolean getKeepAlive()
	    throws SocketException {
	if (isClosed()) {
	    throw new SocketException("Socket is closed");
	}

	return this._keepAlive;
    }

    @Override
    public void shutdownInput()
	    throws IOException {
	if (isClosed()) {
	    throw new SocketException("Socket is closed");
	}

	if (!isConnected()) {
	    throw new SocketException("Socket is not connected");
	}

	if (isInputShutdown()) {
	    throw new SocketException("Socket input is already shutdown");
	}

	this._shutIn = true;

	synchronized (this._recvQueueLock) {
	    this._recvQueueLock.notify();
	}
    }

    @Override
    public void shutdownOutput()
	    throws IOException {
	if (isClosed()) {
	    throw new SocketException("Socket is closed");
	}

	if (!isConnected()) {
	    throw new SocketException("Socket is not connected");
	}

	if (isOutputShutdown()) {
	    throw new SocketException("Socket output is already shutdown");
	}

	this._shutOut = true;

	synchronized (this._unackedSentQueue) {
	    this._unackedSentQueue.notifyAll();
	}
    }

    @Override
    public boolean isInputShutdown() {
	return this._shutIn;
    }

    @Override
    public boolean isOutputShutdown() {
	return this._shutOut;
    }

    /**
     * Resets the socket state.
     * <p>
     * The socket will attempt to deliver all outstanding bytes to the remote
     * endpoint and then it will renegotiate the connection parameters.
     * The transmissions of bytes resumes after the renegotation finishes and
     * the connection is synchronized again.
     *
     * @throws IOException
     *             if an I/O error occurs when resetting the connection.
     */
    public void reset()
	    throws IOException {
	reset(null);
    }

    /**
     * Resets the socket state and profile.
     * <p>
     * The socket will attempt to deliver all outstanding bytes to the remote
     * endpoint and then it will renegotiate the connection parameters
     * specified in the given socket profile.
     * The transmissions of bytes resumes after the renegotation finishes and
     * the connection is synchronized again.
     *
     * @param profile
     *            the socket profile or null if old profile should be used.
     *
     * @throws IOException
     *             if an I/O error occurs when resetting the connection.
     */
    public void reset(final ReliableSocketProfile profile)
	    throws IOException {
	if (isClosed()) {
	    throw new SocketException("Socket is closed");
	}

	if (!isConnected()) {
	    throw new SocketException("Socket is not connected");
	}

	synchronized (this._resetLock) {
	    this._reset = true;

	    sendAndQueueSegment(new RSTSegment(this._counters.nextSequenceNumber()));

	    // Wait to flush all outstanding segments (including last RST segment).
	    synchronized (this._unackedSentQueue) {
		while (!this._unackedSentQueue.isEmpty() && isClosed()) {
		    try {
			Thread.sleep(10);
			// this._unackedSentQueue.wait();
		    } catch (final InterruptedException xcp) {
			xcp.printStackTrace();
		    }
		}
	    }
	}

	connectionReset();

	// Set new profile
	if (profile != null) {
	    this._profile = profile;
	}

	// Synchronize sequence numbers
	this._state = ReliableSocket.SYN_SENT;
	final Random rand = new Random(System.currentTimeMillis());
	final Segment syn = new SYNSegment(this._counters.setSequenceNumber(rand.nextInt(ReliableSocket.MAX_SEQUENCE_NUMBER)),
		this._profile.maxOutstandingSegs(),
		this._profile.maxSegmentSize(),
		this._profile.retransmissionTimeout(),
		this._profile.cumulativeAckTimeout(),
		this._profile.nullSegmentTimeout(),
		this._profile.maxRetrans(),
		this._profile.maxCumulativeAcks(),
		this._profile.maxOutOfSequence(),
		this._profile.maxAutoReset());

	sendAndQueueSegment(syn);
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> as data segments and
     * queues them for immediate transmission.
     *
     * @param b
     *            the data.
     * @param off
     *            the start offset in the data.
     * @param len
     *            the number of bytes to write.
     * @throws IOException
     *             if an I/O error occurs. In particular,
     *             an <code>IOException</code> is thrown if the socket
     *             is closed.
     */
    protected void write(final byte[] b, final int off, final int len)
	    throws IOException {
	if (isClosed()) {
	    throw new SocketException("Socket is closed");
	}

	if (isOutputShutdown()) {
	    throw new IOException("Socket output is shutdown");
	}

	if (!isConnected()) {
	    throw new SocketException("Connection reset");
	}

	int totalBytes = 0;
	while (totalBytes < len) {
	    synchronized (this._resetLock) {
		while (this._reset) {
		    try {
			this._resetLock.wait();
		    } catch (final InterruptedException xcp) {
			xcp.printStackTrace();
		    }
		}

		final int writeBytes = Math.min(this._profile.maxSegmentSize() - Segment.RUDP_HEADER_LEN,
			len - totalBytes);

		sendAndQueueSegment(new DATSegment(this._counters.nextSequenceNumber(),
			this._counters.getLastInSequence(), b, off + totalBytes, writeBytes));
		totalBytes += writeBytes;
	    }
	}
    }

    /**
     * Reads up to <code>len</code> bytes of data from the receiver
     * buffer into an array of bytes. An attempt is made to read
     * as many as <code>len</code> bytes, but a smaller number may
     * be read. The number of bytes actually read is returned as
     * an integer.
     * <p>
     * This method blocks until input data is available, end of file is
     * detected, or an exception is thrown.
     *
     * @param b
     *            the buffer into which the data is read.
     * @param off
     *            the start offset in array <code>b</code>
     *            at which the data is written.
     * @param len
     *            the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer,
     *         or <code>-1</code> if there is no more data because
     *         the end of the stream has been reached.
     * @throws IOException
     *             if an I/O error occurs. In particular,
     *             an <code>IOException</code> is thrown if the socket
     *             is closed, or if the buffer is not big enough to hold
     *             a full data segment.
     */
    protected int read(final byte[] b, final int off, final int len)
	    throws IOException {

	int totalBytes = 0;

	synchronized (this._recvQueueLock) {

	    while (true) {
		while (this._inSeqRecvQueue.isEmpty()) {

		    if (isClosed()) {
			throw new SocketException("Socket is closed");
		    }

		    if (isInputShutdown()) {
			throw new EOFException();
		    }

		    if (!isConnected()) {
			throw new SocketException("Connection reset");
		    }

		    try {
			if (this._timeout == 0) {
			    this._recvQueueLock.wait();
			} else {
			    final long startTime = System.currentTimeMillis();
			    this._recvQueueLock.wait(this._timeout);
			    if ((System.currentTimeMillis() - startTime) >= this._timeout) {
				throw new SocketTimeoutException();
			    }
			}
		    } catch (final InterruptedException xcp) {
			xcp.printStackTrace();
		    }
		}

		for (final Iterator<Segment> it = this._inSeqRecvQueue.iterator(); it.hasNext();) {
		    final Segment s = it.next();

		    if (s instanceof RSTSegment) {
			it.remove();
			break;
		    } else if (s instanceof FINSegment) {
			if (totalBytes <= 0) {
			    it.remove();
			    return -1; /* EOF */
			}
			break;
		    } else if (s instanceof DATSegment) {
			final byte[] data = ((DATSegment) s).getData();
			if ((data.length + totalBytes) > len) {
			    if (totalBytes <= 0) {
				throw new IOException("insufficient buffer space");
			    }
			    break;
			}

			System.arraycopy(data, 0, b, off + totalBytes, data.length);
			totalBytes += data.length;
			it.remove();
		    }
		}

		if (totalBytes > 0) {
		    return totalBytes;
		}
	    }
	}
    }

    /**
     * Adds the specified listener to this socket. If the listener
     * has already been registered, this method does nothing.
     *
     * @param listener
     *            the listener to add.
     */
    public void addListener(final ReliableSocketListener listener) {
	if (listener == null) {
	    throw new NullPointerException("listener");
	}

	synchronized (this._listeners) {
	    if (!this._listeners.contains(listener)) {
		this._listeners.add(listener);
	    }
	}
    }

    /**
     * Removes the specified listener from this socket. This is
     * harmless if the listener was not previously registered.
     *
     * @param listener
     *            the listener to remove.
     */
    public void removeListener(final ReliableSocketListener listener) {
	if (listener == null) {
	    throw new NullPointerException("listener");
	}

	synchronized (this._listeners) {
	    this._listeners.remove(listener);
	}
    }

    /**
     * Adds the specified state listener to this socket. If the listener
     * has already been registered, this method does nothing.
     *
     * @param stateListener
     *            the listener to add.
     */
    public void addStateListener(final ReliableSocketStateListener stateListener) {
	if (stateListener == null) {
	    throw new NullPointerException("stateListener");
	}

	synchronized (this._stateListeners) {
	    if (!this._stateListeners.contains(stateListener)) {
		this._stateListeners.add(stateListener);
	    }
	}
    }

    /**
     * Removes the specified state listener from this socket. This is
     * harmless if the listener was not previously registered.
     *
     * @param stateListener
     *            the listener to remove.
     */
    public void removeStateListener(final ReliableSocketStateListener stateListener) {
	if (stateListener == null) {
	    throw new NullPointerException("stateListener");
	}

	synchronized (this._stateListeners) {
	    this._stateListeners.remove(stateListener);
	}
    }

    /**
     * Sends a segment piggy-backing any pending acknowledgments.
     *
     * @param s
     *            the segment.
     * @throws IOException
     *             if an I/O error occurs in the
     *             underlying UDP socket.
     */
    private void sendSegment(final Segment s)
	    throws IOException {
	/* Piggyback any pending acknowledgments */
	if ((s instanceof DATSegment) || (s instanceof RSTSegment) || (s instanceof FINSegment) || (s instanceof NULSegment)) {
	    checkAndSetAck(s);
	}

	/* Reset null segment timer */
	if ((s instanceof DATSegment) || (s instanceof RSTSegment) || (s instanceof FINSegment)) {
	    this._nullSegmentTimer.reset();
	}

	if (ReliableSocket.DEBUG) {
	    log("sent " + s);
	}

	sendSegmentImpl(s);
    }

    /**
     * Receives a segment and increases the cumulative
     * acknowledgment counter.
     *
     * @return the received segment.
     * @throws IOException
     *             if an I/O error occurs in the
     *             underlying UDP socket.
     */
    private Segment receiveSegment()
	    throws IOException {
	Segment s;
	if ((s = receiveSegmentImpl()) != null) {

	    if (ReliableSocket.DEBUG) {
		log("recv " + s);
	    }

	    if ((s instanceof DATSegment) || (s instanceof NULSegment) ||
		    (s instanceof RSTSegment) || (s instanceof FINSegment) ||
		    (s instanceof SYNSegment)) {
		this._counters.incCumulativeAckCounter();
	    }

	    if (this._keepAlive) {
		this._keepAliveTimer.reset();
	    }
	}

	return s;
    }

    /**
     * Sends a segment and queues a copy of it in the queue of unacknowledged segments.
     *
     * @param segment
     *            a segment for which delivery must be guaranteed.
     * @throws IOException
     *             if an I/O error occurs in the
     *             underlying UDP socket.
     */
    private void sendAndQueueSegment(final Segment segment)
	    throws IOException {
	synchronized (this._unackedSentQueue) {
	    while ((this._unackedSentQueue.size() >= this._sendQueueSize) ||
		    (this._counters.getOutstandingSegsCounter() > this._profile.maxOutstandingSegs())) {
		try {
		    this._unackedSentQueue.wait();
		} catch (final InterruptedException xcp) {
		    xcp.printStackTrace();
		}
	    }

	    this._counters.incOutstandingSegsCounter();
	    this._unackedSentQueue.add(segment);
	}

	if (this._closed) {
	    throw new SocketException("Socket is closed");
	}

	/* Re-start retransmission timer */
	if (!(segment instanceof EAKSegment) && !(segment instanceof ACKSegment)) {
	    synchronized (this._retransmissionTimer) {
		if (this._retransmissionTimer.isIdle()) {
		    this._retransmissionTimer.schedule(this._profile.retransmissionTimeout(),
			    this._profile.retransmissionTimeout());
		}
	    }
	}

	sendSegment(segment);

	if (segment instanceof DATSegment) {
	    synchronized (this._listeners) {
		for (final ReliableSocketListener l : this._listeners) {
		    l.packetSent();
		}
	    }
	}
    }

    /**
     * Sends a segment and increments its retransmission counter.
     *
     * @param segment
     *            the segment to be retransmitted.
     * @throws IOException
     *             if an I/O error occurs in the
     *             underlying UDP socket.
     */
    private void retransmitSegment(final Segment segment)
	    throws IOException {
	if (this._profile.maxRetrans() > 0) {
	    segment.setRetxCounter(segment.getRetxCounter() + 1);
	}

	if ((this._profile.maxRetrans() != 0) && (segment.getRetxCounter() > this._profile.maxRetrans())) {
	    connectionFailure();
	    return;
	}

	sendSegment(segment);

	if (segment instanceof DATSegment) {
	    synchronized (this._listeners) {
		for (final ReliableSocketListener l : this._listeners) {
		    l.packetRetransmitted();
		}
	    }
	}
    }

    /**
     * Puts the connection in an "opened" state and notifies all
     * registered state listeners that the connection is opened.
     */
    private void connectionOpened() {
	if (isConnected()) {

	    this._nullSegmentTimer.cancel();

	    if (this._keepAlive) {
		this._keepAliveTimer.cancel();
	    }

	    synchronized (this._resetLock) {
		this._reset = false;
		this._resetLock.notify();
	    }
	} else {
	    synchronized (this) {
		try {
		    this._in = new ReliableSocketInputStream(this);
		    this._out = new ReliableSocketOutputStream(this);
		    this._connected = true;
		    this._state = ReliableSocket.ESTABLISHED;
		} catch (final IOException xcp) {
		    xcp.printStackTrace();
		}

		notify();
	    }

	    synchronized (this._stateListeners) {
		for (final ReliableSocketStateListener l : this._stateListeners) {
		    l.connectionOpened(this);
		}
	    }
	}

	this._nullSegmentTimer.schedule(0, this._profile.nullSegmentTimeout());

	if (this._keepAlive) {
	    this._keepAliveTimer.schedule(this._profile.nullSegmentTimeout() * 6,
		    this._profile.nullSegmentTimeout() * 6);
	}
    }

    /**
     * Notifies all registered state listeners that
     * the connection attempt has been refused.
     */
    private void connectionRefused() {
	synchronized (this._stateListeners) {
	    for (final ReliableSocketStateListener l : this._stateListeners) {
		l.connectionRefused(this);
	    }
	}
    }

    /**
     * Notifies all registered state listeners
     * that the connection has been closed.
     */
    private void connectionClosed() {
	synchronized (this._stateListeners) {
	    for (final ReliableSocketStateListener l : this._stateListeners) {
		l.connectionClosed(this);
	    }
	}
    }

    /**
     * Puts the connection in a closed state and notifies all
     * registered state listeners that the connection failed.
     */
    private void connectionFailure() {
	synchronized (this._closeLock) {

	    if (isClosed()) {
		return;
	    }

	    switch (this._state) {
	    case SYN_SENT:
		synchronized (this) {
		    notify();
		}
		break;
	    case CLOSE_WAIT:
	    case SYN_RCVD:
	    case ESTABLISHED:
		this._connected = false;
		synchronized (this._unackedSentQueue) {
		    this._unackedSentQueue.notifyAll();
		}

		synchronized (this._recvQueueLock) {
		    this._recvQueueLock.notify();
		}

		closeImpl();
		break;
	    }

	    this._state = ReliableSocket.CLOSED;
	    this._closed = true;
	}

	synchronized (this._stateListeners) {
	    for (final ReliableSocketStateListener l : this._stateListeners) {
		l.connectionFailure(this);
	    }
	}
    }

    /**
     * Notifies all registered state listeners
     * that the connection has been reset.
     */
    private void connectionReset() {
	synchronized (this._stateListeners) {
	    for (final ReliableSocketStateListener l : this._stateListeners) {
		l.connectionReset(this);
	    }
	}
    }

    /**
     * Handles a received SYN segment.
     * <p>
     * When a client initiates a connection it sends a SYN segment which
     * contains the negotiable parameters defined by the Upper Layer Protocol
     * via the API. The server can accept these parameters by echoing them back
     * in its SYN with ACK response or propose different parameters in its SYN
     * with ACK response. The client can then choose to accept the parameters
     * sent by the server by sending an ACK to establish the connection or it can
     * refuse the connection by sending a FIN.
     *
     * @param segment
     *            the SYN segment.
     *
     */
    private void handleSYNSegment(final SYNSegment segment) {
	try {
	    switch (this._state) {
	    case CLOSED:
		this._counters.setLastInSequence(segment.seq());
		this._state = ReliableSocket.SYN_RCVD;

		final Random rand = new Random(System.currentTimeMillis());
		this._profile = new ReliableSocketProfile(
			this._sendQueueSize,
			this._recvQueueSize,
			segment.getMaxSegmentSize(),
			segment.getMaxOutstandingSegments(),
			segment.getMaxRetransmissions(),
			segment.getMaxCumulativeAcks(),
			segment.getMaxOutOfSequence(),
			segment.getMaxAutoReset(),
			segment.getNulSegmentTimeout(),
			segment.getRetransmissionTimeout(),
			segment.getCummulativeAckTimeout());

		final Segment syn = new SYNSegment(this._counters.setSequenceNumber(rand.nextInt(ReliableSocket.MAX_SEQUENCE_NUMBER)),
			this._profile.maxOutstandingSegs(),
			this._profile.maxSegmentSize(),
			this._profile.retransmissionTimeout(),
			this._profile.cumulativeAckTimeout(),
			this._profile.nullSegmentTimeout(),
			this._profile.maxRetrans(),
			this._profile.maxCumulativeAcks(),
			this._profile.maxOutOfSequence(),
			this._profile.maxAutoReset());

		syn.setAck(segment.seq());
		sendAndQueueSegment(syn);
		break;
	    case SYN_SENT:
		this._counters.setLastInSequence(segment.seq());
		this._state = ReliableSocket.ESTABLISHED;
		/*
		 * Here the client accepts or rejects the parameters sent by the
		 * server. For now we will accept them.
		 */
		sendAck();
		connectionOpened();
		break;
	    }
	} catch (final IOException xcp) {
	    xcp.printStackTrace();
	}
    }

    /**
     * Handles a received EAK segment.
     * <p>
     * When a EAK segment is received, the segments specified in
     * the message are removed from the unacknowledged sent queue.
     * The segments to be retransmitted are determined by examining
     * the Ack Number and the last out of sequence ack number in the
     * EAK segment. All segments between but not including these two
     * sequence numbers that are on the unacknowledged sent queue are
     * retransmitted.
     *
     * @param segment
     *            the EAK segment.
     */
    private void handleEAKSegment(final EAKSegment segment) {
	Iterator<Segment> it;
	final int[] acks = segment.getACKs();

	final int lastInSequence = segment.getAck();
	final int lastOutSequence = acks[acks.length - 1];

	synchronized (this._unackedSentQueue) {

	    /* Removed acknowledged segments from sent queue */
	    for (it = this._unackedSentQueue.iterator(); it.hasNext();) {
		final Segment s = it.next();
		if ((compareSequenceNumbers(s.seq(), lastInSequence) <= 0)) {
		    it.remove();
		    continue;
		}

		for (final int ack : acks) {
		    if ((compareSequenceNumbers(s.seq(), ack) == 0)) {
			it.remove();
			break;
		    }
		}
	    }

	    /* Retransmit segments */
	    it = this._unackedSentQueue.iterator();
	    while (it.hasNext()) {
		final Segment s = it.next();
		if ((compareSequenceNumbers(lastInSequence, s.seq()) < 0) &&
			(compareSequenceNumbers(lastOutSequence, s.seq()) > 0)) {

		    try {
			retransmitSegment(s);
		    } catch (final IOException xcp) {
			xcp.printStackTrace();
		    }
		}
	    }

	    this._unackedSentQueue.notifyAll();
	}
    }

    /**
     * Handles a received RST, FIN, or DAT segment.
     *
     * @param segment
     */
    private void handleSegment(final Segment segment) {
	/*
	 * When a RST segment is received, the sender must stop
	 * sending new packets, but most continue to attempt
	 * delivery of packets already accepted from the application.
	 */
	if (segment instanceof RSTSegment) {
	    synchronized (this._resetLock) {
		this._reset = true;
	    }

	    connectionReset();
	}

	/*
	 * When a FIN segment is received, no more packets
	 * are expected to arrive after this segment.
	 */
	if (segment instanceof FINSegment) {
	    switch (this._state) {
	    case SYN_SENT:
		synchronized (this) {
		    notify();
		}
		break;
	    case CLOSED:
		break;
	    default:
		this._state = ReliableSocket.CLOSE_WAIT;
	    }
	}

	boolean inSequence = false;
	synchronized (this._recvQueueLock) {

	    if (compareSequenceNumbers(segment.seq(), this._counters.getLastInSequence()) <= 0) {
		/* Drop packet: duplicate. */
	    } else if (compareSequenceNumbers(segment.seq(), ReliableSocket.nextSequenceNumber(this._counters.getLastInSequence())) == 0) {
		inSequence = true;
		if ((this._inSeqRecvQueue.size() == 0) || ((this._inSeqRecvQueue.size() + this._outSeqRecvQueue.size()) < this._recvQueueSize)) {
		    /* Insert in-sequence segment */
		    this._counters.setLastInSequence(segment.seq());
		    if ((segment instanceof DATSegment) || (segment instanceof RSTSegment) || (segment instanceof FINSegment)) {
			this._inSeqRecvQueue.add(segment);
		    }

		    if (segment instanceof DATSegment) {
			synchronized (this._listeners) {
			    for (final ReliableSocketListener l : this._listeners) {
				l.packetReceivedInOrder();
			    }
			}
		    }

		    checkRecvQueues();
		} else {
		    /* Drop packet: queue is full. */
		}
	    } else if ((this._inSeqRecvQueue.size() + this._outSeqRecvQueue.size()) < this._recvQueueSize) {
		/* Insert out-of-sequence segment, in order */
		boolean added = false;
		for (int i = 0; (i < this._outSeqRecvQueue.size()) && !added; i++) {
		    final Segment s = this._outSeqRecvQueue.get(i);
		    final int cmp = compareSequenceNumbers(segment.seq(), s.seq());
		    if (cmp == 0) {
			/* Ignore duplicate packet */
			added = true;
		    } else if (cmp < 0) {
			this._outSeqRecvQueue.add(i, segment);
			added = true;
		    }
		}

		if (!added) {
		    this._outSeqRecvQueue.add(segment);
		}

		this._counters.incOutOfSequenceCounter();

		if (segment instanceof DATSegment) {
		    synchronized (this._listeners) {
			for (final ReliableSocketListener l : this._listeners) {
			    l.packetReceivedOutOfOrder();
			}
		    }
		}
	    }

	    if (inSequence && ((segment instanceof RSTSegment) ||
		    (segment instanceof NULSegment) ||
		    (segment instanceof FINSegment))) {
		sendAck();
	    } else if ((this._counters.getOutOfSequenceCounter() > 0) &&
		    ((this._profile.maxOutOfSequence() == 0) || (this._counters.getOutOfSequenceCounter() > this._profile.maxOutOfSequence()))) {
			sendExtendedAck();
		    } else
		if ((this._counters.getCumulativeAckCounter() > 0) &&
			((this._profile.maxCumulativeAcks() == 0) || (this._counters.getCumulativeAckCounter() > this._profile.maxCumulativeAcks()))) {
			    sendSingleAck();
			} else {
			    synchronized (this._cumulativeAckTimer) {
				if (this._cumulativeAckTimer.isIdle()) {
				    this._cumulativeAckTimer.schedule(this._profile.cumulativeAckTimeout());
				}
			    }
			}
	}
    }

    /**
     * Acknowledges the next segment to be acknowledged.
     * If there are any out-of-sequence segments in the
     * receiver queue, it sends an EAK segment.
     */
    private void sendAck() {
	synchronized (this._recvQueueLock) {
	    if (!this._outSeqRecvQueue.isEmpty()) {
		sendExtendedAck();
		return;
	    }

	    sendSingleAck();
	}
    }

    /**
     * Sends an EAK segment if there is at least one
     * out-of-sequence received segment.
     */
    private void sendExtendedAck() {
	synchronized (this._recvQueueLock) {

	    if (this._outSeqRecvQueue.isEmpty()) {
		return;
	    }

	    this._counters.getAndResetCumulativeAckCounter();
	    this._counters.getAndResetOutOfSequenceCounter();

	    /* Compose list of out-of-sequence sequence numbers */
	    final int[] acks = new int[this._outSeqRecvQueue.size()];
	    for (int i = 0; i < acks.length; i++) {
		final Segment s = this._outSeqRecvQueue.get(i);
		acks[i] = s.seq();
	    }

	    try {
		final int lastInSequence = this._counters.getLastInSequence();
		sendSegment(new EAKSegment(ReliableSocket.nextSequenceNumber(lastInSequence),
			lastInSequence, acks));
	    } catch (final IOException xcp) {
		xcp.printStackTrace();
	    }

	}
    }

    /**
     * Sends an ACK segment if there is a received segment to
     * be acknowledged.
     */
    private void sendSingleAck() {
	if (this._counters.getAndResetCumulativeAckCounter() == 0) {
	    return;
	}

	try {
	    final int lastInSequence = this._counters.getLastInSequence();
	    sendSegment(new ACKSegment(ReliableSocket.nextSequenceNumber(lastInSequence), lastInSequence));
	} catch (final IOException xcp) {
	    xcp.printStackTrace();
	}
    }

    /**
     * Sets the ACK flag and number of a segment if there is at least
     * one received segment to be acknowledged.
     *
     * @param s
     *            the segment.
     */
    private void checkAndSetAck(final Segment s) {
	if (this._counters.getAndResetCumulativeAckCounter() == 0) {
	    return;
	}

	s.setAck(this._counters.getLastInSequence());
    }

    /**
     * Checks the ACK flag and number of a segment.
     *
     * @param segment
     *            the segment.
     */
    private void checkAndGetAck(final Segment segment) {
	final int ackn = segment.getAck();

	if (ackn < 0) {
	    return;
	}

	this._counters.getAndResetOutstandingSegsCounter();

	if (this._state == ReliableSocket.SYN_RCVD) {
	    this._state = ReliableSocket.ESTABLISHED;
	    connectionOpened();
	}

	synchronized (this._unackedSentQueue) {
	    final Iterator<Segment> it = this._unackedSentQueue.iterator();
	    while (it.hasNext()) {
		final Segment s = it.next();
		if (compareSequenceNumbers(s.seq(), ackn) <= 0) {
		    it.remove();
		}
	    }

	    if (this._unackedSentQueue.isEmpty()) {
		this._retransmissionTimer.cancel();
	    }

	    this._unackedSentQueue.notifyAll();
	}
    }

    /**
     * Checks for in-sequence segments in the out-of-sequence queue
     * that can be moved to the in-sequence queue.
     */
    private void checkRecvQueues() {
	synchronized (this._recvQueueLock) {
	    final Iterator<Segment> it = this._outSeqRecvQueue.iterator();
	    while (it.hasNext()) {
		final Segment s = it.next();
		if (compareSequenceNumbers(s.seq(), ReliableSocket.nextSequenceNumber(this._counters.getLastInSequence())) == 0) {
		    this._counters.setLastInSequence(s.seq());
		    if ((s instanceof DATSegment) || (s instanceof RSTSegment) || (s instanceof FINSegment)) {
			this._inSeqRecvQueue.add(s);
		    }
		    it.remove();
		}
	    }

	    this._recvQueueLock.notify();
	}
    }

    /**
     * Writes out a segment to the underlying UDP socket.
     *
     * @param s
     *            the segment.
     * @throws IOException
     *             if an I/O error occurs in the
     *             underlying UDP socket.
     */
    protected void sendSegmentImpl(final Segment s)
	    throws IOException {
	try {
	    final DatagramPacket packet = new DatagramPacket(
		    s.getBytes(), s.length(), this._endpoint);
	    this._sock.send(packet);
	} catch (final IOException xcp) {
	    if (!isClosed()) {
		xcp.printStackTrace();
	    }
	}
    }

    /**
     * Reads in a segment from the underlying UDP socket.
     *
     * @return s the segment.
     * @throws IOException
     *             if an I/O error occurs in the
     *             underlying UDP socket.
     */
    protected Segment receiveSegmentImpl()
	    throws IOException {
	try {
	    final DatagramPacket packet = new DatagramPacket(this._recvbuffer, this._recvbuffer.length);
	    this._sock.receive(packet);
	    return Segment.parse(packet.getData(), 0, packet.getLength());
	} catch (final IOException ioXcp) {
	    if (!isClosed()) {
		ioXcp.printStackTrace();
	    }
	}

	return null;
    }

    /**
     * Closes the underlying UDP socket.
     */
    protected void closeSocket() {
	this._sock.close();
    }

    /**
     * Cleans up and closes the socket.
     */
    protected void closeImpl() {
	this._nullSegmentTimer.cancel();
	this._keepAliveTimer.cancel();
	this._state = ReliableSocket.CLOSE_WAIT;

	final Thread t = new Thread() {
	    @Override
	    public void run() {
		ReliableSocket.this._keepAliveTimer.destroy();
		ReliableSocket.this._nullSegmentTimer.destroy();

		try {
		    Thread.sleep(ReliableSocket.this._profile.nullSegmentTimeout() * 2);
		} catch (final InterruptedException xcp) {
		    xcp.printStackTrace();
		}

		ReliableSocket.this._retransmissionTimer.destroy();
		ReliableSocket.this._cumulativeAckTimer.destroy();

		closeSocket();
		connectionClosed();
	    }
	};
	t.setName("ReliableSocket-Closing");
	t.setDaemon(true);
	t.start();
    }

    /**
     * Log routine.
     */
    protected void log(final String msg) {
	System.out.println(getLocalPort() + ": " + msg);
    }

    /**
     * Computes the consecutive sequence number.
     *
     * @return the next number in the sequence.
     */
    private static int nextSequenceNumber(final int seqn) {
	return (seqn + 1) % ReliableSocket.MAX_SEQUENCE_NUMBER;
    }

    /**
     * Compares two sequence numbers.
     *
     * @return 0, 1 or -1 if the first sequence number is equal,
     *         greater or less than the second sequence number.
     *         (see RFC 1982).
     */
    private int compareSequenceNumbers(final int seqn, final int aseqn) {
	if (seqn == aseqn) {
	    return 0;
	} else if (((seqn < aseqn) && ((aseqn - seqn) > (ReliableSocket.MAX_SEQUENCE_NUMBER / 2))) ||
		((seqn > aseqn) && ((seqn - aseqn) < (ReliableSocket.MAX_SEQUENCE_NUMBER / 2)))) {
		    return 1;
		} else {
		    return -1;
		}
    }

    protected DatagramSocket _sock;
    protected SocketAddress _endpoint;
    protected ReliableSocketInputStream _in;
    protected ReliableSocketOutputStream _out;

    private final byte[] _recvbuffer = new byte[65535];

    private boolean _closed = false;
    private boolean _connected = false;
    private boolean _reset = false;
    private boolean _keepAlive = true;
    private int _state = ReliableSocket.CLOSED;
    private int _timeout = 0; /* (ms) */
    private boolean _shutIn = false;
    private boolean _shutOut = false;

    private final Object _closeLock = new Object();
    private final Object _resetLock = new Object();

    private final ConcurrentLinkedDeque<ReliableSocketListener> _listeners = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<ReliableSocketStateListener> _stateListeners = new ConcurrentLinkedDeque<>();

    private ShutdownHook _shutdownHook;

    /* RUDP connection parameters */
    private ReliableSocketProfile _profile = new ReliableSocketProfile();

    private final ArrayList<Segment> _unackedSentQueue = new ArrayList<>(); /* Unacknowledged segments send queue */
    private final ArrayList<Segment> _outSeqRecvQueue = new ArrayList<>(); /* Out-of-sequence received segments queue */
    private final ArrayList<Segment> _inSeqRecvQueue = new ArrayList<>(); /* In-sequence received segments queue */

    private final Object _recvQueueLock = new Object(); /* Lock for receiver queues */
    private final Counters _counters = new Counters(); /* Sequence number, ack counters, etc. */

    private final Thread _sockThread = new ReliableSocketThread();

    private final int _sendQueueSize = 32; /* Maximum number of received segments */
    private final int _recvQueueSize = 32; /* Maximum number of sent segments */

    private int _sendBufferSize;
    private int _recvBufferSize;

    /*
     * This timer is started when the connection is opened and is reset
     * every time a data segment is sent. If the client's null segment
     * timer expires, the client sends a null segment to the server.
     */
    private final Timer _nullSegmentTimer = new Timer("ReliableSocket-NullSegmentTimer", new NullSegmentTimerTask());

    /*
     * This timer is re-started every time a data, null, or reset
     * segment is sent and there is not a segment currently being timed.
     * If an acknowledgment for this data segment is not received by
     * the time the timer expires, all segments that have been sent but
     * not acknowledged are retransmitted. The Retransmission timer is
     * re-started when the timed segment is received, if there is still
     * one or more packets that have been sent but not acknowledged.
     */
    private final Timer _retransmissionTimer = new Timer("ReliableSocket-RetransmissionTimer", new RetransmissionTimerTask());

    /*
     * When this timer expires, if there are segments on the out-of-sequence
     * queue, an extended acknowledgment is sent. Otherwise, if there are
     * any segments currently unacknowledged, a stand-alone acknowledgment
     * is sent.
     * The cumulative acknowledge timer is restarted whenever an acknowledgment
     * is sent in a data, null, or reset segment, provided that there are no
     * segments currently on the out-of-sequence queue. If there are segments
     * on the out-of-sequence queue, the timer is not restarted, so that another
     * extended acknowledgment will be sent when it expires again.
     */
    private final Timer _cumulativeAckTimer = new Timer("ReliableSocket-CumulativeAckTimer", new CumulativeAckTimerTask());

    /*
     * When this timer expires, the connection is considered broken.
     */
    private final Timer _keepAliveTimer = new Timer("ReliableSocket-KeepAliveTimer", new KeepAliveTimerTask());

    private static final int MAX_SEQUENCE_NUMBER = 255;

    private static final int CLOSED = 0; /* There is not an active or pending connection */
    private static final int SYN_RCVD = 1; /* Request to connect received, waiting ACK */
    private static final int SYN_SENT = 2; /* Request to connect sent */
    private static final int ESTABLISHED = 3; /* Data transfer state */
    private static final int CLOSE_WAIT = 4; /* Request to close the connection */

    private static final boolean DEBUG = Boolean.getBoolean("net.rudp.debug");

    /*
     * -----------------------------------------------------------------------
     * INTERNAL CLASSES
     * -----------------------------------------------------------------------
     */

    private static class Counters {
	public Counters() {
	}

	public synchronized int nextSequenceNumber() {
	    return (this._seqn = ReliableSocket.nextSequenceNumber(this._seqn));
	}

	public synchronized int setSequenceNumber(final int n) {
	    this._seqn = n;
	    return this._seqn;
	}

	public synchronized int setLastInSequence(final int n) {
	    this._lastInSequence = n;
	    return this._lastInSequence;
	}

	public synchronized int getLastInSequence() {
	    return this._lastInSequence;
	}

	public synchronized void incCumulativeAckCounter() {
	    this._cumAckCounter++;
	}

	public synchronized int getCumulativeAckCounter() {
	    return this._cumAckCounter;
	}

	public synchronized int getAndResetCumulativeAckCounter() {
	    final int tmp = this._cumAckCounter;
	    this._cumAckCounter = 0;
	    return tmp;
	}

	public synchronized void incOutOfSequenceCounter() {
	    this._outOfSeqCounter++;
	}

	public synchronized int getOutOfSequenceCounter() {
	    return this._outOfSeqCounter;
	}

	public synchronized int getAndResetOutOfSequenceCounter() {
	    final int tmp = this._outOfSeqCounter;
	    this._outOfSeqCounter = 0;
	    return tmp;
	}

	public synchronized void incOutstandingSegsCounter() {
	    this._outSegsCounter++;
	}

	public synchronized int getOutstandingSegsCounter() {
	    return this._outSegsCounter;
	}

	public synchronized int getAndResetOutstandingSegsCounter() {
	    final int tmp = this._outSegsCounter;
	    this._outSegsCounter = 0;
	    return tmp;
	}

	public synchronized void reset() {
	    this._outOfSeqCounter = 0;
	    this._outSegsCounter = 0;
	    this._cumAckCounter = 0;
	}

	private int _seqn; /* Segment sequence number */
	private int _lastInSequence; /* Last in-sequence received segment */

	/*
	 * The receiver maintains a counter of unacknowledged segments received
	 * without an acknowledgment being sent to the transmitter. The maximum
	 * value of this counter is configurable. If this counter's maximum is
	 * exceeded, the receiver sends either a stand-alone acknowledgment, or
	 * an extended acknowledgment if there are currently any out-of-sequence
	 * segments. The recommended value for the cumulative acknowledge counter
	 * is 3.
	 */
	private int _cumAckCounter; /* Cumulative acknowledge counter */

	/*
	 * The receiver maintains a counter of the number of segments that have
	 * arrived out-of-sequence. Each time this counter exceeds its configurable
	 * maximum, an extended acknowledgment segment containing the sequence
	 * numbers of all current out-of-sequence segments that have been received
	 * is sent to the transmitter. The counter is then reset to zero. The
	 * recommended value for the out-of-sequence acknowledgments counter is 3.
	 */
	private int _outOfSeqCounter; /* Out-of-sequence acknowledgments counter */

	/*
	 * The transmitter maintains a counter of the number of segments that
	 * have been sent without getting an acknowledgment. This is used
	 * by the receiver as a mean of flow control.
	 */
	private int _outSegsCounter; /* Outstanding segments counter */
    }

    private class ReliableSocketThread extends Thread {
	public ReliableSocketThread() {
	    super("ReliableSocket");
	    setDaemon(true);
	}

	@Override
	public void run() {
	    Segment segment;
	    try {
		while ((segment = receiveSegment()) != null) {

		    if (segment instanceof SYNSegment) {
			handleSYNSegment((SYNSegment) segment);
		    } else if (segment instanceof EAKSegment) {
			handleEAKSegment((EAKSegment) segment);
		    } else if (segment instanceof ACKSegment) {
			// do nothing.
		    } else {
			handleSegment(segment);
		    }

		    checkAndGetAck(segment);
		}
	    } catch (final IOException xcp) {
		xcp.printStackTrace();
	    }
	}
    }

    private class NullSegmentTimerTask implements Runnable {
	@Override
	public void run() {
	    // Send a new NULL segment if there is nothing to be retransmitted.
	    synchronized (ReliableSocket.this._unackedSentQueue) {
		if (ReliableSocket.this._unackedSentQueue.isEmpty()) {
		    try {
			sendAndQueueSegment(new NULSegment(ReliableSocket.this._counters.nextSequenceNumber()));
		    } catch (final IOException xcp) {
			if (ReliableSocket.DEBUG) {
			    xcp.printStackTrace();
			}
		    }
		}
	    }
	}
    }

    private class RetransmissionTimerTask implements Runnable {
	@Override
	public void run() {
	    synchronized (ReliableSocket.this._unackedSentQueue) {
		for (final Segment s : ReliableSocket.this._unackedSentQueue) {
		    try {
			retransmitSegment(s);
		    } catch (final IOException xcp) {
			xcp.printStackTrace();
		    }
		}
	    }
	}
    }

    private class CumulativeAckTimerTask implements Runnable {
	@Override
	public void run() {
	    sendAck();
	}
    }

    private class KeepAliveTimerTask implements Runnable {
	@Override
	public void run() {
	    connectionFailure();
	}
    }

    private class ShutdownHook extends Thread {
	public ShutdownHook() {
	    super("ReliableSocket-ShutdownHook");
	}

	@Override
	public void run() {
	    try {
		switch (ReliableSocket.this._state) {
		case CLOSED:
		    return;
		default:
		    sendSegment(new FINSegment(ReliableSocket.this._counters.nextSequenceNumber()));
		    break;
		}
	    } catch (final Throwable t) {
		// ignore exception
	    }
	}
    }
}
