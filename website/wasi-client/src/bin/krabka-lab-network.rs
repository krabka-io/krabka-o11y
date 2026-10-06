use std::{
    ffi::OsStr,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream, UdpSocket},
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

const TIMEOUT: Duration = Duration::from_secs(2);
const TCP_MESSAGE: &str = "krabka-o11y TCP → π\n";
const FIRST_DATAGRAM: &[u8] = &[0, 255, 65, 128, 66];
const SECOND_DATAGRAM: &[u8] = &[254, 0, 127, 66];

struct NetworkReport {
    tcp_ports: [u16; 2],
    tcp_echo_bytes: usize,
    udp_port: u16,
    udp_source_port: u16,
    udp_datagrams: usize,
    udp_peek_bytes: usize,
    udp_truncated_bytes: usize,
    udp_reply_bytes: usize,
    zero_length_datagram: bool,
    udp_read_timed_out: bool,
    async_echo_bytes: usize,
    async_timer_ticks: usize,
    parked_lock_value: usize,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("network self-test failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> io::Result<()> {
    let mut args = std::env::args_os().skip(1);
    let mode = args.next();
    if mode.as_deref() == Some(OsStr::new("--deadline-test")) && args.next().is_none() {
        let state = Arc::new((parking_lot::Mutex::new(()), parking_lot::Condvar::new()));
        let child_state = Arc::clone(&state);
        let (prepared, ready) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || -> io::Result<()> {
            let mut held = child_state.0.lock();
            prepared.send(()).map_err(io::Error::other)?;
            loop {
                child_state.1.wait(&mut held);
            }
        });
        ready.recv_timeout(TIMEOUT).map_err(io::Error::other)?;
        // This guard can be acquired only after the child has queued its
        // native atomic waiter. Shutdown must terminate that waiting worker.
        let held = state
            .0
            .try_lock_for(TIMEOUT)
            .ok_or_else(|| io::Error::other("deadline child did not enter its wait"))?;
        println!("deadline-ready");
        io::stdout().flush()?;
        let result = worker
            .join()
            .map_err(|_| io::Error::other("deadline worker panicked"))?;
        drop(held);
        return result;
    }
    ensure(
        mode.as_deref() == Some(OsStr::new("--self-test")) && args.next().is_none(),
        "usage: krabka-lab-network --self-test | --deadline-test",
    )?;
    let report = self_test()?;
    writeln!(
        io::stdout().lock(),
        concat!(
            "{{\"tcpPorts\":[{},{}],\"tcpEchoBytes\":{},",
            "\"udpPort\":{},\"udpSourcePort\":{},\"udpDatagrams\":{},",
            "\"udpPeekBytes\":{},\"udpTruncatedBytes\":{},\"udpReplyBytes\":{},",
            "\"zeroLengthDatagram\":{},\"udpReadTimedOut\":{},",
            "\"asyncEchoBytes\":{},\"asyncTimerTicks\":{},",
            "\"parkedLockValue\":{}}}",
        ),
        report.tcp_ports[0],
        report.tcp_ports[1],
        report.tcp_echo_bytes,
        report.udp_port,
        report.udp_source_port,
        report.udp_datagrams,
        report.udp_peek_bytes,
        report.udp_truncated_bytes,
        report.udp_reply_bytes,
        report.zero_length_datagram,
        report.udp_read_timed_out,
        report.async_echo_bytes,
        report.async_timer_ticks,
        report.parked_lock_value,
    )
}

fn ensure(condition: bool, message: &str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message))
    }
}

fn at<T>(operation: &str, result: io::Result<T>) -> io::Result<T> {
    result.map_err(|error| io::Error::new(error.kind(), format!("{operation}: {error}")))
}

fn accept_with_timeout(listener: &TcpListener) -> io::Result<TcpStream> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        match listener.accept() {
            Ok((stream, _)) => return Ok(stream),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "TCP accept timed out",
                    ));
                }
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error),
        }
    }
}

fn tcp_round_trips(listeners: [TcpListener; 2], addresses: [SocketAddr; 2]) -> io::Result<usize> {
    let server = thread::spawn(move || -> io::Result<usize> {
        let mut echoed = 0;
        for listener in listeners {
            let mut stream = accept_with_timeout(&listener)?;
            stream.set_read_timeout(Some(TIMEOUT))?;
            stream.set_write_timeout(Some(TIMEOUT))?;
            let mut message = [0; TCP_MESSAGE.len()];
            stream.read_exact(&mut message)?;
            ensure(
                message == TCP_MESSAGE.as_bytes(),
                "TCP server received different bytes",
            )?;
            stream.write_all(&message)?;
            echoed += message.len();
        }
        Ok(echoed)
    });
    let client = (|| -> io::Result<usize> {
        let mut echoed = 0;
        for address in addresses {
            let mut stream = TcpStream::connect_timeout(&address, TIMEOUT)?;
            stream.set_read_timeout(Some(TIMEOUT))?;
            stream.set_write_timeout(Some(TIMEOUT))?;
            stream.write_all(TCP_MESSAGE.as_bytes())?;
            let mut message = [0; TCP_MESSAGE.len()];
            stream.read_exact(&mut message)?;
            let reply = std::str::from_utf8(&message)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            ensure(
                reply == TCP_MESSAGE,
                "TCP client received a different UTF-8 reply",
            )?;
            echoed += message.len();
        }
        Ok(echoed)
    })();
    let server = server
        .join()
        .map_err(|_| io::Error::other("TCP worker panicked"))?;
    let echoed = client?;
    ensure(
        server? == echoed,
        "TCP worker and client byte counts differ",
    )?;
    Ok(echoed)
}

fn empty_async_read(
    operation: &str,
    read: impl FnOnce(&mut [u8]) -> io::Result<usize>,
) -> io::Result<()> {
    let started = Instant::now();
    let result = read(&mut [0; 1]);
    let elapsed = started.elapsed();
    ensure(
        elapsed < TIMEOUT,
        &format!("{operation}: empty try_read blocked for {elapsed:?}: {result:?}"),
    )?;
    ensure(
        matches!(result, Err(ref error) if error.kind() == io::ErrorKind::WouldBlock),
        &format!("{operation}: empty try_read must return WouldBlock: {result:?}"),
    )
}

async fn read_async_frame(reader: &mut OwnedReadHalf) -> io::Result<usize> {
    let length = reader.read_u32().await?;
    ensure(
        usize::try_from(length).map_err(io::Error::other)? == TCP_MESSAGE.len(),
        "async TCP frame length changed",
    )?;
    let mut message = [0; TCP_MESSAGE.len()];
    reader.read_exact(&mut message).await?;
    ensure(
        message == TCP_MESSAGE.as_bytes(),
        "async TCP frame bytes changed",
    )?;
    // The completed read leaves cached readiness; the now-empty socket must not block.
    empty_async_read("async TCP after frame", |buffer| reader.try_read(buffer))?;
    Ok(message.len())
}

async fn write_async_frame(writer: &mut OwnedWriteHalf) -> io::Result<()> {
    let length = u32::try_from(TCP_MESSAGE.len()).map_err(io::Error::other)?;
    writer.write_u32(length).await?;
    writer.write_all(TCP_MESSAGE.as_bytes()).await?;
    writer.flush().await
}

async fn async_tcp_round_trips() -> io::Result<(usize, usize)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (accepted, ready) = tokio::sync::oneshot::channel();
    let (finished, done) = tokio::sync::oneshot::channel();
    let ticks = Arc::new(AtomicUsize::new(0));
    let timer_ticks = Arc::clone(&ticks);
    let timer = tokio::spawn(async move {
        for _ in 0..4 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            timer_ticks.fetch_add(1, Ordering::Relaxed);
        }
    });
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        empty_async_read("async accepted TCP", |buffer| stream.try_read(buffer))?;
        let (mut reader, mut writer) = stream.into_split();
        accepted
            .send(())
            .map_err(|_| io::Error::other("async TCP client dropped before acceptance"))?;
        let mut echoed = 0;
        for _ in 0..2 {
            echoed += read_async_frame(&mut reader).await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
            write_async_frame(&mut writer).await?;
        }
        done.await
            .map_err(|_| io::Error::other("async TCP client dropped before final empty read"))?;
        Ok::<_, io::Error>(echoed)
    });
    // Use TcpStream::connect itself: TcpSocket has a different Mio nonblocking path.
    let stream = tokio::net::TcpStream::connect(address).await?;
    empty_async_read("async outbound TCP", |buffer| stream.try_read(buffer))?;
    let (mut reader, mut writer) = stream.into_split();
    ready
        .await
        .map_err(|_| io::Error::other("async TCP server failed before acceptance"))?;
    let mut echoed = 0;
    for _ in 0..2 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        write_async_frame(&mut writer).await?;
        echoed += read_async_frame(&mut reader).await?;
    }
    let timer_ticks = ticks.load(Ordering::Relaxed);
    ensure(
        timer_ticks >= 2,
        "short async timer did not progress during the TCP exchanges",
    )?;
    finished
        .send(())
        .map_err(|_| io::Error::other("async TCP server dropped before client finished"))?;
    ensure(
        server.await.map_err(io::Error::other)?? == echoed,
        "async TCP worker and client byte counts differ",
    )?;
    timer.await.map_err(io::Error::other)?;
    Ok((echoed, timer_ticks))
}

fn check_async_tcp() -> io::Result<(usize, usize)> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        tokio::time::timeout(TIMEOUT, async_tcp_round_trips())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "async TCP self-test timed out"))?
    });
    runtime.shutdown_timeout(TIMEOUT);
    result
}

fn check_parked_lock() -> io::Result<usize> {
    let value = Arc::new((
        parking_lot::Mutex::new((false, 0)),
        parking_lot::Condvar::new(),
    ));
    let held = value.0.lock();
    let (timed_out, observed) = std::sync::mpsc::channel();
    let (waiting, prepared) = std::sync::mpsc::channel();
    let (completed, finished) = std::sync::mpsc::channel();
    let worker_value = Arc::clone(&value);
    let worker_completed = completed.clone();
    let worker = thread::spawn(move || -> io::Result<()> {
        let started = Instant::now();
        ensure(
            worker_value
                .0
                .try_lock_for(Duration::from_millis(30))
                .is_none(),
            "contended parking_lot mutex did not time out",
        )?;
        ensure(
            started.elapsed() >= Duration::from_millis(20) && started.elapsed() < TIMEOUT,
            "parking_lot mutex deadline did not advance",
        )?;
        timed_out.send(()).map_err(io::Error::other)?;
        let mut held = worker_value.0.lock();
        waiting.send(()).map_err(io::Error::other)?;
        while !held.0 {
            worker_value.1.wait(&mut held);
        }
        held.1 += 1;
        worker_completed.send(()).map_err(io::Error::other)?;
        Ok(())
    });
    observed.recv_timeout(TIMEOUT).map_err(io::Error::other)?;
    drop(held);
    prepared.recv_timeout(TIMEOUT).map_err(io::Error::other)?;
    {
        // Acquiring this lock after the marker requires Condvar to queue the
        // waiter and release its guard; notify_one must find that queued waiter.
        let mut held = value
            .0
            .try_lock_for(TIMEOUT)
            .ok_or_else(|| io::Error::other("parking_lot waiter did not release its mutex"))?;
        held.0 = true;
        ensure(
            value.1.notify_one(),
            "parking_lot condition waiter was not queued",
        )?;
    }
    finished.recv_timeout(TIMEOUT).map_err(io::Error::other)?;
    worker
        .join()
        .map_err(|_| io::Error::other("parking_lot worker panicked"))??;

    let workers: Vec<_> = (0..4)
        .map(|_| {
            let value = Arc::clone(&value);
            let completed = completed.clone();
            thread::spawn(move || {
                for _ in 0..100 {
                    value.0.lock().1 += 1;
                    thread::yield_now();
                }
                completed.send(()).map_err(io::Error::other)
            })
        })
        .collect();
    for _ in &workers {
        finished.recv_timeout(TIMEOUT).map_err(io::Error::other)?;
    }
    for worker in workers {
        worker
            .join()
            .map_err(|_| io::Error::other("parking_lot contender panicked"))??;
    }
    let result = value.0.lock().1;
    ensure(result == 401, "parking_lot mutex lost a concurrent update")?;
    Ok(result)
}

fn self_test() -> io::Result<NetworkReport> {
    let parked_lock_value = at("parking_lot contention", check_parked_lock())?;
    let first = at("first TCP bind", TcpListener::bind("127.0.0.1:0"))?;
    let second = at("second TCP bind", TcpListener::bind("127.0.0.1:0"))?;
    let addresses = [
        at("first TCP local_addr", first.local_addr())?,
        at("second TCP local_addr", second.local_addr())?,
    ];
    ensure(
        addresses.iter().all(|address| address.port() != 0) && addresses[0] != addresses[1],
        &format!("TCP ephemeral listeners must have distinct nonzero addresses: {addresses:?}"),
    )?;
    at("first TCP nonblocking", first.set_nonblocking(true))?;
    at("second TCP nonblocking", second.set_nonblocking(true))?;

    // Bind while both TCP listeners are alive to check separate protocol namespaces.
    let receiver = at("UDP receiver bind", UdpSocket::bind(addresses[0]))?;
    let source = at("UDP source bind", UdpSocket::bind("127.0.0.1:0"))?;
    let receiver_address = at("UDP receiver local_addr", receiver.local_addr())?;
    let source_address = at("UDP source local_addr", source.local_addr())?;
    ensure(
        receiver_address == addresses[0],
        "UDP bind changed the requested TCP port",
    )?;
    ensure(
        source_address.port() != 0,
        "UDP ephemeral source still has port zero",
    )?;
    at(
        "UDP receiver read timeout",
        receiver.set_read_timeout(Some(TIMEOUT)),
    )?;
    at(
        "UDP receiver write timeout",
        receiver.set_write_timeout(Some(TIMEOUT)),
    )?;
    at(
        "UDP source read timeout",
        source.set_read_timeout(Some(TIMEOUT)),
    )?;
    at(
        "UDP source write timeout",
        source.set_write_timeout(Some(TIMEOUT)),
    )?;
    for socket in [&receiver, &source] {
        ensure(
            at("UDP read_timeout", socket.read_timeout())? == Some(TIMEOUT)
                && at("UDP write_timeout", socket.write_timeout())? == Some(TIMEOUT),
            "UDP timeout getters differ from the configured deadlines",
        )?;
    }

    let tcp_echo_bytes = at(
        "TCP threaded round trips",
        tcp_round_trips([first, second], addresses),
    )?;
    for packet in [FIRST_DATAGRAM, SECOND_DATAGRAM, &[]] {
        ensure(
            source.send_to(packet, receiver_address)? == packet.len(),
            "UDP send returned a different byte count",
        )?;
    }

    let mut buffer = [0; 16];
    let (udp_peek_bytes, peer) = receiver.peek_from(&mut buffer)?;
    ensure(
        peer == source_address && buffer[..udp_peek_bytes] == *FIRST_DATAGRAM,
        "UDP peek changed packet bytes or the source address",
    )?;
    let mut short = [0; 2];
    let (udp_truncated_bytes, peer) = receiver.recv_from(&mut short)?;
    let mut udp_datagrams = 1;
    ensure(
        peer == source_address
            && udp_truncated_bytes == short.len()
            && short == FIRST_DATAGRAM[..short.len()],
        "UDP short receive did not truncate exactly one datagram",
    )?;
    let (received, peer) = receiver.recv_from(&mut buffer)?;
    udp_datagrams += 1;
    ensure(
        peer == source_address && buffer[..received] == *SECOND_DATAGRAM,
        "UDP short receive leaked a tail into the next datagram",
    )?;
    let reply = buffer[..received].to_vec();
    let (received, peer) = receiver.recv_from(&mut buffer)?;
    udp_datagrams += 1;
    let zero_length_datagram = received == 0 && peer == source_address;
    ensure(
        zero_length_datagram,
        "UDP zero-length datagram was not received",
    )?;

    ensure(
        receiver.send_to(&reply, source_address)? == reply.len(),
        "UDP reply send returned a different byte count",
    )?;
    let (udp_reply_bytes, peer) = source.recv_from(&mut buffer)?;
    ensure(
        peer == receiver_address && buffer[..udp_reply_bytes] == reply,
        "UDP reply bytes or source address changed",
    )?;

    let read_timeout = Duration::from_millis(100);
    receiver.set_read_timeout(Some(read_timeout))?;
    let started = Instant::now();
    let timeout = match receiver.recv_from(&mut buffer) {
        Err(error) => error,
        Ok(_) => return Err(io::Error::other("empty UDP receive did not time out")),
    };
    let elapsed = started.elapsed();
    let udp_read_timed_out = matches!(
        timeout.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    ) && elapsed >= Duration::from_millis(70)
        && elapsed < Duration::from_secs(10);
    ensure(
        udp_read_timed_out,
        &format!("UDP deadline failed: {timeout}, elapsed {elapsed:?}"),
    )?;
    receiver.set_read_timeout(None)?;
    receiver.set_write_timeout(None)?;
    ensure(
        receiver.read_timeout()?.is_none() && receiver.write_timeout()?.is_none(),
        "UDP deadlines remained after clearing the options",
    )?;
    let (async_echo_bytes, async_timer_ticks) = at("async TCP round trips", check_async_tcp())?;

    Ok(NetworkReport {
        tcp_ports: [addresses[0].port(), addresses[1].port()],
        tcp_echo_bytes,
        udp_port: receiver_address.port(),
        udp_source_port: source_address.port(),
        udp_datagrams,
        udp_peek_bytes,
        udp_truncated_bytes,
        udp_reply_bytes,
        zero_length_datagram,
        udp_read_timed_out,
        async_echo_bytes,
        async_timer_ticks,
        parked_lock_value,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn checks_real_tcp_udp_and_async_tcp_socket_operations() -> io::Result<()> {
        let report = self_test()?;
        assert!(report.udp_port == report.tcp_ports[0]);
        assert!(report.udp_datagrams == 3);
        assert!(report.zero_length_datagram);
        assert!(report.udp_read_timed_out);
        assert!(report.async_echo_bytes == 2 * TCP_MESSAGE.len());
        assert!(report.async_timer_ticks >= 2);
        assert!(report.parked_lock_value == 401);
        Ok(())
    }
}
