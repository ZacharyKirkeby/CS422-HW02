"""
AI was involved with making this for research and some implementation purposes. Specifically with python-specific socket stuff because I'm not the most familiar with python syntaxes.
"""

import json
import random
import select
import socket
import struct
import time

# Control states (source: iperf_api.h)
TEST_START = 1
TEST_RUNNING = 2
TEST_END = 4
PARAM_EXCHANGE = 9
CREATE_STREAMS = 10
SERVER_TERMINATE = 11
CLIENT_TERMINATE = 12
EXCHANGE_RESULTS = 13
DISPLAY_RESULTS = 14
IPERF_DONE = 16
ACCESS_DENIED = -1
SERVER_ERROR = -2

COOKIE_SIZE = 37
COOKIE_CHARS = "abcdefghijklmnopqrstuvwxyz234567"

# Exceptions

class Iperf3Exception(Exception):
	pass

class Iperf3ServerBusyException(Iperf3Exception):
	pass

# Socket helpers

def make_cookie():
	cookie = ""
	for i in range(COOKIE_SIZE - 1):
		cookie += random.choice(COOKIE_CHARS)
	return cookie.encode() + b"\x00"

def connect(sock, host, port, what):
	# connect() but we remap socket errors to Iperf3Exception
	try:
		sock.connect((host, port))
	except OSError as e:
		raise Iperf3Exception(f"Failed to connect {what} to {host}:{port}: {e}")

def send_all(sock, data, what):
	# sendall() but we remap socket errors to Iperf3Exception
	try:
		sock.sendall(data)
	except socket.timeout:
		raise Iperf3Exception(f"Timeout while sending to {what}")
	except OSError as e:
		raise Iperf3Exception(f"Connection closed while sending to {what}: {e}")

def recv_exact(sock, n, what):
	# read n bytes and raise Iperf3Exception on EOF or timeout
	data = b""
	while len(data) < n:
		try:
			chunk = sock.recv(n - len(data))
		except socket.timeout:
			raise Iperf3Exception(f"Timeout while receiving from {what}")
		except OSError as e:
			raise Iperf3Exception(f"Connection closed while receiving from {what}: {e}")
		if chunk == b"":
			raise Iperf3Exception(f"Connection closed by server while receiving from {what}")
		data += chunk
	return data

def send_state(sock, state):
	# Send state to server
	send_all(sock, struct.pack("!b", state), "control socket")

def read_state(sock):
	# Read server state
	return struct.unpack("!b", recv_exact(sock, 1, "control socket"))[0]

def read_server_error(sock):
	# read 2 4-byte ints after SERVER_ERROR and reconstruct exception
	code, errno = struct.unpack("!ii", recv_exact(sock, 8, "control socket"))
	return Iperf3Exception(f"Server error: iperf3 error code {code}, errno {errno}")

def expect_state(sock, expected):
	# read server state and raise an exception unless same as expected
	# ACCESS_DENIED -> Iperf3ServerBusyException, anything else -> Iperf3Exception.
	state = read_state(sock)
	if state == expected:
		return
	if state == ACCESS_DENIED:
		raise Iperf3ServerBusyException("Server is busy")
	elif state == SERVER_ERROR:
		raise read_server_error(sock)
	elif state == SERVER_TERMINATE:
		raise Iperf3Exception("Connection closed by server")
	else:
		raise Iperf3Exception(f"Unexpected/unknown state {state}, expected {expected}")

def send_json(sock, obj):
	# sends 4-byte BE length and then json
	data = json.dumps(obj).encode()
	send_all(sock, struct.pack("!I", len(data)) + data, "control socket")

def recv_json(sock):
	# read length-prefixed json from server.
	# Raises Iperf3Exception if can't parse.
	length = struct.unpack("!I", recv_exact(sock, 4, "control socket"))[0]
	data = recv_exact(sock, length, "control socket")
	try:
		return json.loads(data)
	except ValueError as e:
		raise Iperf3Exception(f"Failed to parse JSON: {e}")

# Client

class Iperf3Client:
	# A client for running iperf3 tests
	"""_summary_
		An iperf3 client that can connect to a server and run tests.
		Arguments:
			control_sock: The control socket to use for communication with the server.
			data_sock: The data socket to use for sending/receiving data.
			host: The hostname or IP address of the server.
			port: The port number of the server (default is 5201).
			duration: The duration of the test in seconds (default is 60).
			block_size: The size of each data block in bytes (default is 128 * 1024).
			timeout: The timeout for socket operations in seconds (default is 10.0).
	"""
	def __init__(self, control_sock, data_sock, host, port=5201, duration=60, block_size=128 * 1024, timeout=10.0):
		self.control_sock = control_sock
		self.data_sock = data_sock
		self.host = host
		self.port = port
		self.duration = duration
		self.block_size = block_size
		self.cookie = make_cookie()
		self.bytes_sent = 0
		self.start_time = None
		self.end_time = None
		self.elapsed = 0.0
		self.control_connected = False
		self.control_sock.settimeout(timeout)
		self.data_sock.settimeout(timeout)

	def run(self):
		try:
			self.open_control()
			self.exchange_params()
			self.open_data()
			self.send_data()
			# returns server results json fom self.finish()
			return self.finish()
		except Iperf3Exception:
			# if iperf3 exception happens, call abort() and re-raise.
			self.abort()
			raise
		except OSError as e:
			self.abort()
			raise Iperf3Exception(f"Socket error: {e}")
		finally:
			self.close()

	def open_control(self):
		connect(self.control_sock, self.host, self.port, "control socket")
		self.control_connected = True
		send_all(self.control_sock, self.cookie, "control socket")
		expect_state(self.control_sock, PARAM_EXCHANGE)

	def exchange_params(self):
		# send test params and expect CREATE_STREAMS
		params = {
			"tcp": True,
			"omit": 0,
			"time": self.duration,
			"num": 0,
			"blockcount": 0,
			"parallel": 1,
			"len": self.block_size,
			"pacing_timer": 1000,
			"client_version": "3.17",
		}
		send_json(self.control_sock, params)
		expect_state(self.control_sock, CREATE_STREAMS)

	def open_data(self):
		connect(self.data_sock, self.host, self.port, "data socket")
		send_all(self.data_sock, self.cookie, "data socket")
		# expect two states in sequence
		expect_state(self.control_sock, TEST_START)
		expect_state(self.control_sock, TEST_RUNNING)

	def send_data(self):
		# send the buffer until test done
		buf = b"x" * self.block_size
		self.start_time = time.monotonic()
		while time.monotonic() - self.start_time < self.duration:
			self.check_control()
			send_all(self.data_sock, buf, "data socket")
			self.bytes_sent += len(buf)
		self.end_time = time.monotonic()
		self.elapsed = self.end_time - self.start_time

	def check_control(self):
		# polls control socket with select(timeout=0) and reads state if readable
		ready, _, _ = select.select([self.control_sock], [], [], 0)
		if ready:
			state = read_state(self.control_sock)
			if state == SERVER_ERROR:
				raise read_server_error(self.control_sock)
			if state == SERVER_TERMINATE:
				raise Iperf3Exception("Server terminated the test")
			raise Iperf3Exception(f"Unexpected state {state} during test")

	def finish(self):
		send_state(self.control_sock, TEST_END)
		expect_state(self.control_sock, EXCHANGE_RESULTS)
		client_results = {
			"cpu_util_total": 0,
			"cpu_util_user": 0,
			"cpu_util_system": 0,
			"sender_has_retransmits": 0,
			"streams": [{
				"id": 1,
				"bytes": self.bytes_sent,
				"retransmits": -1,
				"jitter": 0,
				"errors": 0,
				"packets": 0,
				"start_time": 0,
				"end_time": self.elapsed,
			}],
		}
		send_json(self.control_sock, client_results)
		server_results = recv_json(self.control_sock)
		expect_state(self.control_sock, DISPLAY_RESULTS)
		send_state(self.control_sock, IPERF_DONE)
		return server_results

	# cleanup
	def abort(self):
		# Tries to send CLIENT_TERMINATE if the control socket is connected, ignores errors
		if self.control_connected:
			try:
				send_state(self.control_sock, CLIENT_TERMINATE)
			except Iperf3Exception:
				pass

	def close(self):
		self.control_sock.close()
		self.data_sock.close()
