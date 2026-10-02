import csv
import json
import os
import random
import statistics
import struct
import sys
import threading
import time
import iperf3jank
import socket
import argparse
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

parser = argparse.ArgumentParser(description="Iperf3 client")
parser.add_argument("--f", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "listed_iperf3_servers.json"), help="File of iperf servers in json format")
parser.add_argument("--duration", type=int, default=60, help="Test duration in seconds")
parser.add_argument("--block-size", type=int, default=128 * 1024, help="Block size in bytes")
parser.add_argument("--timeout", type=float, default=10.0, help="Socket timeout in seconds")
parser.add_argument("--num-tests", type=int, default=10, help="Number of tests to run")
parser.add_argument("--interval", type=float, default=0.2, help="Goodput and TCP stats sampling interval in seconds")
parser.add_argument("--algorithms", default="", help="Comma-separated TCP congestion control algorithms to test, e.g. cubic,bbr (default: system default)")
parser.add_argument("--out-dir", default="results", help="Directory for the TCP stats log and summary table")

args = parser.parse_args()

# socket option numbers from linux/tcp.h
TCP_INFO = 11
TCP_CONGESTION = 13

# Linux struct tcp_info (include/uapi/linux/tcp.h)
# ngl used ai for this because I had no idea how to get a c struct into python
# "=" - native byte order and no padding
# 8B - 8 1 byte values
# 24I - 24 integers (4 bytes each)
# 4Q - 4 quadwords (8 bytes each)
# 6I - 6 integers (4 bytes each)
# 4Q - 4 quadwords (8 bytes each)
# 2I - 2 integers (4 bytes each)
# 2Q - 2 quadwords (8 bytes each)
# 2I - 2 integers (4 bytes each)
TCP_INFO_FMT = "=8B24I4Q6I4Q2I2Q2I"
# expected field names in the TCP_INFO structure
TCP_INFO_FIELDS = [
	"state", "ca_state", "retransmits", "probes", "backoff", "options", "wscale", "delivery_flags",
	"rto", "ato", "snd_mss", "rcv_mss", "unacked", "sacked", "lost", "retrans", "fackets",
	"last_data_sent", "last_ack_sent", "last_data_recv", "last_ack_recv",
	"pmtu", "rcv_ssthresh", "rtt", "rttvar", "snd_ssthresh", "snd_cwnd", "advmss", "reordering",
	"rcv_rtt", "rcv_space", "total_retrans",
	"pacing_rate", "max_pacing_rate", "bytes_acked", "bytes_received",
	"segs_out", "segs_in", "notsent_bytes", "min_rtt", "data_segs_in", "data_segs_out",
	"delivery_rate", "busy_time", "rwnd_limited", "sndbuf_limited",
	"delivered", "delivered_ce",
	"bytes_sent", "bytes_retrans",
	"dsack_dups", "reord_seen"
]
TCP_INFO_SIZE = struct.calcsize(TCP_INFO_FMT)

# columns used in csv per sample log
CSV_COLUMNS = [
	"run_id", "destination", "algorithm", "sample",
	"wall_time_unix_s", "elapsed_s", "interval_s", "goodput_bps",
	"snd_cwnd_pkts", "snd_mss_bytes", "srtt_us", "rttvar_us",
	"lost_pkts", "retrans_pkts", "total_retrans_pkts", "interval_retrans_pkts", "interval_retrans_ratio", "ca_state",
	"pacing_rate_bps", "delivery_rate_bps", "bytes_acked", "bytes_sent", "bytes_retrans",
]

def load_destinations(path):
	with open(path, "r") as f:
		destinations = json.load(f)
	print(f"Number of entries: {len(destinations)}")
	return destinations

# some entries in json list from iperf servers have a range of ports instead of just one port
def get_first_port(port):
	if isinstance(port, str) and "-" in port:
		return int(port.split("-")[0])
	return int(port)

# TCP stats checking

def read_tcp_info(sock):
	raw = sock.getsockopt(socket.IPPROTO_TCP, TCP_INFO, TCP_INFO_SIZE)
	if len(raw) < TCP_INFO_SIZE:
		raise OSError("TCP_INFO is too short, need kernel 4.19+") # apparenlty kernel can be too old that doesn't include our required fields
	values = struct.unpack(TCP_INFO_FMT, raw) # unpack raw data into a tuple of values
	info = {}
	# for each field in the TCP_INFO structure, assign the corresponding value
	for i in range(len(TCP_INFO_FIELDS)):
		info[TCP_INFO_FIELDS[i]] = values[i]
	return info

def sample_tcp_stats(client, samples, stop):
	# runs in separate thread and queries TCP_INFO from data socket while data is being sent
	while client.start_time is None:
		if stop.is_set():
			return
		time.sleep(0.01)
	next_time = time.monotonic()
	while not stop.is_set():
		try:
			info = read_tcp_info(client.data_sock)
		except OSError:
			return  # socket closed after an error
		samples.append([time.time(), time.monotonic() - client.start_time, info])
		if client.end_time is not None:
			return  # done sending last sample
		next_time += args.interval
		time.sleep(max(0, next_time - time.monotonic()))

def make_rows(run_id, destination, algorithm, samples):
	# one csv row per sample
	rows = []
	for i in range(len(samples)):
		wall, elapsed, info = samples[i]
		row = {
			"run_id": run_id, # nth run
			"destination": destination, # the destination of the test
			"algorithm": algorithm, # the congestion control algorithm used
			"sample": i, # the sample number
			"wall_time_unix_s": round(wall, 4), # wall clock time in seconds
			"elapsed_s": round(elapsed, 4), # elapsed time in seconds
			"interval_s": "", # interval duration in seconds
			"goodput_bps": "", # goodput in bits per second
			"snd_cwnd_pkts": info["snd_cwnd"], # congestion window in packets
			"snd_mss_bytes": info["snd_mss"], # send maximum segment size in bytes
			"srtt_us": info["rtt"], # smoothed round-trip time in microseconds
			"rttvar_us": info["rttvar"], # round-trip time variation in microseconds
			"lost_pkts": info["lost"], # num packets tcp thinks are lost at this time (not cumulative)
			"retrans_pkts": info["retrans"], # num packets retransmitted without ack this time (not cumulative)
			"total_retrans_pkts": info["total_retrans"], # total retransmitted packets
			"interval_retrans_pkts": "", # retransmitted packets in this interval
			"interval_retrans_ratio": "", # retransmission ratio in this interval
			"ca_state": info["ca_state"], # congestion control state
			"pacing_rate_bps": info["pacing_rate"] * 8, # pacing rate in bits per second
			"delivery_rate_bps": info["delivery_rate"] * 8, # delivery rate in bits per second
			"bytes_acked": info["bytes_acked"], # bytes acknowledged
			"bytes_sent": info["bytes_sent"], # bytes sent
			"bytes_retrans": info["bytes_retrans"], # bytes retransmitted
		}
		if i > 0:
			# calculate interval-based metrics
			prev_elapsed = samples[i - 1][1] # previous elapsed time
			prev = samples[i - 1][2] # previous TCP info
			dt = elapsed - prev_elapsed # time elapsed since last sample
			sent = info["bytes_sent"] - prev["bytes_sent"] # bytes sent in this interval
			retrans_bytes = info["bytes_retrans"] - prev["bytes_retrans"] # bytes retransmitted in this interval
			row["interval_s"] = round(dt, 4) # interval duration in seconds
			# calculate goodput
			if dt > 0:
				row["goodput_bps"] = round((info["bytes_acked"] - prev["bytes_acked"]) * 8 / dt)
			row["interval_retrans_pkts"] = info["total_retrans"] - prev["total_retrans"] # retransmitted packets in this interval
			# retransmission ratio in this interval
			if sent > 0:
				row["interval_retrans_ratio"] = round(retrans_bytes / sent, 6)
			else:
				row["interval_retrans_ratio"] = 0.0
		rows.append(row)
	return rows

def check_algorithms(algorithms):
	# try each algorithm to make sure it exists
	test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	for algorithm in algorithms:
		try:
			test_sock.setsockopt(socket.IPPROTO_TCP, TCP_CONGESTION, algorithm.encode())
		except OSError as e:
			available = "unknown"
			try:
				with open("/proc/sys/net/ipv4/tcp_available_congestion_control") as f:
					available = f.read().strip()
			except OSError:
				pass
			test_sock.close()
			sys.exit(f"[x] Congestion control algo '{algorithm}' not available: {e}. Available: {available}")
	test_sock.close()

# run tests

def run_one(dest, algorithm, run_id):
	host = dest["IP/HOST"]
	port = get_first_port(dest["PORT"])
	control = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	# select congestion control algorithm
	if algorithm:
		data.setsockopt(socket.IPPROTO_TCP, TCP_CONGESTION, algorithm.encode())
	# read algorithm used by kernel
	used_algorithm = data.getsockopt(socket.IPPROTO_TCP, TCP_CONGESTION, 16).split(b"\0")[0].decode()

	print(f"[*] Running test {run_id} on {dest['IP/HOST']}:{get_first_port(dest['PORT'])} with algorithm {used_algorithm}")
	# setup iperf3 client
	iperfclient = iperf3jank.Iperf3Client(
		control_sock=control,
		data_sock=data,
		host=host,
		port=port,
		duration=args.duration,
		block_size=args.block_size,
		timeout=args.timeout,
	)
	samples = []
	# use threads to get TCP stats while test running
	stop = threading.Event()
	sampler = threading.Thread(target=sample_tcp_stats, args=(iperfclient, samples, stop))
	sampler.start()
	try:
		iperfclient.run()
	finally:
		stop.set()
		sampler.join()

	# process and collate for output
	destination = f"{host}:{port} ({dest['SITE']}, {dest['COUNTRY']})"
	mbps = iperfclient.bytes_sent * 8 / args.duration / 1e6
	run = {
		"run_id": run_id,
		"destination": destination,
		"algorithm": used_algorithm,
		"avg_send_rate_mbps": mbps,
		"rows": make_rows(run_id, destination, used_algorithm, samples),
	}
	return run

def run_tests(destinations, algorithms):
	runs = []
	num_completed = 0
	for i in range(len(destinations)): # whole list
		completed_all = True
		dest_runs = [] # temp list to add to main list if all algos succeed
		for algorithm in algorithms:
			run_id = len(runs) + len(dest_runs) + 1
			# for each algo run a test
			try:
				run = run_one(destinations[i], algorithm, run_id)
				dest_runs.append(run)
				print(f"[+] {run_id} {run['destination']} [{run['algorithm']}]: {run['avg_send_rate_mbps']:.1f} Mbps average, {len(run['rows'])} TCP samples")
			# stop trying this server and move on if exceptions happen
			except iperf3jank.Iperf3ServerBusyException:
				print(f"[x] Server {destinations[i]['IP/HOST']}:{get_first_port(destinations[i]['PORT'])} is busy. Retrying another server...")
				completed_all = False
				break
			except iperf3jank.Iperf3Exception as e:
				print(f"[x] Error occurred while testing {destinations[i]['IP/HOST']}:{destinations[i]['PORT']}: {e}. Retrying another server...")
				completed_all = False
				break
			# let server reset before next test
			if len(algorithms) > 1:
				time.sleep(1)
		if completed_all:
			runs.extend(dest_runs)
			num_completed += 1
		if num_completed == args.num_tests: # manually keep track of how many tests done
			break
	return runs

# output stuff

def get_series(run, column):
	# returns tuple of lists with times (elapsed_s) and values of one CSV column and skips rows where column value empty
	times = []
	values = []
	for row in run["rows"]:
		if row[column] != "":
			times.append(row["elapsed_s"])
			values.append(row[column])
	return times, values

def get_pairs(run, x_column, y_column):
	# returns tuple of lists with matching values from two CSV columns, skips rows where either value empty
	xs = []
	ys = []
	for row in run["rows"]:
		if row[x_column] != "" and row[y_column] != "":
			xs.append(row[x_column])
			ys.append(row[y_column])
	return xs, ys

def write_samples(runs, path):
	with open(path, "w", newline="") as f:
		writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
		writer.writeheader()
		for run in runs:
			writer.writerows(run["rows"])

def make_summary(runs):
	summary = []
	for run in runs:
		values = []
		# get the goodput values for this run
		times, goodput = get_series(run, "goodput_bps")
		for bps in goodput:
			values.append(bps / 1e6) # convert bps to Mbps
		if len(values) == 0:
			continue # skip this run if no goodput data
		if len(values) > 1:
			p95 = statistics.quantiles(values, n=20, method="inclusive")[-1]
		else:
			p95 = values[0]
		# collate summary stats
		summary.append({
			"run_id": run["run_id"],
			"destination": run["destination"],
			"algorithm": run["algorithm"],
			"min_mbps": round(min(values), 2),
			"median_mbps": round(statistics.median(values), 2),
			"mean_mbps": round(statistics.mean(values), 2),
			"p95_mbps": round(p95, 2),
		})
	return summary

def write_summary(summary, path):
	with open(path, "w", newline="") as f:
		writer = csv.DictWriter(f, fieldnames=["run_id", "destination", "algorithm", "min_mbps", "median_mbps", "mean_mbps", "p95_mbps"])
		writer.writeheader()
		writer.writerows(summary)

def print_summary(summary):
	print(f"\n[+] Goodput (Mbps)\n[+] {'-' * 120}")
	print(f"[+] {'destination':<50} {'algorithm':<10} {'min':>8} {'median':>8} {'mean':>8} {'p95':>8}")
	print(f"[+] {'-' * 120}")
	for s in summary:
		print(f"[+] {s['destination']:<50} {s['algorithm']:<10} {s['min_mbps']:>8.2f} {s['median_mbps']:>8.2f} {s['mean_mbps']:>8.2f} {s['p95_mbps']:>8.2f}")

def make_plots(runs, summary, out_dir):
	# pdf plot generation
	# runs is a list that contains dicts with the following fields (one per completed test):
	#   run["run_id"], run["destination"], run["algorithm"], run["avg_send_rate_mbps"]
	#   run["rows"] is a list of dicts (one per TCP_INFO sample) with keys in CSV_COLUMNS
	# get_series(run, column) returns tuple of (times, values) lists for any column in CSV_COLUMNS, e.g.
	#   times, goodput = get_series(run, "goodput_bps")
	#   times, cwnd = get_series(run, "snd_cwnd_pkts")
	#   times, rtt = get_series(run, "srtt_us")
	#   times, retrans = get_series(run, "interval_retrans_pkts")
	# get_pairs(run, x_column, y_column) returns tuple of (xs, ys) lists, e.g.
	#   cwnd, goodput = get_pairs(run, "snd_cwnd_pkts", "goodput_bps")
	# summary is a list of dicts with run_id, destination, algorithm, min_mbps, median_mbps, mean_mbps, and p95_mbps
	# out_dir is where i put the csvs so pdfs should go there too
	cwnds = []
	alltimes = []
	rtts = []
	retranses = []
	goodputs = []
	for run in runs:
		times, cwnd = get_series(run, "snd_cwnd_pkts")
		alltimes = alltimes + times
		cwnds = cwnds + cwnd
		times, rtt = get_series(run, "srtt_us")
		rtts = rtts + rtt
		times, retrans = get_series(run, "interval_retrans_pkts")
		retranses = retranses + retrans
		times, goodput = get_series(run, "goodput_bps")
		goodputs = goodputs + goodput


	with PdfPages(out_dir + "/multiple_plots.pdf") as pdf:
		#TODO: make the scales consistant between different algorithms
		#which will require running it multiple times to figure out what's reasonable
		#times, cwnd = get_series(runs, "snd_cwnd_pkts")
		plt.plot(alltimes, cwnds)
		plt.xlabel("Time")
		plt.ylabel("snd_cwnd")
		pdf.savefig()
		plt.close()
		#plt.show()

		#times, rtt = get_series(runs, "srtt_us")
		plt.plot(alltimes, rtts)
		plt.xlabel("Time")
		plt.ylabel("RTTs")
		pdf.savefig()
		plt.close()
		#plt.show()

		#times, retrans = get_series(runs, "interval_retrans_pkts")
		plt.plot(alltimes, retranses)
		plt.xlabel("Time")
		plt.ylabel("Loss Signal")
		pdf.savefig()
		plt.close()
		#plt.show()

		#times, goodput = get_series(runs, "goodput_bps")
		plt.plot(alltimes, goodputs)
		plt.xlabel("Time")
		plt.ylabel("Throughput")
		pdf.savefig()
		plt.close()
		#plt.show()

		plt.scatter(goodputs, cwnds)
		plt.xlabel("Goodput")
		plt.ylabel("snd_cwnd")
		pdf.savefig()
		plt.close()
		#plt.show()

		plt.scatter(goodputs, rtts)
		plt.xlabel("goodput")
		plt.ylabel("RTTs")
		pdf.savefig()
		plt.close()
		#plt.show()

		plt.scatter(goodputs, retranses)
		plt.xlabel("goodput")
		plt.ylabel("retransmissions")
		pdf.savefig()
		plt.close()
		#plt.show()
	

def main():
	if not sys.platform.startswith("linux"):
		sys.exit("This script must be run on Linux")

	algorithms = []
	for a in args.algorithms.split(","):
		if a.strip() != "":
			algorithms.append(a.strip())
	check_algorithms(algorithms)
	if len(algorithms) == 0:
		algorithms = [None]  # None = system default

	destinations = load_destinations(args.f)
	random.shuffle(destinations)
	runs = run_tests(destinations, algorithms)
	if len(runs) == 0:
		print("No tests completed.")
		return

	os.makedirs(args.out_dir, exist_ok=True)
	write_samples(runs, os.path.join(args.out_dir, "tcp_samples.csv"))
	summary = make_summary(runs)
	write_summary(summary, os.path.join(args.out_dir, "goodput_summary.csv"))
	print_summary(summary)
	make_plots(runs, summary, args.out_dir)
	print(f"\n[+] Done! Wrote TCP samples and summary to {args.out_dir}/")

if __name__ == "__main__":
	main()
