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
parser.add_argument("--repetitions", type=int, default=3, help="Number of repeated CUBIC/Reno/BBR comparison runs for Q3")
parser.add_argument("--max-attempts", type=int, default=30, help="Maximum servers to try while finding successful destinations")

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
# refactored with gpt
def run_one(dest, algorithm, run_id, phase="default", repetition=1, test_order=1):
	host = dest["IP/HOST"]
	port = get_first_port(dest["PORT"])
	control = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	# select congestion control algorithm
	if algorithm:
		data.setsockopt(socket.IPPROTO_TCP, TCP_CONGESTION, algorithm.encode())
	# read algorithm configured by kernel
	used_algorithm = (data.getsockopt(socket.IPPROTO_TCP, 
	                                  TCP_CONGESTION,16).split(b"\0")[0].decode())
	print(f"[*] Running test {run_id} on " f"{host}:{port} with algorithm {used_algorithm}")

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
	sampler = threading.Thread(
		target=sample_tcp_stats,
		args=(iperfclient, samples, stop)
	)
	sampler.start()
	try:
		iperfclient.run()
	finally:
		stop.set()
		sampler.join()

	# process and collate for output
	destination = (
		f"{host}:{port} "
		f"({dest['SITE']}, {dest['COUNTRY']})"
	)
	mbps = (iperfclient.bytes_sent * 8 / args.duration / 1e6)
	run = {
		"run_id": run_id,
		"phase": phase,
		"repetition": repetition,
		"test_order": test_order,
		"destination": destination,
		"algorithm": used_algorithm,
		"avg_send_rate_mbps": mbps,
		"rows": make_rows(
			run_id,
			destination,
			used_algorithm,
			samples
		),
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

def run_default_tests(destinations):
	runs = []
	selected_destinations = []
	failures = []
	attempts = 0
	run_id = 1

	print("\n Q1/Q2: running default congestion control tests")
	for dest in destinations:
		if len(selected_destinations) >= args.num_tests:
			break
		if attempts >= args.num_tests * 3:
			break
		attempts += 1
		busy_attempts = 0
		while True:
			try:
				run = run_one(dest, None, run_id, phase="default", repetition=1, test_order=1)
				runs.append(run)
				selected_destinations.append(dest)

				print(
					f"[+] {run_id} {run['destination']} "
					f"[{run['algorithm']}]: "
					f"{run['avg_send_rate_mbps']:.1f} Mbps average, "
					f"{len(run['rows'])} TCP samples"
				)

				run_id += 1
				break

			except iperf3jank.Iperf3ServerBusyException as e:
				busy_attempts += 1

				if busy_attempts > 3:
					destination = (
						f"{dest['IP/HOST']}:"
						f"{get_first_port(dest['PORT'])}"
					)

					print(
						f"[x] Server {destination} remained busy "
						f"after 3 retries. Trying replacement..."
					)

					failures.append({
						"destination": destination,
						"reason": str(e),
					})

					break

				print(
					f"[x] Server {dest['IP/HOST']}:"
					f"{get_first_port(dest['PORT'])} is busy. "
					f"Retrying in 2 seconds "
					f"({busy_attempts}/3)..."
				)

				time.sleep(2)

			except iperf3jank.Iperf3Exception as e:
				destination = (
					f"{dest['IP/HOST']}:"
					f"{get_first_port(dest['PORT'])}"
				)

				print(
					f"[x] Error testing {destination}: {e}. "
					f"Trying replacement..."
				)

				failures.append({
					"destination": destination,
					"reason": str(e),
				})

				break

	if len(selected_destinations) < args.num_tests:
		print(
			f"[x] Requested {args.num_tests} destinations but only "
			f"{len(selected_destinations)} succeeded after "
			f"{attempts} attempts."
		)

	return runs, selected_destinations, failures

def run_comparison_tests(
	selected_destinations,
	algorithms,
	start_run_id
):
	runs = []
	failures = []
	run_id = start_run_id

	print(
		f"\n[+] Q3: comparing "
		f"{', '.join(algorithms)}"
	)

	for repetition in range(1, args.repetitions + 1):
		# rotate algorithm order so one algorithm does not always run first
		offset = (
			(repetition - 1)
			% len(algorithms)
		)

		ordered_algorithms = (
			algorithms[offset:]
			+ algorithms[:offset]
		)

		print(
			f"\n[+] Repetition {repetition}/"
			f"{args.repetitions}: "
			f"{' -> '.join(ordered_algorithms)}"
		)

		for dest in selected_destinations:
			for test_order, algorithm in enumerate(
				ordered_algorithms,
				start=1
			):
				busy_attempts = 0

				while True:
					try:
						run = run_one(
							dest,
							algorithm,
							run_id,
							phase="comparison",
							repetition=repetition,
							test_order=test_order
						)

						runs.append(run)

						print(
							f"[+] {run_id} "
							f"{run['destination']} "
							f"[{run['algorithm']}] "
							f"rep={repetition} "
							f"order={test_order}: "
							f"{run['avg_send_rate_mbps']:.1f} Mbps"
						)

						run_id += 1
						break

					except iperf3jank.Iperf3ServerBusyException as e:
						busy_attempts += 1

						if busy_attempts > 3:
							destination = (
								f"{dest['IP/HOST']}:"
								f"{get_first_port(dest['PORT'])}"
							)

							print(
								f"[x] Q3 server {destination} "
								f"[{algorithm}] remained busy "
								f"after 3 retries."
							)

							failures.append({
								"destination": destination,
								"algorithm": algorithm,
								"repetition": repetition,
								"reason": str(e),
							})

							break

						print(
							f"[x] Server {dest['IP/HOST']}:"
							f"{get_first_port(dest['PORT'])} "
							f"[{algorithm}] is busy. "
							f"Retrying in 2 seconds "
							f"({busy_attempts}/3)..."
						)

						time.sleep(2)

					except iperf3jank.Iperf3Exception as e:
						destination = (
							f"{dest['IP/HOST']}:"
							f"{get_first_port(dest['PORT'])}"
						)

						print(
							f"[x] Q3 failed: "
							f"{destination} "
							f"[{algorithm}]: {e}"
						)

						failures.append({
							"destination": destination,
							"algorithm": algorithm,
							"repetition": repetition,
							"reason": str(e),
						})

						break

				# let public server/path settle before next test
				time.sleep(1)

	return runs, failures

# output stuff

def get_series(run, column, algorithm):
    # returns tuple of lists with times (elapsed_s) and values of one CSV column and skips rows where column value empty
    # only appends if row's algorithm type matches the provided type; if input is 'None', then doesn't check for algorithm type.
    
    times = []
    values = []
    for row in run["rows"]:
        row_is_valid_algo = (algorithm == None or row["algorithm"] == algorithm)
        
        if row_is_valid_algo and row[column] != "":
            times.append(row["elapsed_s"])
            values.append(row[column])
    return times, values

# same as above, but we skip the first sample
# this is critical for interval related graphing functionalities to not break!
def get_series_ignore_first_sample(run, column, algorithm):
    # returns tuple of lists with times (elapsed_s) and values of one CSV column and skips rows where column value empty
    # only appends if row's algorithm type matches the provided type; if input is 'None', then doesn't check for algorithm type.
    
    times = []
    values = []
    for row in run["rows"]:
        row_is_algo = (algorithm == None or row["algorithm"] == algorithm)
        
        if row_is_algo and row["sample"] > 0 and row[column] != "":
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
        times, goodput = get_series(run, "goodput_bps", None)
        for bps in goodput:
            
            if isinstance(bps, str) == True:
                print("\n[+]WARNING: bps is string: " + str(bps)) # print to debug; if this triggers, 'bps' is most likely empty/invalid
                
                # append nothing for now? (technically an error case here?)
                #values.append(float('-inf'))
            else:
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


# TODO: provide plot names
def make_plots(runs, summary, algorithms, representative_destinations, file_name_base, out_dir):
	# pdf plot generation:
    #
	# runs is a list that contains dicts with the following fields (one per completed test):
	#   run["run_id"], run["destination"], run["algorithm"], run["avg_send_rate_mbps"]
	#   run["rows"] is a list of dicts (one per TCP_INFO sample) with keys in CSV_COLUMNS
    #
	# get_series(run, column, algorithm) returns tuple of (times, values) lists for any column that matches the input algorithm in CSV_COLUMNS, e.g.
	#   times, goodput = get_series(run, "goodput_bps", algorithm)
	#   times, cwnd = get_series(run, "snd_cwnd_pkts", algorithm)
	#   times, rtt = get_series(run, "srtt_us", algorithm)
	#   times, retrans = get_series(run, "interval_retrans_pkts")
    #
	# get_pairs(run, x_column, y_column) returns tuple of (xs, ys) lists, e.g.
	#   cwnd, goodput = get_pairs(run, "snd_cwnd_pkts", "goodput_bps")
	# summary is a list of dicts with run_id, destination, algorithm, min_mbps, median_mbps, mean_mbps, and p95_mbps
	# out_dir is where i put the csvs so pdfs should go there too
    #
    # 'algorithms': list of algorithms ran; need to graph separately for each algo, so must track ran algorithms.
    #
    # 'representative_destinations': should be a list of representative destinations for runs
    #
    # 'run_range': [inclusive start, exclusive end] of run data that we will consider
    
    
    # revised graphing schema:
    # as per the assignment, "Use the same representative destination for all three algorithms, with clearly labeled curves or separate panels and consistent axis scales to facilitate comparison"
    # therefore, will need to actually graph everyone onto the same graph per destination
    # TODO: Zach will be responsible for updating polling s.t. there won't be any repeated (algo, destination) data if the connection was successful; so for now, make sure to run with 'repetitions = 1'!
    
    # TODO: update this function s.t. it'll work for Q2 independently, and then Q3.
    
    """
    destination:
    
    - algo 0, run(s) data:
        cwnds = []
        alltimes = []
        rtts = []
        retranses = []
        goodputs = []
        alltimes_interval = []
        cwnds_interval = []
        rtts_interval = []
    
    - algo 1, runs data:
        (...)
        
    """
    
    # pre-processing should now create DS for each destination as described by above
    # when actually graphing, then we need to create graphs on a per-destination basis!
    
    destination_data = {}
    for d in representative_destinations:
        destination_data[d] = {}
        
        for a in algorithms:
            destination_data[d][a] = {}
            
            destination_data[d][a]['cwnds'] = []
            destination_data[d][a]['alltimes'] = []
            destination_data[d][a]['rtts'] = []
            destination_data[d][a]['retranses'] = []
            destination_data[d][a]['goodputs'] = []
            
            destination_data[d][a]['alltimes_interval'] = []
            destination_data[d][a]['cwnds_interval'] = []
            destination_data[d][a]['rtts_interval'] = []
    
    
    ###############################################################################################
    # PREPROCESSING STEP: fill up 'destination_data' with per-algorithm data for each destination #
    ###############################################################################################
    
    for run in runs:
        """
        cwnds = []
        alltimes = []
        rtts = []
        retranses = []
        goodputs = []
        
        # 'alltimes' but we ignore the very first sample of each run.
        # this is s.t. our collected runtime data (i.e. goodput) can be graphed properly, as these values (interval_s, goodput_bps, interval_retrans_pkts, interval_retrans_ratio) are only calculated AFTER the very first sample.
        # and therefore the output array count could differ than non-interval array counts, which is not ideal.
        alltimes_interval = []
        cwnds_interval = []
        rtts_interval = []
        """
        
        
        # check if we should use this run
        #if run['run_id'] in range(run_range[0], run_range[1]):
        if 1 == 1:
            
            # determine the destination of this run
            run_dest = run['destination']
            
            # determine the algo
            run_algo = run['algorithm']
            
            
            # now, collect data associated with this run
            
            times, cwnd = get_series(run, "snd_cwnd_pkts", None)
            #alltimes = alltimes + times
            #cwnds = cwnds + cwnd
            
            _, rtt = get_series(run, "srtt_us", None)
            #rtts = rtts + rtt
            # translate microseconds to milliseconds
            for i in range(len(rtt)):
                rtt[i] /= 1000
            
            _, retrans = get_series(run, "interval_retrans_pkts", None)
            #retranses = retranses + retrans
            
            _, goodput = get_series(run, "goodput_bps", None)
            #goodputs = goodputs + goodput
            # translate bps to Kbps
            for i in range(len(goodput)):
                goodput[i] /= 1000
            
            # fill arrays of interval-related stuff, where we always ignore the first sample
            newtimes, newcwnd = get_series_ignore_first_sample(run, "snd_cwnd_pkts", None)
            #alltimes_interval = alltimes_interval + newtimes
            #cwnds_interval = cwnds_interval + newcwnd
            
            _, newrtt = get_series_ignore_first_sample(run, "srtt_us", None)
            # translate microseconds to milliseconds
            for i in range(len(newrtt)):
                newrtt[i] /= 1000
            #rtts_interval = rtts_interval + newrtt
            
            
            # now that we've gathered this data, add them to appropriate data structures
            destination_data[run_dest][run_algo]['cwnds'] += cwnd
            destination_data[run_dest][run_algo]['alltimes'] += times
            destination_data[run_dest][run_algo]['rtts'] += rtt
            destination_data[run_dest][run_algo]['retranses'] += retrans
            destination_data[run_dest][run_algo]['goodputs'] += goodput
            
            destination_data[run_dest][run_algo]['alltimes_interval'] += newtimes
            destination_data[run_dest][run_algo]['cwnds_interval'] += newcwnd
            destination_data[run_dest][run_algo]['rtts_interval'] += newrtt
    
    
    
    ##############################
    # actual plotting stuff time #
    ##############################
    
    # label names
    alltimes_label = "Time Elapsed (s)"
    cwnd_label = "Congestion Window Size (packets)"
    rtts_label = "Smoothed RTTs (ms)"
    retranses_label = "Retransmitted Packets (in last time interval)"
    goodput_label = "Goodput (Kbps)"
    
    # destination id tracker; to generate graph names as appropriate
    destination_id = 0
    
    for destination in representative_destinations:
        destination_id += 1
        """
        cwnds = []
        alltimes = []
        rtts = []
        retranses = []
        goodputs = []
        
        # 'alltimes' but we ignore the very first sample of each run.
        # this is s.t. our collected runtime data (i.e. goodput) can be graphed properly, as these values (interval_s, goodput_bps, interval_retrans_pkts, interval_retrans_ratio) are only calculated AFTER the very first sample.
        # and therefore the output array count could differ than non-interval array counts, which is not ideal.
        alltimes_interval = []
        cwnds_interval = []
        rtts_interval = []
        """
        
        
        """
        for run in runs:
            times, cwnd = get_series(run, "snd_cwnd_pkts", destination)
            alltimes = alltimes + times
            cwnds = cwnds + cwnd
            _, rtt = get_series(run, "srtt_us", destination)
            rtts = rtts + rtt
            _, retrans = get_series(run, "interval_retrans_pkts", destination)
            retranses = retranses + retrans
            _, goodput = get_series(run, "goodput_bps", destination)
            goodputs = goodputs + goodput
            
            # fill arrays of interval-related stuff, where we always ignore the first sample
            newtimes, newcwnd = get_series_ignore_first_sample(run, "snd_cwnd_pkts", algorithm)
            alltimes_interval = alltimes_interval + newtimes
            cwnds_interval = cwnds_interval + newcwnd
            
            _, newrtt = get_series_ignore_first_sample(run, "srtt_us", algorithm)
            rtts_interval = rtts_interval + newrtt
            
            
            # add collected data to run-specific data structurs
            run_id = run['run_id'];
            if run_id in algo_to_runs[algorithm]['runIds']:
                # print("found run " + str(run_id) + " in algo: " + algorithm)
                # save run-specific data into appropriate arrays and stuff
                
                algo_to_runs[algorithm]['runData'][run_id]['alltimes'] = times
                algo_to_runs[algorithm]['runData'][run_id]['alltimes_interval'] = newtimes
                algo_to_runs[algorithm]['runData'][run_id]['cwnds'] = cwnd
                algo_to_runs[algorithm]['runData'][run_id]['rtts'] = rtt
                algo_to_runs[algorithm]['runData'][run_id]['retranses'] = retrans
                algo_to_runs[algorithm]['runData'][run_id]['goodputs'] = goodput
        """
        
        """
        # DEBUGGING:
        def debugPrint(name, arr):
            print("[+] " + str(name) + ", len: " + str(len(arr)) + ", values: " + str(arr));
        
        print()
        debugPrint('cwnds', cwnds);
        debugPrint('alltimes', alltimes);
        debugPrint('rtts', rtts);
        debugPrint('retranses', retranses);
        debugPrint('goodputs', goodputs);
        debugPrint('alltimes_interval', alltimes_interval);
        debugPrint('cwnds_interval', cwnds_interval);
        debugPrint('rtts_interval', rtts_interval);
        print()
        """
        
        # Variable Name to Value Meaning Translation:
        
        # alltimes =    Time elapsed, in seconds
        # rtts =        Smoothed RTTs, in milliseconds
        # cwnd =        Congestion window size, in packets
        # retranses =   Retransmitted packet count this interval, integer
        # goodputs =    Goodput, in Kbps
        
        # dynamically generate graph name
        output_file_name = "/" + str(file_name_base) + "_" + str(destination_id) + ".pdf"

        with PdfPages(out_dir + output_file_name) as pdf:
            # Scales are consistent between different algorithms because per destination, all the algos are graphed on the same plots.
            
            # Plot each graph (line or scatter) for this specific destination.
            # The data will be labeled/separated on a per-algorithm basis.
            
            
            ###############
            # LINE GRAPHS #
            ###############
            
            #times, cwnd = get_series(runs, "snd_cwnd_pkts")
            # plot data per algo for this destination
            for algorithm in destination_data[destination]:
                data = destination_data[destination][algorithm]    #algo_to_runs[algorithm]['runData'][run_id];
                plt.plot(data['alltimes'], data['cwnds'], label = "Algo " + str(algorithm))
            plt.xlabel(alltimes_label)
            plt.ylabel(cwnd_label)
            plt.legend()
            plt.title("Destination: " + str(destination))
            pdf.savefig()
            plt.close()
            #plt.show()
        
            #times, rtt = get_series(runs, "srtt_us")
            #plt.plot(alltimes, rtts)
            # plot data per algo for this destination
            for algorithm in destination_data[destination]:
                data = destination_data[destination][algorithm]    #algo_to_runs[algorithm]['runData'][run_id];
                plt.plot(data['alltimes'], data['rtts'], label = "Algo " + str(algorithm))
            plt.xlabel(alltimes_label)
            plt.ylabel(rtts_label)
            plt.legend()
            plt.title("Destination: " + str(destination))
            pdf.savefig()
            plt.close()
            #plt.show()

            #plt.plot(alltimes_interval, retranses)
            # plot data per algo for this destination
            for algorithm in destination_data[destination]:
                data = destination_data[destination][algorithm]    #algo_to_runs[algorithm]['runData'][run_id];
                plt.plot(data['alltimes_interval'], data['retranses'], label = "Algo " + str(algorithm))
            plt.xlabel(alltimes_label)
            plt.ylabel(retranses_label)
            plt.legend()
            plt.title("Destination: " + str(destination))
            pdf.savefig()
            plt.close()
            #plt.show()

            #times, goodput = get_series(runs, "goodput_bps")
            #plt.plot(alltimes_interval, goodputs)
            # plot data per algo for this destination
            for algorithm in destination_data[destination]:
                data = destination_data[destination][algorithm]    #algo_to_runs[algorithm]['runData'][run_id];
                plt.plot(data['alltimes_interval'], data['goodputs'], label = "Algo " + str(algorithm))
            plt.xlabel(alltimes_label)
            plt.ylabel(goodput_label)
            plt.legend()
            plt.title("Destination: " + str(destination))
            pdf.savefig()
            plt.close()
            #plt.show()
            
            
            ################
            # SCATTERPLOTS #
            ################

            #plt.scatter(goodputs, cwnds_interval)
            # plot data per algo for this destination
            for algorithm in destination_data[destination]:
                data = destination_data[destination][algorithm]    #algo_to_runs[algorithm]['runData'][run_id];
                plt.scatter(data['goodputs'], data['cwnds_interval'], label = "Algo " + str(algorithm))
            plt.xlabel(goodput_label)
            plt.ylabel(cwnd_label)
            #plt.legend(plt.legend_elements(), title="Title")
            plt.legend()
            plt.title("Destination: " + str(destination))
            pdf.savefig()
            plt.close()
            #plt.show()

            #'value' must be an instance of str or bytes, not an int
            #plt.scatter(goodputs, rtts_interval)
            # plot data per algo for this destination
            for algorithm in destination_data[destination]:
                data = destination_data[destination][algorithm]    #algo_to_runs[algorithm]['runData'][run_id];
                plt.scatter(data['goodputs'], data['rtts_interval'], label = "Algo " + str(algorithm))
            plt.xlabel(goodput_label)
            plt.ylabel(rtts_label)
            plt.legend()
            plt.title("Destination: " + str(destination))
            pdf.savefig()
            plt.close()
            #plt.show()

            #plt.scatter(goodputs, retranses)
            # plot data per algo for this destination
            for algorithm in destination_data[destination]:
                data = destination_data[destination][algorithm]    #algo_to_runs[algorithm]['runData'][run_id];
                plt.scatter(data['goodputs'], data['retranses'], label = "Algo " + str(algorithm))
            plt.xlabel(goodput_label)
            plt.ylabel(retranses_label)
            plt.legend()
            plt.title("Destination: " + str(destination))
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
	if len(algorithms) > 0:
		check_algorithms(algorithms)

	destinations = load_destinations(args.f)
	random.shuffle(destinations)

	# Q1 / Q2 shit
	default_runs, selected_destinations, default_failures = (
		run_default_tests(destinations)
	)

	if len(default_runs) == 0:
		print("No default congestion-control tests completed.")
		return

	#representative_destination = default_runs[0]["destination"]
	representative_destination = []
	for dest in default_runs:
		representative_destination.append(dest["destination"])

	print(
		f"\n[+] Representative destination(s): "
		f"{representative_destination}"
	)
    
    

	# Q3 - all 3 algos
	comparison_runs = []
	comparison_failures = []
	if len(algorithms) > 0:
		comparison_runs, comparison_failures = (
			run_comparison_tests(selected_destinations, algorithms, len(default_runs) + 1))
	else:
		print(
			"\n[+] No --algorithms provided; "
			"skipping Q3 comparison tests."
		)
		os.makedirs(args.out_dir, exist_ok=True)

	# combine the two phases only for raw output
	runs = default_runs + comparison_runs

	write_samples(runs, os.path.join(args.out_dir, "tcp_samples.csv"))
	
	# Q1/Q2 summary
	default_summary = make_summary(default_runs)

	write_summary(default_summary,
		os.path.join(args.out_dir, "default_goodput_summary.csv"))
	print("\n[+] Q1/Q2 default congestion control")
	print_summary(default_summary)
    
	if len(default_runs) > 0:
    	# plot base graphs
		default_algo = [];
		default_algo.append(default_runs[0]['algorithm'])
		make_plots(default_runs, default_summary, default_algo, representative_destination, "representative_plots", args.out_dir)
	else:
		print("WARNING: No representative runs to graph.");

	# Q3 summary
	if len(comparison_runs) > 0:
		comparison_summary = make_summary(comparison_runs)
		write_summary(comparison_summary,
			os.path.join(args.out_dir, "comparison_goodput_summary.csv"))
		print("\n[+] Q3 congestion control comparison")
		print_summary(comparison_summary)
        
		# plot comparison graphs
		make_plots(comparison_runs, comparison_summary, algorithms, representative_destination, "comparison_plots", args.out_dir)

	print(
		f"\n[+] Done! Wrote results to "
		f"'{args.out_dir}'/"
	)
	print(f"\n[+] Done! Wrote TCP samples and summary to '{args.out_dir}'/")

if __name__ == "__main__":
	main()
