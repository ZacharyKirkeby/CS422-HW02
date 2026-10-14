# CS422-HW02

## Running with Docker

Build the image:

```sh
docker build -t cs422-hw02 .
```

Host folder mounted at `/usr/local/app/results` is used to write TCP samples and summary table (and TODO pdf graphs). Args are passed to the script:

```sh
mkdir -p results
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$PWD/results:/usr/local/app/results" \
  cs422-hw02 --duration 60 --num-tests 10 --algorithms cubic,reno,bbr
```
If you are on windows, you need to enable bbr in the WSL2 kernel. Run the following command before running the docker container:
```wsl -d docker-desktop -u root -- sh -c "modprobe tcp_bbr; cat /proc/sys/net/ipv4/tcp_available_congestion_control"```

```pwsh
docker run --rm `
  -v "${PWD}/results:/usr/local/app/results" `
  cs422-hw02 --duration 60 --num-tests 10 --algorithms cubic,reno,bbr --repetitions 5
```

You have to create `results/` before running or else docker creates it and it breaks everything since the folder isn't writable (docker creates it owned by root).