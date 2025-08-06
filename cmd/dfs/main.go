// Program dfs starts a single DFS node.
package main

import (
	"flag"
	"log"
	"strings"
	"time"

	"dfs"
	dfsfs "dfs/internal/fusefs"
	"dfs/internal/node"
)

func main() {
	id := flag.String("id", "node1", "node ID")
	addr := flag.String("addr", "127.0.0.1:9000", "HTTP bind address")
	peerStr := flag.String("peers", "", "comma separated peer addresses")
	hostfs := flag.String("hostfs", "/mnt/hostfs", "host cache directory")
	blobdir := flag.String("blobdir", "/var/lib/dfs/blobstore", "blob store directory")
	flag.Parse()

	var peers []string
	if *peerStr != "" {
		peers = strings.Split(*peerStr, ",")
	}

	n, err := node.New(*id, *addr, *hostfs, *blobdir, peers)
	if err != nil {
		log.Fatalf("node: %v", err)
	}
	dfs.SetNode(n)
	n.BackgroundCheck(30 * time.Second)

	go func() {
		if err := dfsfs.Mount("/mnt/dfs", *hostfs); err != nil {
			log.Fatalf("mount: %v", err)
		}
	}()
	log.Printf("node %s listening on %s", *id, *addr)
	select {}
}
