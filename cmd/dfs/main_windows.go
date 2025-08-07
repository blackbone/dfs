//go:build windows

package main

import "log"

const msg = "dfs unsupported on windows"

func main() { log.Fatal(msg) }
