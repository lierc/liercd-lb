package main

import (
	"log"
	"net/http"
)

func main() {
	hosts := []string{"http://127.0.0.1:2379"}
	lb := CreateLoadBalancer(hosts, "liercd")

	log.Printf("Listening on port 5002")
	http.HandleFunc("/", lb.ProxyRequest)
	http.ListenAndServe("127.0.0.1:5002", nil)
}
