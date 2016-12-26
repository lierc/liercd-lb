package main

import (
	"fmt"
	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"
)

type LoadBalancer struct {
	sync.RWMutex
	KeyApi   client.KeysAPI
	Prefix   string
	Backends []string
	Proxies  map[string]*httputil.ReverseProxy
}

func CreateLoadBalancer(endpoints []string, prefix string) *LoadBalancer {
	cfg := client.Config{
		Endpoints:               endpoints,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}

	c, err := client.New(cfg)

	if err != nil {
		log.Fatal(err)
	}

	lb := &LoadBalancer{
		KeyApi:   client.NewKeysAPI(c),
		Proxies:  make(map[string]*httputil.ReverseProxy),
		Backends: make([]string, 0),
		Prefix:   prefix,
	}

	lb.SyncBackends()
	go lb.WatchBackends()

	return lb
}

func (lb *LoadBalancer) Key(key string) string {
	return fmt.Sprintf("/%s/%s", lb.Prefix, key)
}

func (lb *LoadBalancer) SyncBackends() {
	opt := &client.GetOptions{Recursive: true}
	resp, err := lb.KeyApi.Get(context.Background(), lb.Key("backend"), opt)

	if err != nil {
		if err == context.Canceled {
			log.Panic("canceled: %v", err)
		} else if err == context.DeadlineExceeded {
			log.Panic("exceeded deadline: %v", err)
		} else if cerr, ok := err.(*client.ClusterError); ok {
			log.Panic("cert error: %v", cerr)
		} else {
			log.Panic("bad cluster endpoint: %v", err)
		}
	}

	if !resp.Node.Dir {
		log.Fatal("/backend is not a directory")
	}

	defer lb.Unlock()
	lb.Lock()

	lb.Backends = make([]string, 0)

	for _, node := range resp.Node.Nodes {
		lb.Backends = append(lb.Backends, node.Value)
	}

	log.Printf("backends: %v", lb.Backends)
}

func (lb *LoadBalancer) RandomBackend() string {
	defer lb.RUnlock()
	lb.RLock()
	return lb.Backends[rand.Intn(len(lb.Backends))]
}

func (lb *LoadBalancer) WatchBackends() {
	opt := &client.WatcherOptions{Recursive: true}
	watcher := lb.KeyApi.Watcher(lb.Key("backend"), opt)

	for {
		_, err := watcher.Next(context.Background())

		if err != nil {
			if err == context.Canceled {
				log.Panic("canceled: %v", err)
			} else if err == context.DeadlineExceeded {
				log.Panic("exceeded deadline: %v", err)
			} else if cerr, ok := err.(*client.ClusterError); ok {
				log.Panic("cert error: %v", cerr)
			} else {
				log.Panic("bad cluster endpoint: %v", err)
			}
		}

		lb.SyncBackends()
	}
}

func (lb *LoadBalancer) GetProxy(host string) (*httputil.ReverseProxy, bool) {
	lb.RLock()
	defer lb.RUnlock()

	if proxy, ok := lb.Proxies[host]; ok {
		return proxy, true
	} else {
		return nil, false
	}
}

func (lb *LoadBalancer) AddProxy(host string) (*httputil.ReverseProxy, error) {
	url, err := url.Parse(host)

	if err != nil {
		return nil, err
	}

	proxy := httputil.NewSingleHostReverseProxy(url)
	lb.Lock()
	lb.Proxies[host] = proxy
	lb.Unlock()

	return proxy, nil
}

func (lb *LoadBalancer) ProxyRequest(w http.ResponseWriter, r *http.Request) {
	parts := strings.SplitN(r.URL.Path, "/", 3)

	if len(parts) < 3 {
		io.WriteString(w, "Invalid request")
		return
	}

	id := parts[1]
	k := fmt.Sprintf("clients/%s", id)
	log.Printf("looking up %s", k)

	var backend string
	resp, err := lb.KeyApi.Get(context.Background(), lb.Key(k), nil)

	if err != nil {
		if err == context.Canceled {
			log.Panic("canceled: %v", err)
		} else if err == context.DeadlineExceeded {
			log.Panic("exceeded deadline: %v", err)
		} else if cerr, ok := err.(*client.ClusterError); ok {
			log.Panic("cert error: %v", cerr)
		} else {
			backend = lb.RandomBackend()
		}
	} else {
		backend = resp.Node.Value
	}

	if proxy, ok := lb.GetProxy(backend); ok {
		proxy.ServeHTTP(w, r)
	} else {
		proxy, err := lb.AddProxy(backend)
		if err != nil {
			log.Panic(err)
		}
		proxy.ServeHTTP(w, r)
	}
}
