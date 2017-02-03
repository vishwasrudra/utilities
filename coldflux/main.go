/*
* Most parts of the code is derived from
* Prometheus collectd exporter.
* Link: https://github.com/prometheus/collectd_exporter
 */
package main

import (
	"collectd.org/api"
	"collectd.org/network"
	"context"
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

// timeout specifies the number of iterations after which a metric times out,
// i.e. becomes stale and is removed from collectdCollector.valueLists. It is
// modeled and named after the top-level "Timeout" setting of collectd.
const timeout = 2

var log = logrus.New()

var (
	showVersion      = flag.Bool("version", false, "Print version information.")
	collectdAddress  = flag.String("collectd.listen-address", "127.0.0.1:25826", "Network address on which to accept collectd binary network packets, e.g. \":25826\".")
	collectdBuffer   = flag.Int("collectd.udp-buffer", 0, "Size of the receive buffer of the socket used by collectd binary protocol receiver.")
	collectdAuth     = flag.String("collectd.auth-file", "", "File mapping user names to pre-shared keys (passwords).")
	collectdSecurity = flag.String("collectd.security-level", "None", "Minimum required security level for accepted packets. Must be one of \"None\", \"Sign\" and \"Encrypt\".")
	collectdTypesDB  = flag.String("collectd.typesdb-file", "/opt/collectd/share/collectd/types.db", "Collectd types.db file for datasource names mapping. Needed only if using a binary network protocol.")
)

type collectdCollector struct {
	ch         chan api.ValueList
	valueLists map[string]api.ValueList
	mu         *sync.Mutex
}

func newCollectdCollector() *collectdCollector {
	c := &collectdCollector{
		ch:         make(chan api.ValueList, 0),
		valueLists: make(map[string]api.ValueList),
		mu:         &sync.Mutex{},
	}
	go c.processSamples()
	go c.influxWrite()
	return c
}

func (c *collectdCollector) processSamples() {
	ticker := time.NewTicker(time.Minute).C
	for {
		select {
		case vl := <-c.ch:
			id := vl.Identifier.String()
			c.mu.Lock()
			c.valueLists[id] = vl
			c.mu.Unlock()

		case <-ticker:
			// Garbage collect expired value lists.
			now := time.Now()
			c.mu.Lock()
			for id, vl := range c.valueLists {
				validUntil := vl.Time.Add(timeout * vl.Interval)
				if validUntil.Before(now) {
					delete(c.valueLists, id)
				}
			}
			c.mu.Unlock()
		}
	}
}

// implements influxdb Collector.
func (c *collectdCollector) influxWrite() {
	for {
		c.mu.Lock()
		valueLists := make([]api.ValueList, 0, len(c.valueLists))
		for id, vl := range c.valueLists {
			valueLists = append(valueLists, vl)
			delete(c.valueLists, id)
		}
		c.mu.Unlock()
		now := time.Now()
		for _, vl := range valueLists {
			validUntil := vl.Time.Add(timeout * vl.Interval)
			if validUntil.Before(now) {
				continue
			}
			var value float64
			for i := range vl.Values {

				switch v := vl.Values[i].(type) {
				case api.Gauge:
					value = float64(v)
				case api.Derive:
					value = float64(v)
				case api.Counter:
					value = float64(v)
				default:
					fmt.Print("Unknown Type")
				}
				//fmt.Print("Type : \t", vl.Type, "\t")
				//fmt.Print("Plugin : \t", vl.Plugin, "\t")
				//fmt.Print("Type-Instance : \t", vl.TypeInstance, "\t")
				fmt.Print("ID : ", vl.Identifier.String(), "\t")
				fmt.Print("Value : ", value)
				fmt.Print("\n")
			}
		}
	}
}

// Write writes "vl" to the collector's channel, to be (asynchronously)
// processed by processSamples(). It implements api.Writer.
func (c collectdCollector) Write(_ context.Context, vl *api.ValueList) error {
	c.ch <- *vl
	return nil
}

func startCollectdServer(ctx context.Context, w api.Writer) {
	if *collectdAddress == "" {
		return
	}

	srv := network.Server{
		Addr:   *collectdAddress,
		Writer: w,
	}

	if *collectdAuth != "" {
		srv.PasswordLookup = network.NewAuthFile(*collectdAuth)
	}

	if *collectdTypesDB != "" {
		file, err := os.Open(*collectdTypesDB)
		if err != nil {
			log.Fatalf("Can't open types.db file %s", *collectdTypesDB)
		}
		defer file.Close()

		typesDB, err := api.NewTypesDB(file)
		if err != nil {
			log.Fatalf("Error in parsing types.db file %s", *collectdTypesDB)
		}
		srv.TypesDB = typesDB
	}

	switch strings.ToLower(*collectdSecurity) {
	case "", "none":
		srv.SecurityLevel = network.None
	case "sign":
		srv.SecurityLevel = network.Sign
	case "encrypt":
		srv.SecurityLevel = network.Encrypt
	default:
		log.Fatalf("Unknown security level %q. Must be one of \"None\", \"Sign\" and \"Encrypt\".", *collectdSecurity)
	}

	laddr, err := net.ResolveUDPAddr("udp", *collectdAddress)
	if err != nil {
		log.Panic("Failed to resolve binary protocol listening UDP address %q: %v", *collectdAddress, err)
	}

	if laddr.IP != nil && laddr.IP.IsMulticast() {
		srv.Conn, err = net.ListenMulticastUDP("udp", nil, laddr)
	} else {
		srv.Conn, err = net.ListenUDP("udp", laddr)
	}
	if err != nil {
		log.Panic("Failed to create a socket for a binary protocol server: %v", err)
	}
	if *collectdBuffer >= 0 {
		if err = srv.Conn.SetReadBuffer(*collectdBuffer); err != nil {
			log.Panic("Failed to adjust a read buffer of the socket: %v", err)
		}
	}

	go func() {
		log.Panic(srv.ListenAndWrite(ctx))
	}()
}

func main() {
	flag.Parse()
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	if *showVersion {
		fmt.Println("collectd Influx 1.0.0")
		os.Exit(0)
	}

	c := newCollectdCollector()
	startCollectdServer(context.Background(), c)

	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		done <- true
	}()

	fmt.Println("Receiving collectd metrics")
	<-done
	fmt.Println("exiting")
}
