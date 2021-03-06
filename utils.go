package netd

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

//==============================================================================

//LoadTLS loads a tls.Config from a key and cert file path
func LoadTLS(cert string, key string, ca string) (*tls.Config, error) {
	var config *tls.Config
	config.MinVersion = tls.VersionTLS12
	config.Certificates = make([]tls.Certificate, 1)

	c, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}

	c.Leaf, err = x509.ParseCertificate(c.Certificate[0])
	if err != nil {
		return nil, err
	}

	if ca != "" {
		rootPEM, err := ioutil.ReadFile(ca)
		if err != nil {
			return nil, err
		}

		if rootPEM == nil {
			return nil, errors.New("Empty perm file")
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(rootPEM) {
			return nil, errors.New("Failed to append perm file data")
		}

		config.RootCAs = pool
	}

	config.Certificates[0] = c
	return config, nil
}

// TLSVersion returns a string version number based on the tls version int.
func TLSVersion(ver uint16) string {
	switch ver {
	case tls.VersionTLS10:
		return "1.0"
	case tls.VersionTLS11:
		return "1.1"
	case tls.VersionTLS12:
		return "1.2"
	}
	return fmt.Sprintf("Unknown [%x]", ver)
}

// TLSCipher returns a cipher string version based on the supplied hex value.
func TLSCipher(cs uint16) string {
	switch cs {
	case 0x0005:
		return "TLS_RSA_WITH_RC4_128_SHA"
	case 0x000a:
		return "TLS_RSA_WITH_3DES_EDE_CBC_SHA"
	case 0x002f:
		return "TLS_RSA_WITH_AES_128_CBC_SHA"
	case 0x0035:
		return "TLS_RSA_WITH_AES_256_CBC_SHA"
	case 0xc007:
		return "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA"
	case 0xc009:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA"
	case 0xc00a:
		return "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA"
	case 0xc011:
		return "TLS_ECDHE_RSA_WITH_RC4_128_SHA"
	case 0xc012:
		return "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA"
	case 0xc013:
		return "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"
	case 0xc014:
		return "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"
	case 0xc02f:
		return "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	case 0xc02b:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"
	case 0xc030:
		return "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
	case 0xc02c:
		return "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"
	}
	return fmt.Sprintf("Unknown [%x]", cs)
}

//==============================================================================

//MakeListener returns a new net.Listener for http.Request
func MakeListener(protocol string, addr string, conf *tls.Config) (net.Listener, error) {
	var l net.Listener
	var err error

	if conf == nil {
		l, err = tls.Listen(protocol, addr, conf)
	} else {
		l, err = net.Listen(protocol, addr)
	}

	if err != nil {
		return nil, err
	}

	return l, nil
}

//NewHTTPServer returns a new http.Server using the provided listener
func NewHTTPServer(l net.Listener, handle http.Handler, c *tls.Config) (*http.Server, net.Listener, error) {
	tl, ok := l.(*net.TCPListener)

	if !ok {
		return nil, nil, fmt.Errorf("Listener is not type *net.TCPListener")
	}

	s := &http.Server{
		Addr:           tl.Addr().String(),
		Handler:        handle,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
		TLSConfig:      c,
	}

	s.SetKeepAlivesEnabled(true)

	go s.Serve(tl)

	return s, tl, nil
}

// NewConn returns a tls.Conn object from the provided parameters.
func NewConn(protocol string, addr string) (net.Conn, error) {
	newConn, err := net.Dial(protocol, addr)
	if err != nil {
		return nil, err
	}

	return newConn, nil
}

// TLSConn returns a tls.Conn object from the provided parameters.
func TLSConn(protocol string, addr string, conf *tls.Config) (*tls.Conn, error) {
	newTls, err := tls.Dial(protocol, addr, conf)
	if err != nil {
		return nil, err
	}

	return newTls, nil
}

// TLSFromConn returns a new tls.Conn using the address and the certicates from
// the provided *tls.Conn.
func TLSFromConn(tl *tls.Conn, addr string) (*tls.Conn, error) {
	var conf *tls.Config

	if err := tl.Handshake(); err != nil {
		return nil, err
	}

	state := tl.ConnectionState()
	pool := x509.NewCertPool()

	for _, v := range state.PeerCertificates {
		pool.AddCert(v)
	}

	conf = &tls.Config{RootCAs: pool}
	newTls, err := tls.Dial("tcp", addr, conf)
	if err != nil {
		return nil, err
	}

	return newTls, nil
}

// ProxyHTTPRequest copies a http request from a src net.Conn connection to a
// destination net.Conn.
func ProxyHTTPRequest(src net.Conn, dest net.Conn) error {
	reader := bufio.NewReader(src)
	req, err := http.ReadRequest(reader)
	if err != nil {
		return err
	}

	if req == nil {
		return errors.New("No Request Read")
	}

	if err = req.Write(dest); err != nil {
		return err
	}

	resread := bufio.NewReader(dest)
	res, err := http.ReadResponse(resread, req)
	if err != nil {
		return err
	}

	if res != nil {
		return errors.New("No Response Read")
	}

	if err = res.Write(src); err != nil {
		return err
	}

	return nil
}

// hop headers, These are removed when sent to the backend
// http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html.
var hopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te", // canonicalized version of "TE"
	"Trailers",
	"Transfer-Encoding",
	"Upgrade",
}

// ConnToHTTP proxies a requests from a net.Conn to a destination request, writing
// the response to provided ResponseWriter.
func ConnToHTTP(src net.Conn, destReq *http.Request, destRes http.ResponseWriter) error {
	reader := bufio.NewReader(src)
	req, err := http.ReadRequest(reader)
	if err != nil {
		return err
	}

	destReq.Method = req.Method

	for key, val := range req.Header {
		destReq.Header.Set(key, strings.Join(val, ","))
	}

	for _, v := range hopHeaders {
		destReq.Header.Del(v)
	}

	ip, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return err
	}

	//add us to the proxy list or makeone
	hops, ok := req.Header["X-Forwarded-For"]
	if ok {
		ip = strings.Join(hops, ",") + "," + ip
	}

	destReq.Header.Set("X-Forwarded-For", ip)

	var buf bytes.Buffer
	if req.Body != nil {
		io.Copy(&buf, req.Body)
	}

	if buf.Len() > 0 {
		destReq.Body = ioutil.NopCloser(&buf)
		destReq.ContentLength = int64(buf.Len())
	}

	res, err := http.DefaultClient.Do(destReq)
	if err != nil {
		return err
	}

	for k, v := range res.Header {
		destRes.Header().Add(k, strings.Join(v, ","))
	}

	if err := res.Write(destRes); err != nil {
		return err
	}

	return nil
}

// HTTPToConn proxies a src Request to a net.Con connection and writes back
// the response to the src Response.
func HTTPToConn(srcReq *http.Request, srcRes http.ResponseWriter, dest net.Conn) error {
	if err := srcReq.Write(dest); err != nil {
		return err
	}

	resRead := bufio.NewReader(dest)
	res, err := http.ReadResponse(resRead, srcReq)
	if err != nil {
		return err
	}

	for key, val := range res.Header {
		srcRes.Header().Set(key, strings.Join(val, ","))
	}

	srcRes.WriteHeader(res.StatusCode)

	if err := res.Write(srcRes); err != nil {
		return err
	}

	return nil
}

//==============================================================================

func GetClustersFriends(clusterPort int, routes []*url.URL) ([]*url.URL, error) {
	var cleanRoutes []*url.URL
	cport := strconv.Itoa(clusterPort)

	selfIPs, err := GetInterfaceIPs()
	if err != nil {
		return nil, err
	}

	for _, r := range routes {
		host, port, err := net.SplitHostPort(r.Host)
		if err != nil {
			return nil, err
		}

		ips, err := GetURLIP(host)
		if err != nil {
			return nil, err
		}

		if cport == port && IsIPInList(selfIPs, ips) {
			continue
		}

		cleanRoutes = append(cleanRoutes, r)
	}

	return cleanRoutes, nil
}

func IsIPInList(list1 []net.IP, list2 []net.IP) bool {
	for _, ip1 := range list1 {
		for _, ip2 := range list2 {
			if ip1.Equal(ip2) {
				return true
			}
		}
	}
	return false
}

func GetURLIP(ipStr string) ([]net.IP, error) {
	ipList := []net.IP{}

	ip := net.ParseIP(ipStr)
	if ip != nil {
		ipList = append(ipList, ip)
		return ipList, nil
	}

	hostAddr, err := net.LookupHost(ipStr)
	if err != nil {
		return nil, err
	}

	for _, addr := range hostAddr {
		ip = net.ParseIP(addr)
		if ip != nil {
			ipList = append(ipList, ip)
		}
	}

	return ipList, nil
}

func GetInterfaceIPs() ([]net.IP, error) {
	var localIPs []net.IP

	interfaceAddr, err := net.InterfaceAddrs()
	if err != nil {
		return nil, errors.New("Error getting self referencing addr")
	}

	for i := 0; i < len(interfaceAddr); i++ {
		interfaceIP, _, _ := net.ParseCIDR(interfaceAddr[i].String())
		if net.ParseIP(interfaceIP.String()) != nil {
			localIPs = append(localIPs, interfaceIP)
		} else {
			err = errors.New("Error getting self referencing addr")
		}
	}

	if err != nil {
		return nil, err
	}

	return localIPs, nil
}

// Helper to move from float seconds to time.Duration
func secondsToDuration(seconds float64) time.Duration {
	ttl := seconds * float64(time.Second)
	return time.Duration(ttl)
}

// Ascii numbers 0-9
const (
	asciiZero = 48
	asciiNine = 57
)

// parseSize expects decimal positive numbers. We
// return -1 to signal error
func parseSize(d []byte) (n int) {
	if len(d) == 0 {
		return -1
	}
	for _, dec := range d {
		if dec < asciiZero || dec > asciiNine {
			return -1
		}
		n = n*10 + (int(dec) - asciiZero)
	}
	return n
}

// parseInt64 expects decimal positive numbers. We
// return -1 to signal error
func parseInt64(d []byte) (n int64) {
	if len(d) == 0 {
		return -1
	}
	for _, dec := range d {
		if dec < asciiZero || dec > asciiNine {
			return -1
		}
		n = n*10 + (int64(dec) - asciiZero)
	}
	return n
}
