package rabbitmq

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/streadway/amqp"

	logf "git.acronis.com/abc/go-libs/log"
)

var (
	rmAPI                      *rabbitManagementAPI
	rabbitAddress              string
	rabbitVhost                string
	user                       string
	pass                       string
	adminUser                  string
	adminPass                  string
	k8sNamespace               string
	ipv4s                      map[string]struct{}
	existingQueue              = "preconfigured-queue"
	logger                     logf.FieldLogger
	globalPub                  *Publisher
	globalPubName              string
	managementPluginResolution int = 1
)

type rabbitManagementAPI struct {
	c    http.Client
	base string
}

type rabbitConnection struct {
	Name     string `json:"name"`
	PeerHost string `json:"peer_host"`
	Channels int    `json:"channels"`
}

func TestMain(m *testing.M) {
	flag.StringVar(&rabbitAddress, "rabbit-addr", "", "rabbitmq address")
	flag.StringVar(&rabbitVhost, "rabbit-vhost", "", "rabbitmq vhost")
	flag.StringVar(&user, "user", "admin", "username for amqp client")
	flag.StringVar(&pass, "pass", "admin", "password for amqp client")
	flag.StringVar(&adminUser, "admin-user", "admin", "username for management api")
	flag.StringVar(&adminPass, "admin-pass", "admin", "password for management api")
	flag.StringVar(&k8sNamespace, "ns", "default", "k8s namespace")
	flag.IntVar(&managementPluginResolution, "rmp-res", 1, "rabbitmq management plugin resolution in seconds")
	flag.Parse()

	if rabbitAddress == "" {
		log.Println("rabbitmq address not set, skip the test suite")
		os.Exit(0)
	}
	ipv4s = getIP4()
	log.Println(amqpURL())
	log.Println(ipv4s)

	baseURI := "http://" + rabbitAddress + ":15672" + "/api"
	rmAPI = &rabbitManagementAPI{c: http.Client{}, base: baseURI}
	for i := 0; i < 10; i++ {
		conns, err := getActiveConnections(rabbitVhost)
		if err != nil {
			log.Fatal(err)
		}
		if len(conns) == 0 {
			break
		}
		for _, conn := range conns {
			if err := deleteActiveConnection(conn.Name); err != nil {
				log.Fatal(err)
			}
		}
		time.Sleep(time.Duration(managementPluginResolution) * time.Second)
	}
	logger, _ = logf.NewLogger(&logf.Config{Output: logf.OutputStdout, Level: logf.LevelDebug})
	if err := createGlobalPublisher(); err != nil {
		log.Fatal(err)
	}
	exitVal := m.Run()
	os.Exit(exitVal)
}

func createGlobalPublisher() error {
	var err error
	for i := 0; i < 10; i++ {
		globalPub, err = NewDefaultPublisher(amqpURL(), logger)
		if err != nil {
			log.Println("couldn't prepare publisher", err)
			log.Println("...try again in a second")
			time.Sleep(time.Second)
			continue
		}
		break
	}
	if err != nil {
		return fmt.Errorf("couldn't prepare publisher: %w", err)
	}
	conns, err := getActiveConnections(rabbitVhost)
	if err != nil {
		return fmt.Errorf("couldn't get active connections: %w", err)
	}
	if len(conns) != 1 {
		return fmt.Errorf("must be one active connection, got %v", len(conns))
	}
	globalPubName = conns[0].Name
	fmt.Println("global publisher connection", globalPubName)
	return nil
}

func amqpURL() *url.URL {
	u, _ := url.Parse(fmt.Sprintf("amqp://%v:%v@%v/%v", user, pass, rabbitAddress, url.PathEscape(rabbitVhost)))
	return u
}

func getIP4() map[string]struct{} {
	ip4s := make(map[string]struct{})
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Fatal("net.InterfaceAddrs failed", err)
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip4s[ipnet.IP.String()] = struct{}{}
			}
		}
	}
	return ip4s
}

func getActiveConnections(vhost string) ([]rabbitConnection, error) {
	time.Sleep(time.Duration(managementPluginResolution) * time.Second)
	var connections []rabbitConnection
	err := rmAPI.Get("/vhosts/"+url.PathEscape(vhost)+"/connections", &connections)
	if err != nil {
		return nil, err
	}
	var activeConns []rabbitConnection
	for _, c := range connections {
		if _, ok := ipv4s[c.PeerHost]; ok {
			if c.Name != globalPubName {
				activeConns = append(activeConns, c)
			}
		}
	}
	return activeConns, nil
}

func deleteActiveConnection(name string) error {
	return rmAPI.Delete("/connections/" + url.PathEscape(name))
}

func mustCreateVhost(t *testing.T, vhost, user string) {
	t.Helper()
	vhost = url.PathEscape(vhost)
	user = url.PathEscape(user)
	err := rmAPI.Put("/vhosts/"+vhost, "")
	require.NoError(t, err)
	err = rmAPI.Put(fmt.Sprintf("/permissions/%s/%s", vhost, user), `{"configure":".*","write":".*","read":".*"}`)
	require.NoError(t, err)
}

func mustDeleteVhost(t *testing.T, name string) {
	t.Helper()
	vhost := url.PathEscape(name)
	err := rmAPI.Delete("/vhosts/" + vhost)
	require.NoError(t, err)
}

func mustResetVhost(t *testing.T, vhost, user string) {
	t.Helper()
	mustDeleteVhost(t, vhost)
	mustCreateVhost(t, vhost, user)
}

func (c *rabbitManagementAPI) Get(uri string, data interface{}) error {
	log.Println("/GET " + c.base + uri)
	request, err := http.NewRequest("GET", c.base+uri, nil)
	if err != nil {
		return err
	}
	request.SetBasicAuth("admin", "admin")
	resp, err := c.c.Do(request)
	if err != nil {
		return err
	}
	return readResponseBody(data, resp)
}

func (c *rabbitManagementAPI) Put(uri, body string) error {
	log.Println("/PUT " + c.base + uri)
	request, err := http.NewRequest("PUT", c.base+uri, strings.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.SetBasicAuth("admin", "admin")
	resp, err := c.c.Do(request)
	if err != nil {
		return err
	}
	return readResponseBody(nil, resp)
}

func (c *rabbitManagementAPI) Delete(uri string) error {
	log.Println("/DELETE " + c.base + uri)
	request, err := http.NewRequest("DELETE", c.base+uri, nil)
	if err != nil {
		return err
	}
	request.SetBasicAuth("admin", "admin")
	resp, err := c.c.Do(request)
	if err != nil {
		return err
	}
	return readResponseBody(nil, resp)
}

func readResponseBody(data interface{}, response *http.Response) error {
	if response.StatusCode/100 != 2 {
		return fmt.Errorf("unexpected response %v from server", response.StatusCode)
	}
	if data == nil {
		return nil
	}
	defer response.Body.Close() //nolint: errcheck
	if err := json.NewDecoder(response.Body).Decode(data); err != nil {
		return err
	}
	return nil
}

func p(m string) *amqp.Publishing {
	return &amqp.Publishing{Body: []byte(m)}
}

func noAck(fn DeliveryFunc) DeliveryFunc {
	return func(d *amqp.Delivery) bool {
		ret := fn(d)
		_ = d.Acknowledger.Nack(d.DeliveryTag, false, true)
		return ret
	}
}

func once(fn DeliveryFunc) DeliveryFunc {
	return ntimes(1, fn)
}

func ntimes(n int, fn DeliveryFunc) DeliveryFunc {
	return func(d *amqp.Delivery) bool {
		res := fn(d)
		n--
		if n == 0 {
			return false
		}
		return res
	}
}

func expectDelivery(t *testing.T, message string) DeliveryFunc {
	return expect(t, message, false)
}

func expectRedelivery(t *testing.T, message string) DeliveryFunc {
	return expect(t, message, true)
}

func ignoreDelivery(t *testing.T, message string) DeliveryFunc {
	return func(d *amqp.Delivery) bool {
		require.Equal(t, message, string(d.Body))
		return true
	}
}

func expect(t *testing.T, message string, redelivered bool) DeliveryFunc {
	return func(d *amqp.Delivery) bool {
		require.Equal(t, message, string(d.Body))
		require.Equal(t, redelivered, d.Redelivered)
		return true
	}
}

func killConnection(t *testing.T, vhost string) {
	t.Helper()
	conns, err := getActiveConnections(vhost)
	require.NoError(t, err)
	fmt.Println(conns)
	require.Len(t, conns, 1, "must be one active connection")
	require.NoError(t, deleteActiveConnection(conns[0].Name))
	time.Sleep(time.Second)
}

func killAllConnections(t *testing.T, vhost string) {
	t.Helper()
	conns, err := getActiveConnections(vhost)
	require.NoError(t, err)
	fmt.Println(conns)
	for _, c := range conns {
		require.NoError(t, deleteActiveConnection(c.Name))
	}
	time.Sleep(time.Second)
}

func restartRabbit(t *testing.T, kubectl, namespace string) {
	t.Helper()
	cmd := exec.Command(kubectl, "delete", "pod", "--selector", "app=rabbitmq", "--namespace", namespace)
	fmt.Println(cmd.String())
	stdoutStderr, err := cmd.CombinedOutput()
	fmt.Printf("%s\n", stdoutStderr)
	require.NoError(t, err)
}
