package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// agentClient talks to the sandbox-agent DaemonSet (agent/main.go): one
// agent per node, which does the file I/O for the sandbox pods on that node.
type agentClient struct {
	core      kubernetes.Interface
	namespace string // AGENT_NAMESPACE, default webapp
	port      string // AGENT_PORT, default 8090
	token     string // AGENT_TOKEN, shared with the agents
	http      *http.Client

	mu    sync.Mutex
	nodes map[string]agentAddr // node name → agent
}

type agentAddr struct {
	base string
	at   time.Time
}

const agentAddrTTL = 30 * time.Second

func newAgentClient(core kubernetes.Interface, namespace, port, token string) *agentClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 5 * time.Second}).DialContext
	return &agentClient{
		core: core, namespace: namespace, port: port, token: token,
		// No overall timeout: transfers stream for as long as they take.
		http:  &http.Client{Transport: transport},
		nodes: map[string]agentAddr{},
	}
}

// base returns the URL of the ready agent on node.
func (a *agentClient) base(ctx context.Context, node string) (string, error) {
	a.mu.Lock()
	addr, ok := a.nodes[node]
	a.mu.Unlock()
	if ok && time.Since(addr.at) < agentAddrTTL {
		return addr.base, nil
	}
	pods, err := a.core.CoreV1().Pods(a.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=sandbox-agent", FieldSelector: "spec.nodeName=" + node,
	})
	if err != nil {
		return "", err
	}
	for _, p := range pods.Items {
		if p.DeletionTimestamp == nil && p.Status.PodIP != "" && podReady(&p) {
			base := "http://" + net.JoinHostPort(p.Status.PodIP, a.port)
			a.mu.Lock()
			a.nodes[node] = agentAddr{base: base, at: time.Now()}
			a.mu.Unlock()
			return base, nil
		}
	}
	return "", fmt.Errorf("no ready sandbox-agent on node %s", node)
}

func (a *agentClient) forget(node string) {
	a.mu.Lock()
	delete(a.nodes, node)
	a.mu.Unlock()
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// do sends a request about pod to its node's agent: path is relative to
// /pods/<uid>, size the body's length (-1 if unknown). The caller closes the
// response body.
func (a *agentClient) do(ctx context.Context, pod *corev1.Pod, method, path, query string, body io.Reader, size int64) (*http.Response, error) {
	base, err := a.base(ctx, pod.Spec.NodeName)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	u.Path = "/pods/" + string(pod.UID) + path
	u.RawQuery = query
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = size
	if body == nil {
		req.ContentLength = 0
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	resp, err := a.http.Do(req)
	if err != nil {
		a.forget(pod.Spec.NodeName) // maybe a replaced agent pod
		return nil, err
	}
	return resp, nil
}

// call is do for JSON requests and responses (in and out may be nil).
// Non-2xx responses come back as errors.
func (a *agentClient) call(ctx context.Context, pod *corev1.Pod, method, path string, in, out any) error {
	var body io.Reader
	size := int64(0)
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body, size = bytes.NewReader(b), int64(len(b))
	}
	resp, err := a.do(ctx, pod, method, path, "", body, size)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("agent %s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(msg)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
