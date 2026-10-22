"""Sandboxes must not be able to communicate with, or observe, each other.

Integration tests against a running SandboxManager (they create real sandboxes):

    kubectl -n webapp port-forward svc/sandbox-manager 8080:80
    SANDBOX_API_URL=http://localhost:8080 uv run pytest -m integration tests/test_isolation.py -v

Each "blocked" check has a positive control proving the probe itself works
(e.g. a sandbox can reach its own server), so a broken probe can't pass as
"isolated".
"""

import socket

import pytest

from sandbox_sdk import Sandbox

pytestmark = pytest.mark.integration

PORT = 8080
CONNECT_TIMEOUT = 3  # seconds; blocked traffic is dropped, so probes wait this long


def start_server(sb: Sandbox, secret: str) -> None:
    """Serve `secret` over HTTP on PORT and write it to /tmp/secret.

    busybox httpd daemonizes and detaches from the exec stream. Its docroot is
    named after the secret, so each sandbox's server process is identifiable in ps.
    """
    sb.exec(
        f"mkdir -p /tmp/www-{secret} && echo {secret} > /tmp/www-{secret}/index.html"
        f" && echo {secret} > /tmp/secret && httpd -p {PORT} -h /tmp/www-{secret}"
    ).check()


def fetch(sb: Sandbox, url: str) -> str:
    """HTTP GET from inside sb; returns the body, or '' if unreachable."""
    cmd = sb.exec(f"wget -T {CONNECT_TIMEOUT} -qO- {url}", timeout=CONNECT_TIMEOUT + 10)
    return cmd.stdout if cmd.ok else ""


def can_connect(sb: Sandbox, host: str, port: int) -> bool:
    """Whether a TCP connection from inside sb to host:port succeeds."""
    cmd = sb.exec(f"nc -w {CONNECT_TIMEOUT} {host} {port} </dev/null", timeout=CONNECT_TIMEOUT + 10)
    return cmd.exit_code == 0


def pod_ip(sb: Sandbox) -> str:
    return sb.exec("hostname -i").check().stdout.split()[0]


@pytest.fixture(scope="module")
def pair():
    """Two sandboxes, each running an HTTP server with its own secret."""
    a = Sandbox("busybox:1.36", command=["sleep", "infinity"], env={"SANDBOX_SECRET": "alpha-secret"}, ttl=600)
    b = Sandbox("busybox:1.36", command=["sleep", "infinity"], env={"SANDBOX_SECRET": "bravo-secret"}, ttl=600)
    with a, b:
        start_server(a, "alpha-secret")
        start_server(b, "bravo-secret")
        yield a, b
    # Leaving the with block deletes them, keeping their volumes for the
    # recovery window; free those now so test runs don't fill the quota.
    a.delete(purge=True)
    b.delete(purge=True)


def test_servers_work_locally(pair):
    """Control: each sandbox reaches its own server, so the probes are sound."""
    a, b = pair
    assert fetch(a, f"http://127.0.0.1:{PORT}/").strip() == "alpha-secret"
    assert fetch(b, f"http://127.0.0.1:{PORT}/").strip() == "bravo-secret"


@pytest.mark.parametrize("direction", ["a->b", "b->a"])
def test_no_network_path_between_sandboxes(pair, direction):
    src, dst = pair if direction == "a->b" else pair[::-1]
    dst_ip = pod_ip(dst)
    assert fetch(src, f"http://{dst_ip}:{PORT}/") == "", f"{direction}: read the other sandbox's server"
    assert not can_connect(src, dst_ip, PORT), f"{direction}: opened a TCP connection to the other sandbox"


def test_cannot_reach_control_plane(pair):
    """A sandbox could otherwise drive the (unauthenticated) API to reach other
    sandboxes, or attack the database or Kubernetes directly."""
    a, _ = pair
    targets = {
        "sandbox-manager API": ("sandbox-manager.webapp.svc.cluster.local", 80),
        "postgres": ("postgres.webapp.svc.cluster.local", 5432),
        "kubernetes API": ("kubernetes.default.svc.cluster.local", 443),
    }
    gateway = a.exec("ip route | awk '/default/ {print $3}'").check().stdout.strip()
    if gateway:
        targets["kubelet on the node"] = (gateway, 10250)
    reachable = [name for name, (host, port) in targets.items() if can_connect(a, host, port)]
    assert reachable == [], f"sandbox can reach: {reachable}"


def test_dns_still_resolves(pair):
    """Control for the control-plane test: names resolve, so the connections
    above failed because they're blocked, not because DNS is broken."""
    a, _ = pair
    out = a.exec("nslookup sandbox-manager.webapp.svc.cluster.local").stdout
    assert "Address" in out and "10.43." in out, out


def test_public_internet_egress_allowed(pair):
    """Isolation shouldn't cut sandboxes off from the internet (package installs etc.)."""
    try:
        socket.create_connection(("1.1.1.1", 443), timeout=CONNECT_TIMEOUT).close()
    except OSError:
        pytest.skip("this machine has no internet access")
    a, _ = pair
    assert can_connect(a, "1.1.1.1", 443)


def test_no_kubernetes_credentials(pair):
    for sb in pair:
        out = sb.exec("ls /var/run/secrets/kubernetes.io/serviceaccount 2>&1; true").stdout
        assert "token" not in out, f"{sb.name} has a service account token mounted"


def test_filesystems_are_separate(pair):
    a, b = pair
    assert a.exec("cat /tmp/secret").stdout.strip() == "alpha-secret"
    assert b.exec("cat /tmp/secret").stdout.strip() == "bravo-secret"
    a.exec("echo only-in-a > /tmp/a-only").check()
    assert not b.exec("test -e /tmp/a-only").ok


def test_processes_are_separate(pair):
    a, b = pair
    a_ps, b_ps = a.exec("ps").check().stdout, b.exec("ps").check().stdout
    assert "www-alpha-secret" in a_ps and "www-bravo-secret" in b_ps  # control: own server visible
    assert "www-bravo-secret" not in a_ps
    assert "www-alpha-secret" not in b_ps


def test_environment_is_separate(pair):
    a, b = pair
    assert a.exec("echo $SANDBOX_SECRET").stdout.strip() == "alpha-secret"
    assert b.exec("echo $SANDBOX_SECRET").stdout.strip() == "bravo-secret"
    assert "alpha-secret" not in b.exec("env").stdout
    assert "bravo-secret" not in a.exec("env").stdout


def test_executions_only_touch_their_own_sandbox(pair):
    a, b = pair
    ca = a.exec("hostname").check()
    cb = b.exec("hostname").check()
    assert ca.stdout.strip() == a.name and cb.stdout.strip() == b.name
    assert ca.sandbox_id == a.sandbox_id and cb.sandbox_id == b.sandbox_id

    a_history = {c.execution_id for c in a.commands()}
    b_history = {c.execution_id for c in b.commands()}
    assert ca.execution_id in a_history and cb.execution_id in b_history
    assert not a_history & b_history
