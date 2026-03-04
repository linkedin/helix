#!/usr/bin/env python3
"""
Fetch all Helix cluster configurations from ZK via the Helix REST 2.0 API.

Usage:
    python fetch_cluster_configs.py --namespace espresso --clusters ESPRESSO_STORAGE_LOR1 ESPRESSO_ROUTER_LOR1
    python fetch_cluster_configs.py --namespace shared --clusters Ambry-delivery --fabric prod-ltx1
    python fetch_cluster_configs.py --namespace espresso --list-clusters
    python fetch_cluster_configs.py --namespace espresso --clusters ESPRESSO_STORAGE_LOR1 --output /tmp/configs

Prerequisites:
    - mTLS identity certs (auto-detected from ULL, identity, or env vars)
      OR use --use-curli to delegate auth to curli
    - Network access to helix.namespaced.rest in the target fabric
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

FABRIC_DOMAINS = {
    "prod-lor1": "prod",
    "prod-ltx1": "prod",
    "prod-lva1": "prod",
    "ei-ltx1":   "stg",
    "ei4":       "int",
    "corp-lca1": "corp",
    "corp-ltx1": "corp",
    "corp-lva1": "corp",
}

KNOWN_NAMESPACES = [
    "espresso", "shared", "ambry", "venice", "venice-parent",
    "venice-sandbox", "seas", "vector", "tscp", "maestro",
    "tectonic",
]

REST_PORT_SSL = 12955
REST_PORT = 12954


def build_base_url(namespace: str, fabric: str, use_curli: bool = False) -> str:
    domain = FABRIC_DOMAINS.get(fabric)
    if not domain:
        raise ValueError(
            f"Unknown fabric '{fabric}'. Known fabrics: {list(FABRIC_DOMAINS)}"
        )
    if use_curli:
        return (
            f"https://helix.namespaced.rest.tag.{fabric}.atd.{domain}.linkedin.com"
            f":{REST_PORT_SSL}/admin/v2/namespaces/{namespace}"
        )
    return (
        f"https://helix.namespaced.rest.helix-rest.{fabric}"
        f".atd-ds.disco.linkedin.com:{REST_PORT_SSL}"
        f"/admin/v2/namespaces/{namespace}"
    )


def get_cert_pair() -> Tuple[str, str]:
    candidate_pairs = [
        (os.environ.get("HELIX_CERT", ""), os.environ.get("HELIX_KEY", "")),
        (os.path.expanduser("~/identity.cert"), os.path.expanduser("~/identity.key")),
        ("/export/content/li-certs/ull/ull-all.cert", "/export/content/li-certs/ull/ull-all.key"),
    ]

    for cert, key in candidate_pairs:
        if cert and key and os.path.isfile(cert) and os.path.isfile(key):
            log.info("Using certs: %s", cert)
            return cert, key

    raise FileNotFoundError(
        "mTLS certs not found. Searched:\n"
        "  - HELIX_CERT/HELIX_KEY env vars\n"
        "  - ~/identity.cert + ~/identity.key\n"
        "  - /export/content/li-certs/ull/ull-all.cert + .key\n"
        "Set HELIX_CERT and HELIX_KEY env vars to override."
    )


def create_session(cert_pair: Tuple[str, str]) -> requests.Session:
    session = requests.Session()
    session.cert = cert_pair
    ca_bundle = "/etc/lipki/ca-bundle.crt"
    session.verify = ca_bundle if os.path.isfile(ca_bundle) else True
    session.headers.update({"Accept": "application/json"})
    retry = Retry(total=0)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def curli_get(url: str, fabric: str = "", timeout: int = 60) -> Optional[Any]:
    """Fetch a URL using curli (handles mTLS/DataVault auth automatically).
    When fabric is set, runs via `eh` SSH proxy on a prod host."""
    try:
        if fabric:
            ssh_cmd = (
                f'curli --dv-auth SELF --dv-fast-access "{url}"'
            )
            cmd = ["eh", "-e", "%%%shel", "-f", fabric, "-c", ssh_cmd]
        else:
            cmd = ["curli", "--dv-auth", "SELF", "--dv-fast-access", url]

        log.debug("Running: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

        output = result.stdout.strip()
        if not output:
            log.warning("Empty response from curli for %s", url)
            return None

        json_start = output.find("{")
        json_start_arr = output.find("[")
        if json_start < 0:
            json_start = json_start_arr
        elif json_start_arr >= 0:
            json_start = min(json_start, json_start_arr)

        if json_start < 0:
            log.warning("No JSON in curli output for %s: %s", url, output[:200])
            return None

        return json.loads(output[json_start:])
    except subprocess.TimeoutExpired:
        log.warning("curli timed out for %s", url)
        return None
    except json.JSONDecodeError as e:
        log.warning("Invalid JSON from curli for %s: %s", url, e)
        return None
    except Exception as e:
        log.warning("curli request failed for %s: %s", url, e)
        return None


_CONNECT_FAILED = False


def api_get(session: Optional[requests.Session], url: str, timeout: int = 30,
            use_curli: bool = False, curli_fabric: str = "") -> Optional[Any]:
    global _CONNECT_FAILED
    if _CONNECT_FAILED:
        return None
    if use_curli:
        return curli_get(url, fabric=curli_fabric, timeout=timeout)
    try:
        resp = session.get(url, timeout=(5, timeout))
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            log.debug("Not found: %s", url)
            return None
        log.warning("HTTP error for %s: %s", url, e)
        return None
    except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout) as e:
        _CONNECT_FAILED = True
        log.error(
            "Cannot reach Helix REST endpoint. This usually means you're on a dev "
            "laptop without prod network access.\n"
            "  Options:\n"
            "    1. Open https://helix.prod.linkedin.com/ in your browser (seas namespace)\n"
            "    2. SSH to a prod host first, then run this script\n"
            "    3. Use --use-curli from a machine with prod access\n"
            "  Error: %s", e
        )
        return None
    except Exception as e:
        log.warning("Request failed for %s: %s", url, e)
        return None


class HelixClient:
    """Thin wrapper: picks requests.Session or curli depending on mode."""

    def __init__(self, base_url: str, session: Optional[requests.Session] = None,
                 use_curli: bool = False, curli_fabric: str = ""):
        self.base_url = base_url
        self._session = session
        self._use_curli = use_curli
        self._curli_fabric = curli_fabric

    def get(self, url: str, timeout: int = 60) -> Optional[Any]:
        return api_get(self._session, url, timeout=timeout,
                       use_curli=self._use_curli, curli_fabric=self._curli_fabric)


def list_clusters(client: HelixClient) -> List[str]:
    data = client.get(f"{client.base_url}/clusters")
    if data and "clusters" in data:
        return data["clusters"]
    return []


def fetch_cluster_config(client: HelixClient, cluster: str) -> Dict:
    return client.get(f"{client.base_url}/clusters/{cluster}/configs") or {}


def fetch_cloud_config(client: HelixClient, cluster: str) -> Optional[Dict]:
    return client.get(f"{client.base_url}/clusters/{cluster}/cloudconfig")


def fetch_resource_list(client: HelixClient, cluster: str) -> List[str]:
    data = client.get(f"{client.base_url}/clusters/{cluster}/resources")
    if data and "idealStates" in data:
        return data["idealStates"]
    if data and "resources" in data:
        return data["resources"]
    return list(data.keys()) if isinstance(data, dict) else []


def fetch_resource_config(client: HelixClient, cluster: str, resource: str) -> Optional[Dict]:
    return client.get(f"{client.base_url}/clusters/{cluster}/resources/{resource}/configs")


def fetch_instance_list(client: HelixClient, cluster: str) -> List[str]:
    data = client.get(f"{client.base_url}/clusters/{cluster}/instances")
    if data and "instances" in data:
        return data["instances"]
    if data and "instanceInfo" in data:
        return [i.get("id", i) for i in data["instanceInfo"]]
    return list(data.keys()) if isinstance(data, dict) else []


def fetch_instance_config(client: HelixClient, cluster: str, instance: str) -> Optional[Dict]:
    return client.get(f"{client.base_url}/clusters/{cluster}/instances/{instance}/configs")


def fetch_state_model_defs(client: HelixClient, cluster: str) -> Optional[Dict]:
    return client.get(f"{client.base_url}/clusters/{cluster}/statemodeldefs")


def fetch_controller_info(client: HelixClient, cluster: str) -> Optional[Dict]:
    return client.get(f"{client.base_url}/clusters/{cluster}/controller")


def fetch_all_configs_for_cluster(
    client: HelixClient,
    cluster: str,
    include_instance_configs: bool = True,
    include_resource_configs: bool = True,
    max_workers: int = 8,
) -> Dict[str, Any]:
    """Fetch all configuration types for a single cluster."""
    log.info("Fetching configs for cluster: %s", cluster)
    result: Dict[str, Any] = {"cluster_name": cluster}

    # Cluster-level configs (parallel)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(fetch_cluster_config, client, cluster): "cluster_config",
            pool.submit(fetch_cloud_config, client, cluster): "cloud_config",
            pool.submit(fetch_state_model_defs, client, cluster): "state_model_defs",
            pool.submit(fetch_controller_info, client, cluster): "controller",
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                val = future.result()
                if val is not None:
                    result[key] = val
            except Exception as e:
                log.warning("Failed to fetch %s for %s: %s", key, cluster, e)

    # Resource configs
    if include_resource_configs:
        resources = fetch_resource_list(client, cluster)
        log.info("  Found %d resources in %s", len(resources), cluster)
        result["resources"] = {"count": len(resources), "names": resources, "configs": {}}

        if resources:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(fetch_resource_config, client, cluster, r): r
                    for r in resources
                }
                for future in as_completed(futures):
                    rname = futures[future]
                    try:
                        cfg = future.result()
                        if cfg:
                            result["resources"]["configs"][rname] = cfg
                    except Exception as e:
                        log.warning("  Failed to fetch resource config %s: %s", rname, e)

    # Instance configs
    if include_instance_configs:
        instances = fetch_instance_list(client, cluster)
        log.info("  Found %d instances in %s", len(instances), cluster)
        result["instances"] = {"count": len(instances), "names": instances, "configs": {}}

        if instances:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(fetch_instance_config, client, cluster, i): i
                    for i in instances
                }
                for future in as_completed(futures):
                    iname = futures[future]
                    try:
                        cfg = future.result()
                        if cfg:
                            result["instances"]["configs"][iname] = cfg
                    except Exception as e:
                        log.warning("  Failed to fetch instance config %s: %s", iname, e)

    return result


def save_output(data: Dict, output_dir: str, cluster: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{cluster}_configs.json"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)
    return filepath


def print_summary(all_configs: List[Dict]):
    print("\n" + "=" * 70)
    print("CONFIGURATION SUMMARY")
    print("=" * 70)
    for cfg in all_configs:
        cluster = cfg.get("cluster_name", "unknown")
        print(f"\n--- {cluster} ---")

        cc = cfg.get("cluster_config", {})
        simple = cc.get("simpleFields", {})
        if simple:
            print(f"  Cluster simpleFields ({len(simple)} keys):")
            for k, v in sorted(simple.items())[:15]:
                print(f"    {k}: {v}")
            if len(simple) > 15:
                print(f"    ... and {len(simple) - 15} more")

        ctrl = cfg.get("controller", {})
        if ctrl:
            leader = ctrl.get("LEADER", ctrl.get("leader", "N/A"))
            print(f"  Controller leader: {leader}")

        res = cfg.get("resources", {})
        if res:
            print(f"  Resources: {res.get('count', 0)}")

        inst = cfg.get("instances", {})
        if inst:
            print(f"  Instances: {inst.get('count', 0)}")

        cloud = cfg.get("cloud_config")
        if cloud:
            print(f"  Cloud config: enabled={cloud.get('simpleFields', {}).get('CLOUD_ENABLED', 'N/A')}")

    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Helix cluster configurations from ZK via REST API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --namespace espresso --list-clusters
  %(prog)s --namespace espresso --clusters ESPRESSO_STORAGE_LOR1
  %(prog)s --namespace shared --clusters Ambry-delivery --skip-instances
  %(prog)s --namespace espresso --clusters ESPRESSO_STORAGE_LOR1 -o /tmp/helix_configs
        """,
    )
    parser.add_argument(
        "--namespace", "-n", required=True,
        help=f"Helix REST namespace. Known: {', '.join(KNOWN_NAMESPACES)}",
    )
    parser.add_argument(
        "--clusters", "-c", nargs="+",
        help="Cluster names to fetch configs for",
    )
    parser.add_argument(
        "--fabric", "-f", default="prod-lor1",
        help="Target fabric (default: prod-lor1)",
    )
    parser.add_argument(
        "--list-clusters", "-l", action="store_true",
        help="List all clusters in the namespace and exit",
    )
    parser.add_argument(
        "--output", "-o",
        help="Output directory for JSON files (default: print to stdout)",
    )
    parser.add_argument(
        "--skip-instances", action="store_true",
        help="Skip fetching individual instance configs (faster for large clusters)",
    )
    parser.add_argument(
        "--skip-resources", action="store_true",
        help="Skip fetching individual resource configs",
    )
    parser.add_argument(
        "--workers", "-w", type=int, default=8,
        help="Max concurrent requests per cluster (default: 8)",
    )
    parser.add_argument(
        "--summary", "-s", action="store_true",
        help="Print a human-readable summary",
    )
    parser.add_argument(
        "--use-curli", action="store_true",
        help="Use curli via SSH to a prod host (recommended from dev laptops)",
    )
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    base_url = build_base_url(args.namespace, args.fabric, use_curli=args.use_curli)
    log.info("Base URL: %s", base_url)

    session = None
    if not args.use_curli:
        cert_pair = get_cert_pair()
        session = create_session(cert_pair)

        import socket
        from urllib.parse import urlparse
        parsed = urlparse(base_url)
        host, port = parsed.hostname, parsed.port or 443
        log.info("Testing connectivity to %s:%s ...", host, port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect((host, port))
            sock.close()
            log.info("Connected OK.")
        except (socket.timeout, OSError) as e:
            sock.close()
            log.error(
                "Cannot reach %s:%s (%s).\n\n"
                "  You are likely on a dev laptop without prod network access.\n"
                "  Options:\n"
                "    1. Open https://helix.prod.linkedin.com/ in your browser\n"
                "       → select namespace '%s', fabric '%s'\n"
                "    2. SSH to a prod shell host first, then run this script\n"
                "    3. Run from a machine inside the prod network\n",
                host, port, e, args.namespace, args.fabric,
            )
            sys.exit(1)

    client = HelixClient(base_url, session=session, use_curli=args.use_curli,
                         curli_fabric=args.fabric if args.use_curli else "")

    if args.list_clusters:
        clusters = list_clusters(client)
        if clusters:
            print(f"\nClusters in namespace '{args.namespace}' ({args.fabric}):")
            print(f"  Total: {len(clusters)}\n")
            for c in sorted(clusters):
                print(f"  {c}")
        else:
            print(f"No clusters found in namespace '{args.namespace}' ({args.fabric})")
        return

    if not args.clusters:
        parser.error("--clusters is required (or use --list-clusters to discover them)")

    all_configs = []
    for cluster in args.clusters:
        config = fetch_all_configs_for_cluster(
            client=client,
            cluster=cluster,
            include_instance_configs=not args.skip_instances,
            include_resource_configs=not args.skip_resources,
            max_workers=args.workers,
        )
        all_configs.append(config)

        if args.output:
            path = save_output(config, args.output, cluster)
            log.info("Saved %s configs to %s", cluster, path)

    if args.summary:
        print_summary(all_configs)

    if not args.output:
        print(json.dumps(all_configs if len(all_configs) > 1 else all_configs[0], indent=2, default=str))

    log.info("Done. Fetched configs for %d cluster(s).", len(all_configs))


if __name__ == "__main__":
    main()
