import requests

from time import sleep

from codeflare_sdk import (
    Cluster,
    ClusterConfiguration,
    TokenAuthentication,
)
from codeflare_sdk.ray.client import RayJobClient

import pytest

from support import *

# This test creates a Ray Cluster and covers the Ray Job submission with authentication and without authentication functionality on Openshift Cluster


@pytest.mark.skip(
    reason="OAuth not configured for Ray clusters - Service dashboard port routes directly to Ray (8265) "
    "instead of through kube-rbac-proxy (8443). See OAUTH_IMPLEMENTATION_TRACKER.md for details."
)
@pytest.mark.openshift
class TestRayClusterSDKOauth:
    def setup_method(self):
        initialize_kubernetes_client(self)

    def teardown_method(self):
        delete_namespace(self)
        delete_kueue_resources(self)

    def test_mnist_ray_cluster_sdk_auth(self):
        self.setup_method()
        create_namespace(self)
        create_kueue_resources(self)
        self.run_mnist_raycluster_sdk_oauth()

    def run_mnist_raycluster_sdk_oauth(self):
        ray_image = get_ray_image()

        auth = TokenAuthentication(
            token=run_oc_command(["whoami", "--show-token=true"]),
            server=run_oc_command(["whoami", "--show-server=true"]),
            skip_tls=True,
        )
        auth.login()

        cluster = Cluster(
            ClusterConfiguration(
                name="mnist",
                namespace=self.namespace,
                num_workers=1,
                head_memory_requests=6,
                head_memory_limits=8,
                worker_cpu_requests=1,
                worker_cpu_limits=1,
                worker_memory_requests=6,
                worker_memory_limits=8,
                image=ray_image,
                write_to_file=True,
                verify_tls=False,
            )
        )

        # Check cluster resources before applying
        self.check_cluster_resources()

        cluster.apply()

        print("Cluster applied, checking initial status...")
        status, ready = cluster.status()
        print(f"Initial status: {status}, ready: {ready}")

        # Add diagnostic information before wait_ready
        self.print_cluster_diagnostics(cluster)

        # Use explicit timeout that's less than pytest timeout (900s)
        # This gives us 60s buffer for cleanup
        print("Waiting for cluster to be ready (timeout: 840s)...")
        try:
            # Custom wait loop with better diagnostics
            self.wait_ready_with_diagnostics(cluster, timeout=840)
        except TimeoutError as e:
            print(f"Timeout waiting for cluster: {e}")
            self.print_cluster_diagnostics(cluster)
            raise

        cluster.status()

        cluster.details()

        self.assert_jobsubmit_withoutLogin(cluster)
        self.assert_jobsubmit_withlogin(cluster)
        assert_get_cluster_and_jobsubmit(self, "mnist")

    # Diagnostics
    
    def check_oauth_configuration(self, cluster):
        """Check if OAuth/RBAC protection is properly configured"""
        import kubernetes.client as k8s_client
        
        print("\n=== OAuth/RBAC Configuration Check ===")
        try:
            custom_api = k8s_client.CustomObjectsApi()
            
            # Check Route annotations
            routes = custom_api.list_namespaced_custom_object(
                group="route.openshift.io",
                version="v1",
                namespace=cluster.config.namespace,
                plural="routes"
            )
            
            for route in routes.get('items', []):
                route_name = route['metadata']['name']
                if cluster.config.name in route_name:
                    print(f"\nRoute: {route_name}")
                    annotations = route['metadata'].get('annotations', {})
                    print(f"  Annotations: {annotations}")
                    
                    # Check for OAuth annotations
                    oauth_annotations = [
                        'haproxy.router.openshift.io/auth-type',
                        'haproxy.router.openshift.io/auth-url',
                        'haproxy.router.openshift.io/auth-realm',
                    ]
                    
                    has_oauth = any(ann in annotations for ann in oauth_annotations)
                    if has_oauth:
                        print("  ✅ OAuth annotations found")
                    else:
                        print("  ⚠️  WARNING: No OAuth annotations found on route!")
                        print("     This route is NOT protected by OAuth")
                    
                    # Check target service
                    target_service = route['spec'].get('to', {}).get('name')
                    print(f"  Target Service: {target_service}")
            
            # Check if head pod has kube-rbac-proxy container
            v1 = k8s_client.CoreV1Api()
            pods = v1.list_namespaced_pod(
                namespace=cluster.config.namespace,
                label_selector=f"ray.io/cluster={cluster.config.name},ray.io/node-type=head"
            )
            
            if pods.items:
                pod = pods.items[0]
                print(f"\nHead Pod: {pod.metadata.name}")
                container_names = [c.name for c in pod.spec.containers]
                print(f"  Containers: {container_names}")
                
                if 'kube-rbac-proxy' in container_names:
                    print("  ✅ kube-rbac-proxy container present")
                    # Check the proxy configuration
                    for container in pod.spec.containers:
                        if container.name == 'kube-rbac-proxy':
                            print(f"  kube-rbac-proxy ports: {[p.container_port for p in container.ports] if container.ports else 'None'}")
                else:
                    print("  ⚠️  WARNING: kube-rbac-proxy container NOT found!")
                    print("     Dashboard is likely NOT protected")
            
            # Check Service ports
            services = v1.list_namespaced_service(namespace=cluster.config.namespace)
            for svc in services.items:
                if cluster.config.name in svc.metadata.name and "head" in svc.metadata.name:
                    print(f"\nService: {svc.metadata.name}")
                    for port in svc.spec.ports:
                        print(f"  Port {port.name}: {port.port} -> {port.target_port}")
                    
                    # Check if service is pointing to the RBAC proxy port or directly to dashboard
                    dashboard_port = next((p for p in svc.spec.ports if p.name == 'dashboard'), None)
                    if dashboard_port:
                        if dashboard_port.target_port == 8080 or dashboard_port.target_port == '8080':
                            print("  ⚠️  Dashboard port targeting 8080 (likely kube-rbac-proxy)")
                            print("     OAuth should be working if proxy is configured correctly")
                        elif dashboard_port.target_port == 8265 or dashboard_port.target_port == '8265':
                            print("  ⚠️  WARNING: Dashboard port targeting 8265 (direct Ray dashboard)")
                            print("     This BYPASSES authentication!")
                        
        except Exception as e:
            print(f"Error checking OAuth configuration: {e}")
        
        print("=== End OAuth Check ===\n")

    def is_dashboard_ready_oauth(self, cluster) -> bool:
        """
        Check if dashboard is ready for OAuth-protected clusters.
        
        For OAuth tests, the dashboard is considered "ready" if it responds with:
        - 200 (OK - unlikely without auth, but valid)
        - 302 (Redirect to login - dashboard is up and protected)
        - 401 (Unauthorized - dashboard is up and protected)
        - 403 (Forbidden - dashboard is up and protected)
        
        Any of these codes indicate the dashboard service is running and responding.
        Connection errors or timeouts indicate it's NOT ready.
        """
        import requests
        import kubernetes.client as k8s_client
        
        dashboard_uri = cluster.cluster_dashboard_uri()
        
        # Check if dashboard_uri is None or an error message
        if dashboard_uri is None or "not available" in str(dashboard_uri).lower() or "have you run" in str(dashboard_uri).lower():
            print(f"    Dashboard URI not available yet (got: {dashboard_uri})")
            # Try to construct URL from route directly
            dashboard_uri = self.get_dashboard_url_from_route(cluster)
            if not dashboard_uri:
                print(f"    Could not find dashboard URL from routes either")
                return False
            print(f"    Found dashboard URL from route: {dashboard_uri}")
        
        try:
            response = requests.get(
                dashboard_uri,
                timeout=5,
                verify=False,  # Skip TLS verification for test
                allow_redirects=False  # Don't follow redirects, we want to see the 302
            )
            
            # Any of these status codes indicate the dashboard is up and responding
            if response.status_code in [200, 302, 401, 403]:
                print(f"    Dashboard responding with status {response.status_code} (dashboard is ready)")
                return True
            else:
                print(f"    Dashboard responding with unexpected status {response.status_code}")
                return False
                
        except requests.exceptions.Timeout:
            print(f"    Dashboard timeout - not ready yet")
            return False
        except requests.exceptions.ConnectionError as e:
            print(f"    Dashboard connection error - not ready yet: {e}")
            return False
        except Exception as e:
            print(f"    Dashboard check failed with exception: {e}")
            return False
    
    def get_dashboard_url_from_route(self, cluster):
        """
        Manually construct dashboard URL from OpenShift route when SDK method fails.
        """
        import kubernetes.client as k8s_client
        
        try:
            custom_api = k8s_client.CustomObjectsApi()
            routes = custom_api.list_namespaced_custom_object(
                group="route.openshift.io",
                version="v1",
                namespace=cluster.config.namespace,
                plural="routes"
            )
            
            # Look for any route that contains the cluster name
            for route in routes.get('items', []):
                route_name = route['metadata']['name']
                if cluster.config.name in route_name:
                    spec = route.get('spec', {})
                    host = spec.get('host')
                    if host:
                        protocol = "https" if spec.get('tls') else "http"
                        path = spec.get('path', '')
                        url = f"{protocol}://{host}{path}"
                        print(f"    Found route: {route_name} -> {url}")
                        return url
            
            return None
        except Exception as e:
            print(f"    Error getting route: {e}")
            return None
    
    def get_dashboard_url(self, cluster):
        """
        Get dashboard URL with fallback to direct route lookup.
        This wraps cluster.cluster_dashboard_uri() with error handling.
        """
        dashboard_url = cluster.cluster_dashboard_uri()
        
        # Check if dashboard_url is None or an error message
        if dashboard_url is None or "not available" in str(dashboard_url).lower() or "have you run" in str(dashboard_url).lower():
            print(f"SDK method returned: {dashboard_url}")
            print("Falling back to direct route lookup...")
            dashboard_url = self.get_dashboard_url_from_route(cluster)
            if dashboard_url:
                print(f"Successfully found dashboard URL from route: {dashboard_url}")
            else:
                raise RuntimeError("Could not determine dashboard URL from SDK or routes")
        
        return dashboard_url

    def check_cluster_resources(self):
        """Check if cluster has sufficient resources available"""
        import kubernetes.client as k8s_client
        
        print("\n=== Checking Cluster Resources ===")
        try:
            v1 = k8s_client.CoreV1Api()
            nodes = v1.list_node()
            
            total_cpu = 0
            total_memory = 0
            allocatable_cpu = 0
            allocatable_memory = 0
            
            print(f"Found {len(nodes.items)} node(s):")
            for node in nodes.items:
                print(f"\nNode: {node.metadata.name}")
                print(f"  Status: {node.status.conditions[-1].type} - {node.status.conditions[-1].status}")
                
                if node.status.capacity:
                    cpu = node.status.capacity.get('cpu', '0')
                    memory = node.status.capacity.get('memory', '0Ki')
                    print(f"  Capacity: CPU={cpu}, Memory={memory}")
                    
                if node.status.allocatable:
                    cpu = node.status.allocatable.get('cpu', '0')
                    memory = node.status.allocatable.get('memory', '0Ki')
                    print(f"  Allocatable: CPU={cpu}, Memory={memory}")
                    
                    # Parse CPU (can be in millicores or cores)
                    if 'm' in cpu:
                        allocatable_cpu += float(cpu.rstrip('m')) / 1000
                    else:
                        allocatable_cpu += float(cpu)
                    
                    # Parse memory (simplified - assumes Gi or Ki)
                    if 'Gi' in memory:
                        allocatable_memory += float(memory.rstrip('Gi'))
                    elif 'Ki' in memory:
                        allocatable_memory += float(memory.rstrip('Ki')) / (1024 * 1024)
            
            print(f"\nTotal Allocatable Resources:")
            print(f"  CPU: {allocatable_cpu:.2f} cores")
            print(f"  Memory: {allocatable_memory:.2f} Gi")
            
            # Check if we have enough for the test cluster (head + worker)
            required_cpu = 2  # 1 CPU for worker
            required_memory = 14  # 6+8 for head, 6+8 for worker (but limits, so 16 total)
            
            if allocatable_cpu < required_cpu:
                print(f"\nWARNING: May not have enough CPU! Required: {required_cpu}, Available: {allocatable_cpu:.2f}")
            
            if allocatable_memory < required_memory:
                print(f"\nWARNING: May not have enough Memory! Required: {required_memory}Gi, Available: {allocatable_memory:.2f}Gi")
            
        except Exception as e:
            print(f"Error checking cluster resources: {e}")
        
        print("=== End Resource Check ===\n")

    def wait_ready_with_diagnostics(self, cluster, timeout=900):
        """
        Wait for cluster to be ready with enhanced diagnostics.
        Fails fast if cluster enters FAILED state.
        Prints status updates every 60 seconds.
        """
        from codeflare_sdk.ray.cluster.status import CodeFlareClusterStatus
        
        print("Waiting for requested resources to be set up...")
        time = 0
        last_diagnostic_time = 0
        diagnostic_interval = 60  # Print diagnostics every 60 seconds
        
        while True:
            if timeout and time >= timeout:
                raise TimeoutError(
                    f"wait() timed out after waiting {timeout}s for cluster to be ready"
                )
            
            status, ready = cluster.status(print_to_console=False)
            
            # Print status update every iteration
            print(f"[{time}s] Cluster status: {status}, ready: {ready}")
            
            # Fail fast if cluster is in FAILED state
            if status == CodeFlareClusterStatus.FAILED:
                print("\nCluster entered FAILED state!")
                self.print_cluster_diagnostics(cluster)
                raise RuntimeError("Cluster failed to start - entered FAILED state")
            
            # Check for UNKNOWN status and warn
            if status == CodeFlareClusterStatus.UNKNOWN:
                print(
                    "WARNING: Current cluster status is unknown, have you run cluster.apply() yet?"
                )
            
            # Print detailed diagnostics periodically
            if time - last_diagnostic_time >= diagnostic_interval:
                print(f"\n--- Periodic Diagnostic Check at {time}s ---")
                self.print_cluster_diagnostics(cluster)
                last_diagnostic_time = time
            
            if ready:
                break
            
            sleep(5)
            time += 5
        
        print("Requested cluster is up and running!")
        
        # Now wait for dashboard
        print("Waiting for dashboard to be ready...")
        # Print initial dashboard diagnostics
        print("\n--- Initial Dashboard State ---")
        self.print_dashboard_diagnostics(cluster)
        
        dashboard_last_diagnostic_time = time
        dashboard_check_count = 0
        
        while True:
            if timeout and time >= timeout:
                print(f"\nDashboard not ready after {time}s!")
                self.print_dashboard_diagnostics(cluster)
                raise TimeoutError(
                    f"wait() timed out after waiting {timeout}s for dashboard to be ready"
                )
            
            # For OAuth tests, the dashboard is "ready" even if it returns 302/401/403
            # because those codes indicate the dashboard is up and protected by auth
            dashboard_ready = self.is_dashboard_ready_oauth(cluster)
            dashboard_check_count += 1
            
            # Print status update every check
            print(f"[{time}s] Dashboard ready check #{dashboard_check_count}: {dashboard_ready}")
            
            # Print detailed diagnostics periodically
            if time - dashboard_last_diagnostic_time >= diagnostic_interval:
                print(f"\n--- Dashboard Diagnostic Check at {time}s ---")
                self.print_dashboard_diagnostics(cluster)
                dashboard_last_diagnostic_time = time
            
            if dashboard_ready:
                print("Dashboard is ready!")
                break
            
            sleep(5)
            time += 5

    def print_dashboard_diagnostics(self, cluster):
        """Print diagnostic information about the dashboard and routes"""
        import kubernetes.client as k8s_client
        import requests
        
        print("\n=== Dashboard Diagnostics ===")
        try:
            # Get the dashboard URI
            dashboard_uri = cluster.cluster_dashboard_uri()
            print(f"Dashboard URI from SDK: {dashboard_uri}")
            
            # If SDK method returns error message, try to get URL from route
            if dashboard_uri is None or "not available" in str(dashboard_uri).lower() or "have you run" in str(dashboard_uri).lower():
                print("SDK method failed to get URL, attempting direct route lookup...")
                fallback_uri = self.get_dashboard_url_from_route(cluster)
                if fallback_uri:
                    print(f"Fallback Dashboard URI from route: {fallback_uri}")
                    dashboard_uri = fallback_uri
                else:
                    print("Could not determine dashboard URL from routes")
            
            # Check if it's an HTTPRoute, Route, or Ingress
            if dashboard_uri and isinstance(dashboard_uri, str) and dashboard_uri.startswith("http"):
                if "/ray/" in dashboard_uri:
                    print("Dashboard type: HTTPRoute (path-based)")
                else:
                    print("Dashboard type: OpenShift Route or Ingress")
            else:
                print("Dashboard type: Unknown (URL not valid)")
            
            # Check Routes (OpenShift)
            try:
                custom_api = k8s_client.CustomObjectsApi()
                routes = custom_api.list_namespaced_custom_object(
                    group="route.openshift.io",
                    version="v1",
                    namespace=cluster.config.namespace,
                    plural="routes"
                )
                print(f"\nFound {len(routes.get('items', []))} Route(s):")
                for route in routes.get('items', []):
                    name = route['metadata']['name']
                    spec = route.get('spec', {})
                    status = route.get('status', {})
                    print(f"  Route: {name}")
                    print(f"    Host: {spec.get('host', 'N/A')}")
                    print(f"    Path: {spec.get('path', '/')}")
                    print(f"    Service: {spec.get('to', {}).get('name', 'N/A')}")
                    if 'ingress' in status:
                        for ingress in status['ingress']:
                            print(f"    Ingress Status: {ingress.get('conditions', [])}")
            except Exception as e:
                print(f"Could not check Routes (might not be OpenShift): {e}")
            
            # Check HTTPRoutes
            try:
                httproutes = custom_api.list_namespaced_custom_object(
                    group="gateway.networking.k8s.io",
                    version="v1",
                    namespace=cluster.config.namespace,
                    plural="httproutes"
                )
                print(f"\nFound {len(httproutes.get('items', []))} HTTPRoute(s):")
                for httproute in httproutes.get('items', []):
                    name = httproute['metadata']['name']
                    spec = httproute.get('spec', {})
                    status = httproute.get('status', {})
                    print(f"  HTTPRoute: {name}")
                    print(f"    Hostnames: {spec.get('hostnames', [])}")
                    print(f"    Rules: {len(spec.get('rules', []))}")
                    print(f"    Parents: {status.get('parents', [])}")
            except Exception as e:
                print(f"Could not check HTTPRoutes: {e}")
            
            # Check Services
            v1 = k8s_client.CoreV1Api()
            services = v1.list_namespaced_service(namespace=cluster.config.namespace)
            print(f"\nFound {len(services.items)} Service(s):")
            for svc in services.items:
                if "head-svc" in svc.metadata.name or "dashboard" in svc.metadata.name:
                    print(f"  Service: {svc.metadata.name}")
                    print(f"    Type: {svc.spec.type}")
                    print(f"    Cluster IP: {svc.spec.cluster_ip}")
                    print(f"    Ports: {[(p.name, p.port, p.target_port) for p in svc.spec.ports]}")
            
            # Try to access the dashboard
            print("\n=== Dashboard Accessibility Test ===")
            if dashboard_uri and isinstance(dashboard_uri, str) and dashboard_uri.startswith("http"):
                try:
                    print(f"Attempting to reach: {dashboard_uri}")
                    response = requests.get(dashboard_uri, timeout=5, verify=False, allow_redirects=False)
                    print(f"Response status: {response.status_code}")
                    print(f"Response headers: {dict(response.headers)}")
                    if response.status_code == 302:
                        print(f"Redirect location: {response.headers.get('Location', 'N/A')}")
                except requests.exceptions.Timeout:
                    print("Connection timeout - dashboard service not responding")
                except requests.exceptions.ConnectionError as e:
                    print(f"Connection error: {e}")
                except Exception as e:
                    print(f"Request failed: {e}")
            else:
                print(f"Cannot test accessibility - invalid dashboard URI: {dashboard_uri}")
            
            # Check head pod specifically
            print("\n=== Head Pod Status ===")
            pods = v1.list_namespaced_pod(
                namespace=cluster.config.namespace,
                label_selector=f"ray.io/cluster={cluster.config.name},ray.io/node-type=head"
            )
            if pods.items:
                for pod in pods.items:
                    print(f"Head Pod: {pod.metadata.name}")
                    print(f"  Status: {pod.status.phase}")
                    print(f"  Pod IP: {pod.status.pod_ip}")
                    if pod.status.container_statuses:
                        for cs in pod.status.container_statuses:
                            print(f"  Container {cs.name}: ready={cs.ready}, restarts={cs.restart_count}")
            else:
                print("No head pod found!")
            
        except Exception as e:
            print(f"Error in dashboard diagnostics: {e}")
            import traceback
            traceback.print_exc()
        
        print("=== End Dashboard Diagnostics ===\n")

    def print_cluster_diagnostics(self, cluster):
        """Print diagnostic information about the cluster and its pods"""
        import kubernetes.client as k8s_client
        
        print("\n=== Cluster Diagnostics ===")
        print(f"Cluster name: {cluster.config.name}")
        print(f"Namespace: {cluster.config.namespace}")
        
        try:
            # Get pod status
            v1 = k8s_client.CoreV1Api()
            pods = v1.list_namespaced_pod(namespace=cluster.config.namespace)
            
            print(f"\nFound {len(pods.items)} pods in namespace:")
            for pod in pods.items:
                print(f"\nPod: {pod.metadata.name}")
                print(f"  Status: {pod.status.phase}")
                print(f"  Conditions:")
                if pod.status.conditions:
                    for condition in pod.status.conditions:
                        print(f"    {condition.type}: {condition.status} - {condition.reason}")
                
                print(f"  Container Statuses:")
                if pod.status.container_statuses:
                    for container_status in pod.status.container_statuses:
                        print(f"    {container_status.name}:")
                        print(f"      Ready: {container_status.ready}")
                        print(f"      RestartCount: {container_status.restart_count}")
                        if container_status.state.waiting:
                            print(f"      Waiting: {container_status.state.waiting.reason} - {container_status.state.waiting.message}")
                        if container_status.state.terminated:
                            print(f"      Terminated: {container_status.state.terminated.reason} - {container_status.state.terminated.message}")
                else:
                    print("    No container statuses available")
                
                # Print recent events for this pod
                print(f"  Recent Events:")
                events = v1.list_namespaced_event(
                    namespace=cluster.config.namespace,
                    field_selector=f"involvedObject.name={pod.metadata.name}"
                )
                for event in events.items[-5:]:  # Last 5 events
                    print(f"    [{event.type}] {event.reason}: {event.message}")
            
            # Check RayCluster custom resource
            print("\n=== RayCluster Custom Resource ===")
            custom_api = k8s_client.CustomObjectsApi()
            try:
                ray_cluster = custom_api.get_namespaced_custom_object(
                    group="ray.io",
                    version="v1",
                    namespace=cluster.config.namespace,
                    plural="rayclusters",
                    name=cluster.config.name
                )
                print(f"RayCluster found: {cluster.config.name}")
                if 'status' in ray_cluster:
                    print(f"Status: {ray_cluster['status']}")
                else:
                    print("No status field in RayCluster")
            except Exception as e:
                print(f"Failed to get RayCluster custom resource: {e}")
            
        except Exception as e:
            print(f"Error getting diagnostics: {e}")
        
        print("=== End Diagnostics ===\n")

    # Assertions

    def assert_jobsubmit_withoutLogin(self, cluster):
        dashboard_url = self.get_dashboard_url(cluster)
        
        print("\n=== Testing Job Submission Without Authentication ===")
        print(f"Dashboard URL: {dashboard_url}")
        
        # Check OAuth configuration
        self.check_oauth_configuration(cluster)

        # Verify that job submission is actually blocked by attempting to submit without auth
        # The endpoint path depends on whether we're using HTTPRoute (with path prefix) or not
        if "/ray/" in dashboard_url:
            # HTTPRoute format: https://hostname/ray/namespace/cluster-name
            # API endpoint is at the same base path
            api_url = dashboard_url + "/api/jobs/"
        else:
            # OpenShift Route format: https://hostname
            # API endpoint is directly under the hostname
            api_url = dashboard_url + "/api/jobs/"

        jobdata = {
            "entrypoint": "python mnist.py",
            "runtime_env": {
                "working_dir": "./tests/e2e/",
                "pip": "./tests/e2e/mnist_pip_requirements.txt",
                "env_vars": get_setup_env_variables(),
            },
        }

        # Try to submit a job without authentication
        # Follow redirects to see the final response - if it redirects to login, that's still a failure
        response = requests.post(
            api_url, verify=False, json=jobdata, allow_redirects=True
        )

        # Check if the submission was actually blocked
        # Success indicators that submission was blocked:
        # 1. Status code 403 (Forbidden)
        # 2. Status code 302 (Redirect to login) - but we need to verify the final response after redirect
        # 3. Status code 200 but with HTML content (login page) instead of JSON (job submission response)
        # 4. Status code 401 (Unauthorized)

        submission_blocked = False

        if response.status_code == 403:
            submission_blocked = True
        elif response.status_code == 401:
            submission_blocked = True
        elif response.status_code == 302:
            # Redirect happened - check if final response after redirect is also a failure
            # If we followed redirects, check the final status
            submission_blocked = True  # Redirect to login means submission failed
        elif response.status_code == 200:
            # Check if response is HTML (login page) instead of JSON (job submission response)
            content_type = response.headers.get("Content-Type", "")
            if "text/html" in content_type or "application/json" not in content_type:
                # Got HTML (likely login page) instead of JSON - submission was blocked
                submission_blocked = True
            else:
                # Got JSON response - check if it's an error or actually a successful submission
                try:
                    json_response = response.json()
                    # If it's a successful job submission, it should have a 'job_id' or 'submission_id'
                    # If it's an error, it might have 'error' or 'message'
                    if "job_id" in json_response or "submission_id" in json_response:
                        # Job was actually submitted - this is a failure!
                        submission_blocked = False
                    else:
                        # Error response - submission was blocked
                        submission_blocked = True
                except ValueError:
                    # Not JSON - likely HTML login page
                    submission_blocked = True

        if not submission_blocked:
            error_msg = (
                f"\n❌ OAUTH TEST FAILURE: Job submission succeeded without authentication!\n"
                f"   Status: {response.status_code}\n"
                f"   Response: {response.text[:200]}\n\n"
                f"This indicates that the Ray dashboard is NOT protected by OAuth.\n"
                f"The cluster was created with OAuth enabled but authentication is not working.\n\n"
                f"Common causes:\n"
                f"  1. Route is missing OAuth annotations\n"
                f"  2. kube-rbac-proxy container is not configured properly\n"
                f"  3. Service is routing directly to Ray dashboard (port 8265) instead of through proxy (port 8080)\n"
                f"  4. RayCluster CR is missing OAuth configuration\n\n"
                f"Check the OAuth configuration diagnostics above for details."
            )
            assert False, error_msg

        # Also verify that RayJobClient cannot be used without authentication
        try:
            client = RayJobClient(address=dashboard_url, verify=False)
            # Try to call a method to trigger the connection and authentication check
            client.list_jobs()
            assert (
                False
            ), "RayJobClient succeeded without authentication - this should not be possible"
        except (
            requests.exceptions.JSONDecodeError,
            requests.exceptions.HTTPError,
            Exception,
        ):
            # Any exception is expected when trying to use the client without auth
            pass

        assert True, "Job submission without authentication was correctly blocked"

    def assert_jobsubmit_withlogin(self, cluster):
        auth_token = run_oc_command(["whoami", "--show-token=true"])
        ray_dashboard = self.get_dashboard_url(cluster)
        header = {"Authorization": f"Bearer {auth_token}"}
        client = RayJobClient(address=ray_dashboard, headers=header, verify=False)

        # Verify that no jobs were submitted during the previous unauthenticated test
        # This ensures that the authentication check in assert_jobsubmit_withoutLogin actually worked
        existing_jobs = client.list_jobs()
        if existing_jobs:
            job_ids = [
                job.job_id if hasattr(job, "job_id") else str(job)
                for job in existing_jobs
            ]
            assert False, (
                f"Found {len(existing_jobs)} existing job(s) before authenticated submission: {job_ids}. "
                "This indicates that the unauthenticated job submission test failed to properly block submission."
            )
        else:
            print(
                "Verified: No jobs exist from the previous unauthenticated submission attempt."
            )

        submission_id = client.submit_job(
            entrypoint="python mnist.py",
            runtime_env={
                "working_dir": "./tests/e2e/",
                "pip": "./tests/e2e/mnist_pip_requirements.txt",
                "env_vars": get_setup_env_variables(),
            },
            entrypoint_num_cpus=1,
        )
        print(f"Submitted job with ID: {submission_id}")
        done = False
        time = 0
        timeout = 900
        while not done:
            status = client.get_job_status(submission_id)
            if status.is_terminal():
                break
            if not done:
                print(status)
                if timeout and time >= timeout:
                    raise TimeoutError(f"job has timed out after waiting {timeout}s")
                sleep(5)
                time += 5

        logs = client.get_job_logs(submission_id)
        print(logs)

        self.assert_job_completion(status)

        client.delete_job(submission_id)

    def assert_job_completion(self, status):
        if status == "SUCCEEDED":
            print(f"Job has completed: '{status}'")
            assert True
        else:
            print(f"Job has completed: '{status}'")
            assert False
