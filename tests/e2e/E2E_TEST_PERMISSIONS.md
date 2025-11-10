# E2E Test Permissions Setup

## Problem

Running e2e tests with a non-admin user results in permission errors:
```
User "ldap-admin1" cannot delete resource "namespaces"
User "ldap-admin1" cannot create resource "localqueues" 
```

## Solution

The e2e test user needs specific cluster-level permissions to create and clean up test resources.

## Quick Setup

### Option 1: Grant Admin Permissions (Simplest - Test Clusters Only!)

⚠️ **Only use on test/development clusters**

```bash
# Replace 'ldap-admin1' with your test user
oc adm policy add-cluster-role-to-user cluster-admin ldap-admin1
```

### Option 2: Grant Minimal Required Permissions (Recommended)

Apply the RBAC configuration:

```bash
# Edit test-rbac.yaml and replace 'ldap-admin1' with your user
vi tests/e2e/test-rbac.yaml

# Apply the RBAC configuration
oc apply -f tests/e2e/test-rbac.yaml
```

This grants permissions to:
- ✅ Create/delete namespaces (for test isolation)
- ✅ Manage Kueue resources (ClusterQueue, ResourceFlavor, LocalQueue)
- ✅ Manage Ray clusters
- ✅ View routes, pods, services (for diagnostics)

### Option 3: Custom Service Account (CI/CD Pipelines)

Create a dedicated service account for tests:

```bash
# Create service account
oc create serviceaccount codeflare-e2e-sa -n your-namespace

# Grant permissions
oc apply -f tests/e2e/test-rbac.yaml

# Update ClusterRoleBinding to use service account:
oc patch clusterrolebinding codeflare-e2e-test-binding \
  --type='json' \
  -p='[{"op": "replace", "path": "/subjects/0", "value": {"kind": "ServiceAccount", "name": "codeflare-e2e-sa", "namespace": "your-namespace"}}]'

# Get token
SA_TOKEN=$(oc create token codeflare-e2e-sa -n your-namespace --duration=24h)

# Use token in tests
export KUBECONFIG=/path/to/kubeconfig
# Update kubeconfig to use the token
```

## Permissions Breakdown

### Cluster-Scoped Resources

| Resource | Permissions | Why Needed |
|----------|------------|------------|
| Namespaces | create, delete, list | Tests create isolated namespaces for each run |
| ClusterQueues | create, delete, list | Kueue resource for job scheduling |
| ResourceFlavors | create, delete, list | Kueue resource for node targeting |
| Nodes | list | Check cluster resources before test |

### Namespace-Scoped Resources

| Resource | Permissions | Why Needed |
|----------|------------|------------|
| RayClusters | create, delete, get, list | Core test subject |
| LocalQueues | create, delete, get, list | Kueue namespace resource |
| Pods | get, list | Diagnostics and status checking |
| Services | get, list | Dashboard URL discovery |
| Routes | create, get, list | OpenShift dashboard access |
| Events | get, list | Debugging pod failures |

## Verifying Permissions

Check if user has required permissions:

```bash
# Check namespace creation
oc auth can-i create namespaces

# Check Kueue resources
oc auth can-i create clusterqueues
oc auth can-i create resourceflavors

# Check Ray clusters
oc auth can-i create rayclusters --all-namespaces

# Check Routes
oc auth can-i list routes --all-namespaces
```

All should return `yes`.

## Security Considerations

### For Production Environments

❌ **DO NOT** grant these permissions in production clusters

✅ **DO** use:
- Dedicated test clusters
- Time-limited tokens
- Service accounts with audit logging
- Namespace-scoped permissions where possible

### Principle of Least Privilege

The `test-rbac.yaml` grants minimal permissions needed for tests. For even tighter security:

1. **Restrict to namespace patterns**: Only allow operations on namespaces matching `test-ns-*`
2. **Use ResourceQuotas**: Limit resource consumption
3. **Enable audit logging**: Track all test operations
4. **Use temporary tokens**: Short-lived credentials for CI/CD

### Example: Namespace Pattern Restriction

```yaml
# More restricted: only test namespaces
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: codeflare-e2e-test-role-restricted
rules:
  - apiGroups: [""]
    resources: ["namespaces"]
    resourceNames: ["test-ns-*"]  # Only namespaces matching pattern
    verbs: ["delete"]
  - apiGroups: [""]
    resources: ["namespaces"]
    verbs: ["create", "list"]  # Can create/list any namespace
```

## Troubleshooting

### Error: Cannot delete namespace

```
User "ldap-admin1" cannot delete resource "namespaces"
```

**Solution**: Grant namespace delete permission:
```bash
oc adm policy add-cluster-role-to-user codeflare-e2e-test-role ldap-admin1
```

### Error: Cannot create ClusterQueue

```
User "ldap-admin1" cannot create resource "clusterqueues"
```

**Solution**: Ensure Kueue CRDs are installed and permissions granted:
```bash
# Check if Kueue is installed
oc get crd clusterqueues.kueue.x-k8s.io

# Grant permissions
oc apply -f tests/e2e/test-rbac.yaml
```

### Error: Cannot list routes

```
User "ldap-admin1" cannot list resource "routes"
```

**Solution**: Grant route read permissions:
```bash
oc adm policy add-cluster-role-to-user view ldap-admin1
```

## Alternative: Pre-created Test Namespace

If you cannot grant namespace creation permissions, use a pre-created namespace:

```bash
# Admin creates namespace
oc create namespace codeflare-e2e-test

# Grant user access
oc adm policy add-role-to-user admin ldap-admin1 -n codeflare-e2e-test

# Modify test to use fixed namespace instead of random
# In tests/e2e/support.py:
def create_namespace(self):
    self.namespace = "codeflare-e2e-test"
    # Don't create, assume it exists
```

## CI/CD Integration

For automated testing:

```yaml
# .github/workflows/e2e-test.yml (example)
- name: Setup test permissions
  run: |
    oc apply -f tests/e2e/test-rbac.yaml
    oc adm policy add-cluster-role-to-user codeflare-e2e-test-role system:serviceaccount:$NAMESPACE:$SA_NAME

- name: Run e2e tests
  run: |
    pytest tests/e2e/mnist_raycluster_sdk_oauth_test.py -v
```

## Summary

✅ **Required**: Cluster-level permissions for namespace and Kueue resource management  
✅ **Recommended**: Use `test-rbac.yaml` for minimal required permissions  
✅ **Test-only**: Never use in production environments  
✅ **Alternative**: Run as cluster-admin in test clusters (simplest)

After applying permissions, re-run the test:
```bash
pytest tests/e2e/mnist_raycluster_sdk_oauth_test.py::TestRayClusterSDKOauth::test_mnist_ray_cluster_sdk_auth -v -s
```

