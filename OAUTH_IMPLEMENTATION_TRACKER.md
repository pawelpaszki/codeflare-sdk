# OAuth Implementation Tracker for Ray Clusters

## 📋 Overview

**Status**: 🔴 Not Implemented  
**Priority**: High  
**Test**: `tests/e2e/mnist_raycluster_sdk_oauth_test.py`  
**Issue**: Ray clusters created by SDK are not protected by OAuth/RBAC  

## 🔍 Current State

### What Works ✅
- kube-rbac-proxy sidecar container is present in head pod (port 8443)
- Ray dashboard is accessible and functional (port 8265)
- Test diagnostics correctly identify the configuration gap

### What's Broken ❌
- Service routes dashboard traffic directly to Ray (port 8265)
- Should route through kube-rbac-proxy (port 8443)
- No OAuth annotations on OpenShift Route
- Job submission succeeds without authentication

### Diagnostic Evidence

```
Service: mnist-head-svc
  Port dashboard: 8265 -> 8265  ❌ WRONG
  ⚠️  WARNING: Dashboard port targeting 8265 (direct Ray dashboard)
     This BYPASSES authentication!

Should be:
  Port dashboard: 8265 -> 8443  ✅ CORRECT (through proxy)
```

## 🎯 Implementation Tasks

### Task 1: Configure Service Port Routing
**Status**: 🔴 To Do  
**Owner**: TBD  
**Files**: `src/codeflare_sdk/ray/cluster/build_ray_cluster.py`

**Current**:
```yaml
spec:
  headGroupSpec:
    template:
      spec:
        containers:
          - name: ray-head
            ports:
              - containerPort: 8265
                name: dashboard
```

**Required**:
```yaml
spec:
  headGroupSpec:
    template:
      spec:
        containers:
          - name: ray-head
            ports:
              - containerPort: 8265
                name: ray-dashboard  # Internal Ray port
          - name: kube-rbac-proxy
            ports:
              - containerPort: 8443
                name: dashboard  # External proxy port
    # Service should target 'dashboard' (8443) not 'ray-dashboard' (8265)
```

**Code Changes Needed**:
1. Ensure service `dashboard` port targets `8443` (proxy) not `8265` (Ray)
2. Add kube-rbac-proxy container configuration if not present
3. Configure RBAC proxy to forward to Ray dashboard on `localhost:8265`

### Task 2: Add OAuth Route Annotations
**Status**: 🔴 To Do  
**Owner**: TBD  
**Component**: KubeRay Operator or SDK

**Required Annotations** (OpenShift):
```yaml
metadata:
  annotations:
    # OAuth redirect annotations
    haproxy.router.openshift.io/auth-type: "oauth2"
    haproxy.router.openshift.io/auth-url: "https://oauth-openshift.apps.cluster.example.com/oauth/authorize"
    haproxy.router.openshift.io/auth-realm: "OpenShift"
```

**Alternative** (for ODH/RHOAI):
```yaml
metadata:
  annotations:
    odh.ray.io/secured: "true"
    # Additional ODH-specific annotations TBD
```

**Implementation Options**:
- [ ] SDK adds annotations when creating Route
- [ ] KubeRay operator adds annotations (preferred)
- [ ] Separate admission webhook adds annotations

### Task 3: Configure kube-rbac-proxy
**Status**: 🟡 Partially Done  
**Owner**: TBD  
**Notes**: Container exists but needs proper configuration

**Required Configuration**:
```yaml
- name: kube-rbac-proxy
  image: quay.io/openshift/origin-kube-rbac-proxy:latest
  args:
    - --secure-listen-address=0.0.0.0:8443
    - --upstream=http://127.0.0.1:8265/  # Ray dashboard
    - --tls-cert-file=/etc/tls/tls.crt
    - --tls-private-key-file=/etc/tls/tls.key
    - --logtostderr=true
  ports:
    - containerPort: 8443
      name: dashboard
      protocol: TCP
  volumeMounts:
    - name: tls
      mountPath: /etc/tls
      readOnly: true
```

**Questions**:
- [ ] Is kube-rbac-proxy configured to proxy to localhost:8265?
- [ ] Are TLS certificates properly mounted?
- [ ] Is RBAC configured to require authentication?

### Task 4: Add ClusterConfiguration Options
**Status**: 🔴 To Do  
**Owner**: TBD  
**Files**: `src/codeflare_sdk/ray/cluster/config.py`

**Add Configuration Options**:
```python
@dataclass
class ClusterConfiguration:
    # ... existing fields ...
    
    enable_oauth: bool = True
    """Enable OAuth/RBAC protection for Ray dashboard (OpenShift only)"""
    
    oauth_annotations: Optional[Dict[str, str]] = None
    """Custom OAuth annotations for Route (overrides defaults)"""
    
    rbac_proxy_image: str = "quay.io/openshift/origin-kube-rbac-proxy:latest"
    """kube-rbac-proxy image to use for dashboard authentication"""
```

**Usage**:
```python
cluster = Cluster(ClusterConfiguration(
    name="mnist",
    namespace="test",
    enable_oauth=True,  # Enable OAuth protection
    ...
))
```

### Task 5: Update Documentation
**Status**: 🔴 To Do  
**Owner**: TBD

**Documents to Update**:
- [ ] SDK README - OAuth configuration section
- [ ] API documentation - ClusterConfiguration OAuth options
- [ ] Example notebooks - OAuth-enabled cluster creation
- [ ] Security documentation - RBAC requirements

### Task 6: Update Tests
**Status**: 🟡 In Progress  
**Owner**: TBD  
**Files**: `tests/e2e/mnist_raycluster_sdk_oauth_test.py`

**Current Status**:
- [x] Test correctly identifies OAuth not working
- [x] Comprehensive diagnostics added
- [x] Dashboard URL discovery with fallback
- [x] OAuth-aware readiness checks
- [ ] Test needs to be un-skipped once OAuth works

**Test Checklist**:
- [x] Create cluster with OAuth enabled
- [x] Verify dashboard is accessible
- [x] Verify job submission WITHOUT auth is blocked (302/401/403)
- [x] Verify job submission WITH auth succeeds
- [x] Verify OAuth configuration is correct (diagnostics)
- [ ] Test passes with OAuth properly configured

## 🔧 Technical Approach

### Option A: SDK-Level Implementation
**Pros**: 
- Quick to implement
- Full control over configuration
- Can support multiple K8s distributions

**Cons**:
- SDK needs to know about OAuth specifics
- Harder to maintain across versions
- Not consistent with operator-managed clusters

### Option B: Operator-Level Implementation ⭐ (Recommended)
**Pros**:
- Centralized configuration
- Consistent across all Ray clusters
- Leverages existing operator capabilities
- Can be enabled cluster-wide

**Cons**:
- Requires KubeRay operator changes
- Longer implementation timeline
- Need to wait for operator release

### Option C: Hybrid Approach
**Pros**:
- SDK provides configuration options
- Operator respects/implements them
- Backwards compatible

**Cons**:
- More complex
- Requires coordination

## 📊 Acceptance Criteria

### Must Have
- [ ] Service dashboard port routes through kube-rbac-proxy (8443)
- [ ] Job submission without authentication returns 401/403
- [ ] Job submission with valid token succeeds
- [ ] `tests/e2e/mnist_raycluster_sdk_oauth_test.py` passes
- [ ] OAuth works with both admin and non-admin users
- [ ] Documentation updated

### Should Have
- [ ] OAuth can be disabled via configuration
- [ ] Works with OpenShift Routes
- [ ] Works with HTTPRoute/Gateway API (RHOAI v3.0+)
- [ ] Custom OAuth annotations supported
- [ ] TLS properly configured

### Nice to Have
- [ ] Works with Kubernetes Ingress (non-OpenShift)
- [ ] Multiple authentication providers
- [ ] Token refresh support
- [ ] Audit logging

## 🧪 Testing Checklist

### Unit Tests
- [ ] ClusterConfiguration validates OAuth options
- [ ] Service port configuration is correct
- [ ] Route annotations are added

### Integration Tests
- [ ] Cluster creates successfully with OAuth enabled
- [ ] Service routes to correct port
- [ ] Route has OAuth annotations

### E2E Tests
- [ ] `tests/e2e/mnist_raycluster_sdk_oauth_test.py` passes
- [ ] Dashboard accessible with authentication
- [ ] Job submission blocked without auth
- [ ] Job submission succeeds with auth

### Manual Testing
- [ ] Test with admin user
- [ ] Test with non-admin user
- [ ] Test with no authentication
- [ ] Test with expired token
- [ ] Test dashboard UI in browser
- [ ] Test Ray job submission CLI

## 📝 Implementation Plan

### Phase 1: Investigation (1 week)
- [ ] Review KubeRay operator OAuth support
- [ ] Identify required changes
- [ ] Determine implementation approach
- [ ] Create detailed design document

### Phase 2: Core Implementation (2-3 weeks)
- [ ] Implement service port routing fix
- [ ] Add kube-rbac-proxy configuration
- [ ] Add ClusterConfiguration options
- [ ] Update cluster creation logic

### Phase 3: Route/OAuth Configuration (1-2 weeks)
- [ ] Implement OAuth annotation logic
- [ ] Add HTTPRoute support (RHOAI v3.0+)
- [ ] Configure TLS certificates
- [ ] Test on OpenShift clusters

### Phase 4: Testing & Documentation (1 week)
- [ ] Un-skip OAuth test
- [ ] Add unit tests
- [ ] Update documentation
- [ ] Create examples

### Phase 5: Review & Release
- [ ] Code review
- [ ] Security review
- [ ] Integration testing
- [ ] Release notes

**Estimated Total**: 5-7 weeks

## 🐛 Known Issues

### Issue 1: Service Port Misconfiguration
**Severity**: High  
**Impact**: Dashboard accessible without authentication  
**Status**: Confirmed  

**Diagnostic Output**:
```
Service: mnist-head-svc
  Port dashboard: 8265 -> 8265
  ⚠️  WARNING: Dashboard port targeting 8265 (direct Ray dashboard)
     This BYPASSES authentication!
```

### Issue 2: Missing Route Annotations
**Severity**: High  
**Impact**: No OAuth redirect at route level  
**Status**: Confirmed  

**Diagnostic Output**:
```
Route: mnist-head-route
  Annotations: {'odh.ray.io/secure-trusted-network': 'true'}
  ⚠️  WARNING: No OAuth annotations found on route!
     This route is NOT protected by OAuth
```

### Issue 3: Admin Users Bypass RBAC
**Severity**: Low (Expected behavior)  
**Impact**: Tests with admin users won't validate OAuth  
**Status**: By Design  
**Workaround**: Test with non-admin users  

## 📚 Resources

### Documentation
- [KubeRay Operator](https://github.com/ray-project/kuberay)
- [kube-rbac-proxy](https://github.com/brancz/kube-rbac-proxy)
- [OpenShift OAuth](https://docs.openshift.com/container-platform/latest/authentication/index.html)
- [Gateway API](https://gateway-api.sigs.k8s.io/)

### Related Issues
- TBD: Create GitHub issue for tracking

### Code Locations
- `src/codeflare_sdk/ray/cluster/build_ray_cluster.py` - Cluster creation
- `src/codeflare_sdk/ray/cluster/config.py` - Configuration
- `tests/e2e/mnist_raycluster_sdk_oauth_test.py` - OAuth tests

## 🔄 Updates

### 2025-11-10 - Initial Assessment
- ✅ Test diagnostics implemented
- ✅ Root cause identified (service port misconfiguration)
- ✅ Test correctly identifies OAuth not working
- ❌ OAuth implementation not started

---

## 📞 Next Steps

1. **Review this tracker** with the team
2. **Decide on implementation approach** (SDK vs Operator vs Hybrid)
3. **Assign ownership** for each task
4. **Create tracking issue** in GitHub
5. **Begin Phase 1** investigation

## 📌 Quick Reference

**To skip the OAuth test until fixed**:
```python
@pytest.mark.skip(reason="OAuth not configured - Service port routes to 8265 instead of 8443 (OAUTH_IMPLEMENTATION_TRACKER.md)")
@pytest.mark.openshift
class TestRayClusterSDKOauth:
    ...
```

**To test OAuth diagnostics**:
```bash
pytest tests/e2e/mnist_raycluster_sdk_oauth_test.py::TestRayClusterSDKOauth::test_mnist_ray_cluster_sdk_auth -v -s
```

**Current diagnostic output** shows exact issues that need fixing.

