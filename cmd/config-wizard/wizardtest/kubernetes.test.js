// The Kubernetes manifests the wizard generates, held to the lifecycle the shipped overlays use
// (TODO-3 item 181): startup and liveness on /livez, readiness on /readyz, a preStop pause, and a
// grace period that covers the pause and the shutdown after it.

import test from "node:test";
import assert from "node:assert/strict";

import { loadWizard } from "./harness.js";

test("the generated workload probes the lifecycle endpoints", () => {
  const wizard = loadWizard();

  wizard.setValue("gateway.port", 8080);

  const manifests = wizard.kubernetesManifests();

  assert.match(manifests, /startupProbe:\n(?:\s+#.*\n)*\s+httpGet:\n\s+path: \/livez/);
  assert.match(manifests, /livenessProbe:\n(?:\s+#.*\n)*\s+httpGet:\n\s+path: \/livez/);
  assert.match(manifests, /readinessProbe:\n(?:\s+#.*\n)*\s+httpGet:\n\s+path: \/readyz/);
  assert.doesNotMatch(manifests, /path: \/healthz/);
  assert.match(manifests, /preStop:\n\s+sleep:\n\s+seconds: 5/);
});

test("the grace period covers the pause and three shutdown phases", () => {
  const wizard = loadWizard();

  wizard.setValue("shutdown.timeoutSeconds", 20);

  assert.match(wizard.kubernetesManifests(), /terminationGracePeriodSeconds: 65\n/);
});

test("the pause is kept without the gateway, which only the probes need", () => {
  const wizard = loadWizard();

  wizard.setValue("gateway.port", 0);

  const manifests = wizard.kubernetesManifests();

  assert.doesNotMatch(manifests, /Probe:/);
  assert.match(manifests, /preStop:/);
});
