package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"

	yaml "go.yaml.in/yaml/v3"
)

// TestKubernetesOverlaysPinTheImage (TODO-3 item 170): every workload named
// ghcr.io/fastbean-au/hippocampus:latest, so a node that pulled the image afresh could run a newer
// build than the rest - and a newer build migrates the schema forward, after which a rollback is
// refused with ErrSchemaTooNew. Each overlay pins the tag with kustomize's images stanza, to the
// newest version CHANGELOG.md has released, which scripts/release.sh bumps as it cuts one.
func TestKubernetesOverlaysPinTheImage(t *testing.T) {
	changelog, err := os.ReadFile(filepath.Join("..", "..", "CHANGELOG.md"))
	if err != nil {
		t.Fatalf("reading the changelog: %s", err)
	}

	released := regexp.MustCompile(`(?m)^## \[(\d+\.\d+\.\d+)\]`).FindSubmatch(changelog)
	if released == nil {
		t.Fatal("no released version in CHANGELOG.md")
	}

	want := string(released[1])

	overlays, err := filepath.Glob(filepath.Join("..", "..", "deploy", "k8s", "overlays", "*", "kustomization.yaml"))
	if err != nil || len(overlays) == 0 {
		t.Fatalf("found no overlays: %v", err)
	}

	for _, path := range overlays {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %s", path, err)
		}

		var kustomization struct {
			Images []struct {
				Name   string `yaml:"name"`
				NewTag string `yaml:"newTag"`
			} `yaml:"images"`
		}

		if err := yaml.Unmarshal(source, &kustomization); err != nil {
			t.Fatalf("parsing %s: %s", path, err)
		}

		pinned := ""

		for _, image := range kustomization.Images {
			if image.Name == "ghcr.io/fastbean-au/hippocampus" {
				pinned = image.NewTag
			}
		}

		if pinned != want {
			t.Errorf("%s pins the hippocampus image to %q, want the newest release %q", path, pinned, want)
		}
	}
}

// TestPinK8sImageScriptBumpsEveryOverlay runs the script release.sh calls against a copy of the
// overlays, since nothing else executes it until the day a release depends on it.
func TestPinK8sImageScriptBumpsEveryOverlay(t *testing.T) {
	root := t.TempDir()

	overlays, err := filepath.Glob(filepath.Join("..", "..", "deploy", "k8s", "overlays", "*", "kustomization.yaml"))
	if err != nil || len(overlays) == 0 {
		t.Fatalf("found no overlays: %v", err)
	}

	for _, path := range overlays {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read: %s", err)
		}

		target := filepath.Join(root, "deploy", "k8s", "overlays", filepath.Base(filepath.Dir(path)), "kustomization.yaml")

		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatalf("mkdir: %s", err)
		}

		if err := os.WriteFile(target, source, 0o600); err != nil {
			t.Fatalf("write: %s", err)
		}
	}

	for range 2 { // twice: the second run proves it idempotent
		out, err := exec.Command(filepath.Join("..", "..", "scripts", "pin-k8s-image.sh"), "v9.8.7", root).CombinedOutput()
		if err != nil {
			t.Fatalf("pin-k8s-image.sh: %s\n%s", err, out)
		}
	}

	pinned, err := filepath.Glob(filepath.Join(root, "deploy", "k8s", "overlays", "*", "kustomization.yaml"))
	if err != nil {
		t.Fatalf("glob: %s", err)
	}

	for _, path := range pinned {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read: %s", err)
		}

		if !regexp.MustCompile(`name: ghcr\.io/fastbean-au/hippocampus\s*\n\s+newTag: "9\.8\.7"`).Match(source) {
			t.Errorf("%s was not pinned to 9.8.7:\n%s", path, source)
		}
	}
}

// TestKubernetesWorkloadsProbeTheLifecycleEndpoints (TODO-3 item 181): every hippocampus container
// in the overlays has a startup and a liveness probe on /livez, a readiness probe on /readyz, and a
// preStop pause, inside a grace period long enough for the pause and the shutdown after it. A
// liveness probe left on /healthz still works, but a workload missing the startup probe or the
// pause is the one that gets killed mid-migration or drops requests on every rollout.
func TestKubernetesWorkloadsProbeTheLifecycleEndpoints(t *testing.T) {
	type probe struct {
		HTTPGet struct {
			Path string `yaml:"path"`
		} `yaml:"httpGet"`
	}

	type workload struct {
		Kind string `yaml:"kind"`
		Spec struct {
			Template struct {
				Spec struct {
					TerminationGracePeriodSeconds int `yaml:"terminationGracePeriodSeconds"`
					Containers                    []struct {
						Name           string `yaml:"name"`
						StartupProbe   *probe `yaml:"startupProbe"`
						LivenessProbe  *probe `yaml:"livenessProbe"`
						ReadinessProbe *probe `yaml:"readinessProbe"`
						Lifecycle      struct {
							PreStop struct {
								Sleep struct {
									Seconds int `yaml:"seconds"`
								} `yaml:"sleep"`
							} `yaml:"preStop"`
						} `yaml:"lifecycle"`
					} `yaml:"containers"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}

	paths, err := filepath.Glob(filepath.Join("..", "..", "deploy", "k8s", "overlays", "*", "*.yaml"))
	if err != nil {
		t.Fatalf("glob: %s", err)
	}

	var found int

	for _, path := range paths {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %s", path, err)
		}

		decoder := yaml.NewDecoder(bytes.NewReader(source))

		for {
			var w workload

			if err := decoder.Decode(&w); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				t.Fatalf("parsing %s: %s", path, err)
			}

			if w.Kind != "Deployment" && w.Kind != "StatefulSet" {
				continue
			}

			pod := w.Spec.Template.Spec

			for _, c := range pod.Containers {
				if c.Name != "hippocampus" {
					continue
				}

				found++

				checks := map[string]*probe{"startupProbe": c.StartupProbe, "livenessProbe": c.LivenessProbe}

				for name, p := range checks {
					if p == nil || p.HTTPGet.Path != "/livez" {
						t.Errorf("%s: the %s must GET /livez", path, name)
					}
				}

				if c.ReadinessProbe == nil || c.ReadinessProbe.HTTPGet.Path != "/readyz" {
					t.Errorf("%s: the readinessProbe must GET /readyz", path)
				}

				pause := c.Lifecycle.PreStop.Sleep.Seconds

				if pause <= 0 {
					t.Errorf("%s: no preStop sleep, so a rollout drops the requests routed while the endpoint removal propagates", path)
				}

				// Three shutdown phases of shutdown.timeoutSeconds (10s by default) follow the pause.
				if pod.TerminationGracePeriodSeconds < pause+30 {
					t.Errorf("%s: terminationGracePeriodSeconds %d leaves less than 30s for the shutdown after the %ds pause", path, pod.TerminationGracePeriodSeconds, pause)
				}
			}
		}
	}

	if found == 0 {
		t.Fatal("found no hippocampus containers in the overlays")
	}
}
