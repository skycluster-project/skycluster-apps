package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type vServiceStruct struct {
  VServiceName     string  `json:"vservice_name,omitempty"`
  VServiceKind     string  `json:"vservice_kind,omitempty"`
  ProviderName     string  `json:"provider_name,omitempty"`
  ProviderPlatform string  `json:"provider_platform,omitempty"`
  ProviderRegion   string  `json:"provider_region,omitempty"`
  ProviderZone     string  `json:"provider_zone,omitempty"`
  DeployCost       float64 `json:"deploy_cost"`
  Availability     int     `json:"availability,omitempty"`
}

// Local placeholder types for YAML unmarshalling.
// Replace or extend with your actual types (cv1a1.ZoneOfferings, hv1a1.ManagedK8s).
type ZoneOfferings struct {
  Zone      string `yaml:"zone"`
  Offerings []InstanceOffering `json:"zoneOfferings" yaml:"zoneOfferings"`
}

type InstanceOffering struct {
	Name        string   `json:"name,omitempty" yaml:"name,omitempty"`
	NameLabel   string   `json:"nameLabel,omitempty" yaml:"nameLabel,omitempty"`
	VCPUs       int      `json:"vcpus,omitempty" yaml:"vcpus,omitempty"`
	RAM         string   `json:"ram,omitempty" yaml:"ram,omitempty"`
	Price       string   `json:"price,omitempty" yaml:"price,omitempty"`
	GPU         GPU      `json:"gpu,omitempty" yaml:"gpu,omitempty"`
	Generation  string   `json:"generation,omitempty" yaml:"generation,omitempty"`
	VolumeTypes []string `json:"volumeTypes,omitempty" yaml:"volumeTypes,omitempty"`
	Spot        Spot     `json:"spot,omitempty" yaml:"spot,omitempty"`
}

type GPU struct {
	Enabled      bool   `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Manufacturer string `json:"manufacturer,omitempty" yaml:"manufacturer,omitempty"`
	Count        int    `json:"count,omitempty" yaml:"count,omitempty"`
	Model        string `json:"model,omitempty" yaml:"model,omitempty"`
	Unit         string `json:"unit,omitempty" yaml:"unit,omitempty"`
	Memory       string `json:"memory,omitempty" yaml:"memory,omitempty"`
}

type Spot struct {
	Price   string `json:"price,omitempty" yaml:"price,omitempty"`
	Enabled bool   `json:"enabled,omitempty" yaml:"enabled,omitempty"`
}

type ManagedK8s struct {
  Name      string `yaml:"name"`
  NameLabel string `yaml:"nameLabel"`
  Price     string `yaml:"price"`
  Overhead  struct {
    Cost string `yaml:"cost"`
  } `yaml:"overhead"`
}

type DeviceZoneSpec struct {
	Type      string             `json:"type" yaml:"type,omitempty"`
	Zone      string             `json:"zone" yaml:"zone,omitempty"`
	PrivateIp string             `json:"privateIp" yaml:"privateIp,omitempty"`
	PublicIp  string             `json:"publicIp,omitempty" yaml:"publicIp,omitempty"`
	Configs   *InstanceOffering   `json:"configs,omitempty" yaml:"configs,omitempty"`
}

func main() {
  ctx := context.Background()

  // OUTPUT_PATH env var controls where to write the JSON file inside the pod.
  // Default is /tmp/vservices.json (placeholder; change as needed).
  outputPath := os.Getenv("OUTPUT_PATH")
  if outputPath == "" {
    outputPath = "/tmp/vservices.json" // placeholder default
  }

  // LABEL_SELECTOR env var optional override for the configmap label selector.
  // Default uses the two labels from the snippet.
  labelSelector := os.Getenv("LABEL_SELECTOR")
  if labelSelector == "" {
    labelSelector = "skycluster.io/config-type=provider-profile,skycluster.io/managed-by=skycluster"
  }

  // NAMESPACE env var optional: if empty, list across all namespaces.
  // To restrict to a single namespace, set NAMESPACE=<your-namespace>.
  namespace := os.Getenv("NAMESPACE") // empty => all namespaces

  // In-cluster config
  cfg, err := rest.InClusterConfig()
  if err != nil {
    fmt.Fprintf(os.Stderr, "failed to get in-cluster config: %v\n", err)
    os.Exit(2)
  }

  clientset, err := kubernetes.NewForConfig(cfg)
  if err != nil {
    fmt.Fprintf(os.Stderr, "failed to create kubernetes clientset: %v\n", err)
    os.Exit(2)
  }

  listOptions := metav1.ListOptions{
    LabelSelector: labelSelector,
  }

  var vServicesList []vServiceStruct

  // List ConfigMaps (namespace == "" => all namespaces)
  cmList, err := clientset.CoreV1().ConfigMaps(namespace).List(ctx, listOptions)
  if err != nil {
    fmt.Fprintf(os.Stderr, "failed to list configmaps: %v\n", err)
    os.Exit(2)
  }

  for _, cm := range cmList.Items {
    pName := cm.Labels["skycluster.io/provider-profile"]
    pPlatform := cm.Labels["skycluster.io/provider-platform"]
    pRegion := cm.Labels["skycluster.io/provider-region"]
    fmt.Printf("Processing configmap %s/%s\n", cm.Namespace, cm.Name)

    // flavors.yaml: best-effort parsing (ignore errors as in original snippet)
    if cmData, ok := cm.Data["flavors.yaml"]; ok {
      var zoneOfferings []ZoneOfferings
      if err := yaml.Unmarshal([]byte(cmData), &zoneOfferings); err == nil {
        fmt.Printf("parsed flavors.yaml in configmap %s/%s with %d zone offerings\n", cm.Namespace, cm.Name, len(zoneOfferings))
        for _, zo := range zoneOfferings {
          fmt.Printf("  found zone offering: %s, %d offerings\n", zo.Zone, len(zo.Offerings))
          for _, of := range zo.Offerings {
            priceFloat, err := parseAmount(of.Price)
            if err != nil {
              fmt.Fprintf(os.Stderr, "failed to parse price for vservice %s in configmap %s/%s: %v\n", of.NameLabel, cm.Namespace, cm.Name, err)
              continue // skip invalid price
            }
            vServicesList = append(vServicesList, vServiceStruct{
              VServiceName:     of.NameLabel,
              VServiceKind:     "ComputeProfile",
              ProviderName:     pName,
              ProviderPlatform: pPlatform,
              ProviderRegion:   pRegion,
              ProviderZone:     zo.Zone,
              DeployCost:       priceFloat,
              Availability:     10000, // placeholder assumption
            })
          }
        }
      } else {
        // best-effort: log and continue
        fmt.Fprintf(os.Stderr, "warning: failed to unmarshal flavors.yaml in configmap %s/%s: %v\n", cm.Namespace, cm.Name, err)
      }
    }

    // managed-k8s.yaml: treat parse errors as fatal (mirrors original snippet)
    if cmData, ok := cm.Data["managed-k8s.yaml"]; ok {
      var managedK8s []ManagedK8s
      if err := yaml.Unmarshal([]byte(cmData), &managedK8s); err != nil {
        fmt.Fprintf(os.Stderr, "failed to unmarshal managed-k8s config map %s/%s: %v\n", cm.Namespace, cm.Name, err)
        os.Exit(2)
      }
      fmt.Println("parsed managed-k8s.yaml with", len(managedK8s), "managed k8s offerings")
      for _, mk8s := range managedK8s {
        priceFloat, err1 := parseAmount(mk8s.Price)
        priceOverheadFloat, err2 := parseAmount(mk8s.Overhead.Cost)
        if err1 != nil || err2 != nil {
          fmt.Fprintf(os.Stderr, "failed to parse price or overhead for managed k8s vservice %s in configmap %s/%s: price error: %v; overhead error: %v\n", mk8s.Name, cm.Namespace, cm.Name, err1, err2)
          os.Exit(2)
        }
        vServicesList = append(vServicesList, vServiceStruct{
          VServiceName:     mk8s.NameLabel,
          VServiceKind:     "ManagedKubernetes",
          ProviderName:     pName,
          ProviderPlatform: pPlatform,
          ProviderRegion:   pRegion,
          DeployCost:       priceFloat + priceOverheadFloat,
          Availability:     100000, // placeholder assumption
        })
      }
    }

    // baremetal workers
    if cmData, ok := cm.Data["worker"]; ok {
      var workerSpecs map[string]DeviceZoneSpec
      if err := yaml.Unmarshal([]byte(cmData), &workerSpecs); err != nil {
        fmt.Fprintf(os.Stderr, "failed to unmarshal workers config map %s/%s: %v\n", cm.Namespace, cm.Name, err)
        os.Exit(2)
      }
      fmt.Printf("parsed workers in configmap %s/%s with %d device specs\n", cm.Namespace, cm.Name, len(workerSpecs))
      for devName, devSpec := range workerSpecs {
        if devSpec.Configs == nil {continue}
        fmt.Printf(" Processing device %s\n", devName)
        priceFloat, err := parseAmount(devSpec.Configs.Price)
        if err != nil {
          fmt.Fprintf(os.Stderr, "failed to parse price for vservice %s in configmap %s/%s: %v\n", devName, cm.Namespace, cm.Name, err)
          continue // skip invalid price
        }
        computeValueNormalized := NormalizeToTOPS(float64(devSpec.Configs.GPU.Count), devSpec.Configs.GPU.Unit)
        devNameLabel := fmt.Sprintf("%dvCPU-%s-%dxTOPS", devSpec.Configs.VCPUs, devSpec.Configs.RAM, int(computeValueNormalized))
        fmt.Printf("  Device %s: %s\n", devName, devNameLabel)
        vServicesList = append(vServicesList, vServiceStruct{
          VServiceName:     devNameLabel,
          VServiceKind:     "ComputeProfile",
          ProviderName:     pName,
          ProviderPlatform: pPlatform,
          ProviderRegion:   pRegion,
          ProviderZone:     devSpec.Zone,
          DeployCost:       priceFloat,
          Availability:     1, // one device
        })
      }
    }
  }

  b, err := json.MarshalIndent(vServicesList, "", "  ")
  if err != nil {
    fmt.Fprintf(os.Stderr, "failed to marshal virtual services: %v\n", err)
    os.Exit(2)
  }

  if err := os.WriteFile(outputPath, b, 0644); err != nil {
    fmt.Fprintf(os.Stderr, "failed to write output file %s: %v\n", outputPath, err)
    os.Exit(2)
  }

  fmt.Printf("wrote %d virtual services to %s\n", len(vServicesList), outputPath)
}



func parseAmount(s string) (float64, error) {
	s = strings.TrimSpace(s)

	// handle parentheses as negative: (123.45)
	neg := false
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		neg = true
		s = s[1 : len(s)-1]
	}

	// remove dollar sign(s), commas and surrounding spaces
	s = strings.ReplaceAll(s, "$", "")
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("no numeric content")
	}

	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	if neg {
		v = -v
	}
	return v, nil
}

// NormalizeToTOPS converts a compute value (with unit) into TOPS
func NormalizeToTOPS(value float64, unit string) float64 {
	unit = strings.ToUpper(strings.TrimSpace(unit))

	switch unit {
	case "GFLOPS":
		// 1 GFLOP = 0.001 TFLOP ≈ 0.002 TOPS
		return value * 0.002
	case "TFLOPS":
		// 1 TFLOP ≈ 2 TOPS (rough heuristic for FP32->INT8 conversion)
		return value * 2.0
	case "TOPS":
		return value
	default:
		fmt.Printf("Warning: Unknown unit '%s', returning raw value\n", unit)
		return value
	}
}
