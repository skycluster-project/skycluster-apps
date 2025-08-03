package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {

	// Connect to Kubernetes
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("failed to get in-cluster config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		log.Fatalf("failed to create Kubernetes client: %v", err)
	}

	// Load config.yaml from current directory
	inputConfigPath := "/app/config.yml"
	configFile, err := os.Open(inputConfigPath)
	if err != nil {
		log.Fatalf("failed to open %s: %v", inputConfigPath, err)
	}
	defer configFile.Close()

	var config map[string]interface{}
	if err := yaml.NewDecoder(configFile).Decode(&config); err != nil {
		log.Fatalf("failed to decode YAML config: %v", err)
	}

	// Load acl.json from current directory
	inputACLPath := "/app/acl.json"
	aclFile, err := os.Open(inputACLPath)
	if err != nil {
		log.Fatalf("failed to open %s: %v", inputACLPath, err)
	}
	defer aclFile.Close()

	var acl map[string]interface{}
	if err := json.NewDecoder(aclFile).Decode(&acl); err != nil {
		log.Fatalf("failed to decode ACL JSON: %v", err)
	}

	// Fetch TLS secret
	secretNamespace := os.Getenv("SECRET_NAMESPACE")
	secretName := os.Getenv("SECRET_NAME")
	if secretNamespace == "" || secretName == "" {
		log.Fatalf("SECRET_NAMESPACE and SECRET_NAME environment variables must be set")
	}
	secret, err := clientset.CoreV1().Secrets(secretNamespace).Get(context.TODO(), secretName, metav1.GetOptions{})
	if err != nil {
		log.Fatalf("failed to get secret %s: %v", secretName, err)
	}

	certData, ok1 := secret.Data["tls.crt"]
	keyData, ok2 := secret.Data["tls.key"]
	caCertData, ok3 := secret.Data["ca.crt"]
	if !ok1 || !ok2 || !ok3 {
		log.Fatalf("secret %s missing tls.crt, tls.key or ca.crt", secretName)
	}

	// Create output directory
	outputDir := "/config"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("failed to create output dir: %v", err)
	}

	// Write TLS files
	certPath := filepath.Join(outputDir, "tls.crt")
	keyPath := filepath.Join(outputDir, "tls.key")
	caCertPath := filepath.Join(outputDir, "ca.crt")

	if err := os.WriteFile(certPath, certData, 0644); err != nil {
		log.Fatalf("failed to write %s: %v", certPath, err)
	}
	if err := os.WriteFile(keyPath, keyData, 0644); err != nil {
		log.Fatalf("failed to write %s: %v", keyPath, err)
	}
	if err := os.WriteFile(caCertPath, caCertData, 0644); err != nil {
		log.Fatalf("failed to write %s: %v", caCertPath, err)
	}

	if err := setConfigFromEnv(config, "./tls.crt", "./tls.key", "./acl.json"); err != nil {
		log.Fatalf("failed to set config from env: %v", err)
	}

	// Write config.yaml to /config/
	outputConfigPath := filepath.Join(outputDir, "config.yml")
	outConfigFile, err := os.Create(outputConfigPath)
	if err != nil {
		log.Fatalf("failed to create %s: %v", outputConfigPath, err)
	}
	defer outConfigFile.Close()

	if err := yaml.NewEncoder(outConfigFile).Encode(config); err != nil {
		log.Fatalf("failed to encode YAML config: %v", err)
	}

	// Write acl.json to /config/
	outputACLPath := filepath.Join(outputDir, "acl.json")
	outACLFile, err := os.Create(outputACLPath)
	if err != nil {
		log.Fatalf("failed to create %s: %v", outputACLPath, err)
	}
	defer outACLFile.Close()

	encoder := json.NewEncoder(outACLFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(acl); err != nil {
		log.Fatalf("failed to encode ACL JSON: %v", err)
	}
}

func setConfigFromEnv(config map[string]interface{}, certPath, keyPath, aclPath string) error {
	// Example of setting a config value from an environment variable

	val, exists := os.LookupEnv("HEADSCALE_SERVER_URL")
	if exists {
		if _, err := url.Parse(val); err != nil {
			return fmt.Errorf("invalid HEADSCALE_SERVER_URL format: %v", err)
		}
		config["server_url"] = val
	} else {
		return fmt.Errorf("HEADSCALE_SERVER_URL not set")
	}

	u, _ := url.Parse(val)
	config["listen_addr"] = fmt.Sprintf("0.0.0.0:%s", u.Port())

	if keyPath == "" || certPath == "" {
		return fmt.Errorf("TLS paths cannot be empty")
	}
	config["tls_key_path"] = keyPath
	config["tls_cert_path"] = certPath
	config["policy"] = map[string]string{
		"mode": "file",
		"path": aclPath,
	}

	// log level
	if val, exists := os.LookupEnv("HEADSCALE_LOG_LEVEL"); exists {
		config["log"] = map[string]interface{}{
			"level": val,
		}
	}

	return nil
}
