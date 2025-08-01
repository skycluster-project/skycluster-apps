package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	// "k8s.io/client-go/tools/clientcmd"
)

func main() {
	caSecretName := os.Getenv("CA_SECRET_NAME")
	outSecretName := os.Getenv("OUTPUT_SECRET_NAME")
	namespace := os.Getenv("NAMESPACE")
	commonName := os.Getenv("CERT_COMMON_NAME")
	sans := os.Getenv("CERT_SANS") // comma-separated e.g. "DNS:host.docker.internal,IP:10.30.20.18"
	if caSecretName == "" || outSecretName == "" || namespace == "" || commonName == "" {
		log.Fatal("CA_SECRET_NAME, OUTPUT_SECRET_NAME, NAMESPACE, CERT_COMMON_NAME required")
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatal(err)
	}

	// var kubeconfig *string
	// if home := os.Getenv("HOME"); home != "" {
	// 	kubeconfig = &[]string{home + "/.kube/config"}[0]
	// } else {
	// 	kubeconfig = nil
	// 	return
	// }
	// cfg, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	controllerNodeIps, err := getControllerNodeIPs(ctx, client)
	if err != nil {
		log.Fatalf("Failed to get controller node IPs: %v", err)
	}
	log.Printf("Controller node Internal IPs: %v", controllerNodeIps)

	caSecret, err := client.CoreV1().Secrets(namespace).Get(ctx, caSecretName, metav1.GetOptions{})
	if err != nil {
		log.Fatal(err)
	}

	caCertPEM := caSecret.Data["ca.crt"]
	caKeyPEM := caSecret.Data["ca.key"]
	if caCertPEM == nil || caKeyPEM == nil {
		log.Fatal("ca.crt or ca.key missing in CA secret")
	}

	caCertBlock, _ := pem.Decode(caCertPEM)
	if caCertBlock == nil {
		log.Fatal("failed to decode ca.crt")
	}
	caCert, err := x509.ParseCertificate(caCertBlock.Bytes)
	if err != nil {
		log.Fatal(err)
	}

	caKeyBlock, _ := pem.Decode(caKeyPEM)
	if caKeyBlock == nil {
		log.Fatal("failed to decode ca.key")
	}
	caKey, err := x509.ParsePKCS1PrivateKey(caKeyBlock.Bytes)
	if err != nil {
		log.Fatal(err)
	}

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatal(err)
	}

	serialNumber, err := rand.Int(rand.Reader, big.NewInt(0).Lsh(big.NewInt(1), 128))
	if err != nil {
		log.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Add controller node IPs as SANs if available
	if len(controllerNodeIps) > 0 {
		log.Printf("Adding controller node IPs as SANs: %v", controllerNodeIps)
		for _, ip := range controllerNodeIps {
			template.IPAddresses = append(template.IPAddresses, net.ParseIP(ip))
		}
	}

	// Add SANs from environment variable
	if sans != "" {
		log.Printf("Processing SANs: %s", sans)
		for _, san := range splitSANs(sans) {
			log.Printf("Found SAN: %s (%s)", san.value, san.typ)
			switch san.typ {
			case "DNS":
				template.DNSNames = append(template.DNSNames, san.value)
			case "IP":
				ip := net.ParseIP(san.value)
				if ip != nil {
					template.IPAddresses = append(template.IPAddresses, ip)
				}
			}
		}
		if len(template.DNSNames) == 0 && len(template.IPAddresses) == 0 {
			log.Println("No valid SANs provided, using only common name")
		}
		log.Printf("Using SANs: DNS=%v, IP=%v", template.DNSNames, template.IPAddresses)
	} else {
		log.Println("No SANs provided, using only common name")
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, caCert, privKey.Public(), caKey)
	if err != nil {
		log.Fatal(err)
	}

	privBytes, err := x509.MarshalECPrivateKey(privKey)
	if err != nil {
		log.Fatal(err)
	}

	privPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privBytes})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: outSecretName, Namespace: namespace},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			"tls.key": privPEM,
			"tls.crt": certPEM,
			"ca.crt":  caCertPEM,
		},
	}

	_, err = client.CoreV1().Secrets(namespace).Update(ctx, secret, metav1.UpdateOptions{})
	if err != nil {
		_, err = client.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("Generated key and cert with SANs stored in secret %s/%s", namespace, outSecretName)
}

type sanEntry struct {
	typ   string
	value string
}

func getControllerNodeIPs(ctx context.Context, client *kubernetes.Clientset) ([]string, error) {
	nodeName := os.Getenv("CONTROLLER_NODE_NAME")
	if nodeName == "" {
		nodeName = "skycluster-control-plane" // Default node name if not set
	}
	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	var controllerIP []string
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			controllerIP = append(controllerIP, addr.Address)
		}
	}
	if len(controllerIP) == 0 {
		return nil, fmt.Errorf("no internal IP found for node %s", nodeName)
	}
	return controllerIP, nil
}

// Updated splitSANs: Flexible typ parsing, case-insensitive, trims whitespace.
func splitSANs(sans string) []sanEntry {
	var res []sanEntry
	for _, part := range strings.Split(sans, ",") {
		part = strings.TrimSpace(part) // Use standard TrimSpace.
		if part == "" {
			continue
		}
		colon := strings.Index(part, ":")
		if colon < 1 || colon == len(part)-1 { // Require typ (at least 1 char) and value (non-empty after :).
			continue // Skip invalid (no :, or no value/typ).
		}
		typ := strings.ToUpper(strings.TrimSpace(part[:colon])) // Case-insensitive (e.g., "dns" -> "DNS").
		value := strings.TrimSpace(part[colon+1:])
		if typ == "" || value == "" {
			continue
		}
		res = append(res, sanEntry{typ: typ, value: value})
	}
	return res
}
