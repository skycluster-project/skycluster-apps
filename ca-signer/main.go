package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"net"
	"os"
	"time"

	"k8s.io/client-go/tools/clientcmd"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	// "k8s.io/client-go/rest"
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

	// cfg, err := rest.InClusterConfig()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	var kubeconfig *string
	if home := os.Getenv("HOME"); home != "" {
		kubeconfig = &[]string{home + "/.kube/config"}[0]
	} else {
		kubeconfig = nil
		return
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		log.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

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

	if sans != "" {
		for _, san := range splitSANs(sans) {
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

func splitSANs(sans string) []sanEntry {
	var res []sanEntry
	for _, part := range splitAndTrim(sans, ",") {
		if len(part) < 4 || part[3] != ':' {
			continue
		}
		res = append(res, sanEntry{typ: part[:3], value: part[4:]})
	}
	return res
}

func splitAndTrim(s, sep string) []string {
	raw := []string{}
	for _, part := range stringSplit(s, sep) {
		raw = append(raw, trimSpace(part))
	}
	return raw
}

// simple replacements for strings package functions:
func stringsTrimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t' || s[0] == '\n' || s[0] == '\r') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t' || s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

func stringSplit(s, sep string) []string {
	// minimal splitter by sep string (here always ",")
	var parts []string
	start := 0
	for i := 0; i+len(sep) <= len(s); i++ {
		if s[i:i+len(sep)] == sep {
			parts = append(parts, s[start:i])
			start = i + len(sep)
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func trimSpace(s string) string {
	return stringsTrimSpace(s)
}
