package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func generateCA() (certPEM, keyPEM []byte, err error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Self Generated CA SkyCluster"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 0, 0), // valid for 1 year
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	return certPEM, keyPEM, nil
}

func main() {
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "skycluster-system" // Default namespace if not set
	}
	cmName := "skycluster-self-ca"

	cert, key, err := generateCA()
	if err != nil {
		log.Fatalf("Failed to generate CA: %v", err)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Failed to get in-cluster config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create clientset: %v", err)
	}

	scClient := clientset.CoreV1().Secrets(namespace)

	ctx := context.Background()
	secret, err := scClient.Get(ctx, cmName, metav1.GetOptions{})
	if err != nil {
		// Create new Secret
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: cmName,
			},
			Data: map[string][]byte{
				"ca.crt": cert,
				"ca.key": key,
			},
		}
		_, err = scClient.Create(ctx, secret, metav1.CreateOptions{})
		if err != nil {
			log.Fatalf("Failed to create Secret: %v", err)
		}
		log.Println("Secret created successfully")
	} else {
		// Update existing Secret
		secret.Data["ca.crt"] = cert
		secret.Data["ca.key"] = key
		_, err = scClient.Update(ctx, secret, metav1.UpdateOptions{})
		if err != nil {
			log.Fatalf("Failed to update Secret: %v", err)
		}
		log.Println("Secret updated successfully")
	}
}
