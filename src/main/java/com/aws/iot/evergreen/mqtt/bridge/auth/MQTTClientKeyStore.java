package com.aws.iot.evergreen.mqtt.bridge.auth;

import com.aws.iot.evergreen.dcm.certificate.CertificateManager;
import com.aws.iot.evergreen.dcm.certificate.CertificateRequestGenerator;
import com.aws.iot.evergreen.dcm.certificate.CsrProcessingException;
import lombok.AccessLevel;
import lombok.Getter;
import org.bouncycastle.operator.OperatorCreationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import javax.inject.Inject;

public class MQTTClientKeyStore {
    protected static final String DEFAULT_KEYSTORE_PASSWORD = "";
    private static final String   DEFAULT_CN = "greengrass-mqtt-bridge";
    protected static final String KEY_ALIAS = "greengrass-mqtt-bridge";
    private static final String   RSA_KEY_INSTANCE = "RSA";
    private static final int      RSA_KEY_LENGTH = 2048;

    @Getter(AccessLevel.PACKAGE)
    private KeyStore keyStore;

    private KeyPair keyPair;

    private final CertificateManager certificateManager;

    @Inject
    public MQTTClientKeyStore(CertificateManager certificateManager) {
        this.certificateManager = certificateManager;
    }

    /**
     * Initialize keypair and keystore and subscribe to cert updates.
     *
     * @throws CsrProcessingException if unable to subscribe with csr
     * @throws KeyStoreException      if unable to generate keypair or load keystore
     * @throws CsrGeneratingException if unable to generate csr
     */
    public void initAndSubscribe() throws CsrProcessingException, KeyStoreException, CsrGeneratingException {
        try {
            keyPair = newRSAKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new KeyStoreException("unable to generate keypair for key store", e);
        }

        keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            keyStore.load(null, DEFAULT_KEYSTORE_PASSWORD.toCharArray());
        } catch (IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new KeyStoreException("unable to load keystore", e);
        }

        String csr;
        try {
            //client cert doesn't require SANs
            csr = CertificateRequestGenerator.createCSR(keyPair, DEFAULT_CN, null,  null);
        } catch (IOException | OperatorCreationException e) {
            throw new CsrGeneratingException("unable to generate CSR from keypair", e);
        }
        certificateManager.subscribeToCertificateUpdates(csr, this::updateCertInKeyStore);
    }

    private void updateCertInKeyStore(String certPem) {
        try {
            X509Certificate cert = pemToX509Certificate(certPem);
            Certificate[] certChain = {cert};
            keyStore.setKeyEntry(KEY_ALIAS, keyPair.getPrivate(), DEFAULT_KEYSTORE_PASSWORD.toCharArray(), certChain);
            //TODO: notify MQTTClient with keystore
        } catch (CertificateException | IOException | KeyStoreException e) {
            //consumer can only throw runtime exception
            throw new RuntimeException("unable to store generated cert", e);
        }
    }

    private X509Certificate pemToX509Certificate(String certPem) throws IOException, CertificateException {
        byte[] certBytes = certPem.getBytes(StandardCharsets.UTF_8);
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        X509Certificate cert;
        try (InputStream certStream = new ByteArrayInputStream(certBytes)) {
            cert = (X509Certificate) certFactory.generateCertificate(certStream);
        }
        return cert;
    }

    private KeyPair newRSAKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance(RSA_KEY_INSTANCE);
        kpg.initialize(RSA_KEY_LENGTH);
        return kpg.generateKeyPair();
    }
}
