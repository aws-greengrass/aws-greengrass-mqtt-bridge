package com.aws.iot.evergreen.mqtt.bridge.auth;

import com.aws.iot.evergreen.dcm.certificate.CertificateManager;
import com.aws.iot.evergreen.dcm.certificate.CertificateRequestGenerator;
import com.aws.iot.evergreen.dcm.certificate.CsrProcessingException;
import com.aws.iot.evergreen.logging.api.Logger;
import com.aws.iot.evergreen.logging.impl.LogManager;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.RandomStringUtils;
import org.bouncycastle.operator.OperatorCreationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.inject.Inject;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

public class MQTTClientKeyStore {
    private static final Logger LOGGER = LogManager.getLogger(MQTTClientKeyStore.class);
    static final char[] DEFAULT_KEYSTORE_PASSWORD = "".toCharArray();
    private static final String DEFAULT_CN = "greengrass-mqtt-bridge";
    static final String KEY_ALIAS = "greengrass-mqtt-bridge";
    private static final String RSA_KEY_INSTANCE = "RSA";
    private static final int RSA_KEY_LENGTH = 2048;

    @Getter(AccessLevel.PACKAGE)
    private KeyStore keyStore;

    private KeyPair keyPair;

    private final CertificateManager certificateManager;

    private final List<UpdateListener> updateListeners = new CopyOnWriteArrayList<>();

    @FunctionalInterface
    public interface UpdateListener {
        void onUpdate();
    }

    /**
     * Constructor for MQTTClient KeyStore.
     *
     * @param certificateManager certificate manager for subscribing to cert updates
     */
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
    public void init() throws CsrProcessingException, KeyStoreException, CsrGeneratingException {
        try {
            keyPair = newRSAKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new KeyStoreException("unable to generate keypair for key store", e);
        }

        keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            keyStore.load(null, DEFAULT_KEYSTORE_PASSWORD);
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
        certificateManager.subscribeToCertificateUpdates(csr, this::updateCert);
    }

    private KeyPair newRSAKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance(RSA_KEY_INSTANCE);
        kpg.initialize(RSA_KEY_LENGTH);
        return kpg.generateKeyPair();
    }

    private void updateCert(String certPem) {
        try {
            X509Certificate cert = pemToX509Certificate(certPem);
            Certificate[] certChain = {cert};
            keyStore.setKeyEntry(KEY_ALIAS, keyPair.getPrivate(), DEFAULT_KEYSTORE_PASSWORD, certChain);

            updateListeners.forEach(UpdateListener::onUpdate); //notify MQTTClient
        } catch (CertificateException | IOException | KeyStoreException e) {
            //consumer can't throw checked exception
            LOGGER.atError("Unable to store generated cert", e);
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

    /**
     * Update CA in keystore.
     *
     * @param caCerts CA to trust MQTT broker
     * @throws IOException          if unable to read cert pem
     * @throws CertificateException if unable to generate cert from pem
     * @throws KeyStoreException    if unable to store cert in keystore
     */
    public void updateCA(List<String> caCerts) throws IOException, CertificateException, KeyStoreException {
        //Delete existing CAs
        Enumeration<String> entries = keyStore.aliases();
        while (entries.hasMoreElements()) {
            String alias = entries.nextElement();
            if (keyStore.isCertificateEntry(alias)) {
                keyStore.deleteEntry(alias);
            }
        }

        for (String caCertPem : caCerts) {
            X509Certificate caCert = pemToX509Certificate(caCertPem);
            keyStore.setCertificateEntry(RandomStringUtils.randomAlphanumeric(10), caCert);
        }

        updateListeners.forEach(UpdateListener::onUpdate); //notify MQTTClient
    }

    /**
     * Add listener to listen to KeyStore updates.
     * @param listener listener method
     */
    public void listenToUpdates(UpdateListener listener) {
        updateListeners.add(listener);
    }

    /**
     * Gets SSL Socket Factory from Key Store.
     *
     * @return SSLSocketFactory
     * @throws KeyStoreException if unable to create Socket Factory
     */
    public SSLSocketFactory getSSLSocketFactory() throws KeyStoreException {
        try {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, DEFAULT_KEYSTORE_PASSWORD);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keyStore);

            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            return sc.getSocketFactory();
        } catch (NoSuchAlgorithmException | KeyStoreException | UnrecoverableKeyException | KeyManagementException e) {
            throw new KeyStoreException("unable to create SocketFactory from KeyStore", e);
        }
    }
}
