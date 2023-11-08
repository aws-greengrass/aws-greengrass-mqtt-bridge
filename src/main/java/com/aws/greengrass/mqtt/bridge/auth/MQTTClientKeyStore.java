/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.auth;

import com.aws.greengrass.clientdevices.auth.api.CertificateUpdateEvent;
import com.aws.greengrass.clientdevices.auth.api.ClientDevicesAuthServiceApi;
import com.aws.greengrass.clientdevices.auth.api.GetCertificateRequest;
import com.aws.greengrass.clientdevices.auth.api.GetCertificateRequestOptions;
import com.aws.greengrass.clientdevices.auth.exception.CertificateGenerationException;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqtt.bridge.MQTTBridge;
import com.aws.greengrass.util.EncryptionUtils;
import lombok.Getter;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1Primitive;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

public class MQTTClientKeyStore {
    private static final Logger LOGGER = LogManager.getLogger(MQTTClientKeyStore.class);
    public static final char[] DEFAULT_KEYSTORE_PASSWORD = "".toCharArray();
    public static final String KEY_ALIAS = "aws-greengrass-mqttbridge";

    @Getter
    private KeyStore keyStore;
    private final ClientDevicesAuthServiceApi clientDevicesAuthServiceApi;
    private final Set<UpdateListener> updateListeners = new CopyOnWriteArraySet<>();
    private List<String> caCerts = new ArrayList<>();
    private final GetCertificateRequest clientCertificateRequest;

    @FunctionalInterface
    public interface UpdateListener {
        void onCAUpdate();

        default void onClientCertUpdate() {
        }
    }

    /**
     * Constructor for MQTTClient KeyStore.
     *
     * @param clientDevicesAuthServiceApi client devices auth api for subscribing to cert updates
     */
    @Inject
    public MQTTClientKeyStore(ClientDevicesAuthServiceApi clientDevicesAuthServiceApi) {
        GetCertificateRequestOptions options = new GetCertificateRequestOptions();
        options.setCertificateType(GetCertificateRequestOptions.CertificateType.CLIENT);
        this.clientCertificateRequest = new GetCertificateRequest(MQTTBridge.SERVICE_NAME, options, this::updateCert);
        this.clientDevicesAuthServiceApi = clientDevicesAuthServiceApi;
    }

    /**
     * Initialize keypair and keystore and subscribe to cert updates.
     *
     * @throws KeyStoreException              if unable to generate keypair or load keystore
     * @throws CertificateGenerationException if unable to request a client certificate
     */
    public void init() throws KeyStoreException, CertificateGenerationException {
        keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            keyStore.load(null, DEFAULT_KEYSTORE_PASSWORD);
        } catch (IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new KeyStoreException("Unable to load keystore", e);
        }

        clientDevicesAuthServiceApi.subscribeToCertificateUpdates(clientCertificateRequest);
    }

    /**
     * Shutdown client key store.
     */
    public void shutdown() {
        clientDevicesAuthServiceApi.unsubscribeFromCertificateUpdates(clientCertificateRequest);
    }

    /**
     * Update keystore entry.
     *
     * @param certificateUpdate certificate update event
     */
    public void updateCert(CertificateUpdateEvent certificateUpdate) {
        try {
            LOGGER.atDebug().log("Storing new client certificate to be used on next connect attempt");
            X509Certificate[] certChain = Stream.concat(
                            Stream.of(certificateUpdate.getCertificate()),
                            Arrays.stream(certificateUpdate.getCaCertificates()))
                    .toArray(X509Certificate[]::new);
            keyStore.setKeyEntry(
                    KEY_ALIAS, certificateUpdate.getKeyPair().getPrivate(), DEFAULT_KEYSTORE_PASSWORD, certChain);
            updateListeners.forEach(UpdateListener::onClientCertUpdate);
        } catch (KeyStoreException e) {
            LOGGER.atError().log("Unable to store generated cert", e);
        }
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
        for (int i = 0; i < caCerts.size(); i++) {
            X509Certificate caCert = pemToX509Certificate(caCerts.get(i));
            keyStore.setCertificateEntry("CA" + i, caCert);
        }
        this.caCerts = caCerts;
        updateListeners.forEach(UpdateListener::onCAUpdate); //notify MQTTClient
    }

    /**
     * Return CA cert chain as a string.
     *
     * @return ca certs
     */
    public Optional<String> getCaCertsAsString() {
        List<String> certs = this.caCerts;
        if (certs.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(String.join("", certs));
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
     * Add listener to listen to KeyStore updates.
     * @param listener listener method
     */
    public void listenToUpdates(UpdateListener listener) {
        updateListeners.add(listener);
    }

    /**
     * Remove a listener from KeyStore updates.
     * @param listener listener method
     */
    public void unsubscribeFromUpdates(UpdateListener listener) {
        updateListeners.remove(listener);
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
        } catch (NoSuchAlgorithmException | UnrecoverableKeyException | KeyManagementException e) {
            throw new KeyStoreException("Unable to create SocketFactory from KeyStore", e);
        }
    }


    /**
     * Retrieve the cert under alias {@link MQTTClientKeyStore#KEY_ALIAS} from the keystore.
     *
     * @return cert in pem format
     * @throws KeyStoreException            if keystore isn't initialized
     * @throws CertificateEncodingException if unable to encode certificate
     * @throws IOException                  if unable to encode certificate
     */
    public String getCertPem() throws KeyStoreException, CertificateEncodingException, IOException {
        Certificate certificateData = keyStore.getCertificate(KEY_ALIAS);
        return EncryptionUtils.encodeToPem("CERTIFICATE", certificateData.getEncoded());
    }

    /**
     * Retrieve the key under alias {@link MQTTClientKeyStore#KEY_ALIAS} from the keystore.
     *
     * @return key in pem format
     * @throws UnrecoverableKeyException if key cannot be recovered
     * @throws KeyStoreException         if keystore isn't initialized
     * @throws NoSuchAlgorithmException  if the key algo can't be found
     * @throws IOException               if key cannot be parsed or encoded
     */
    public String getKeyPem() throws UnrecoverableKeyException, KeyStoreException,
            NoSuchAlgorithmException, IOException {
        // aws-c-io requires PKCS#1 key encoding for non-linux
        // https://github.com/awslabs/aws-c-io/issues/260
        // once this is resolved we can remove the conversion
        Key key = keyStore.getKey(KEY_ALIAS, DEFAULT_KEYSTORE_PASSWORD);
        PrivateKeyInfo pkInfo = PrivateKeyInfo.getInstance(key.getEncoded());
        ASN1Encodable privateKeyPKCS1ASN1Encodable = pkInfo.parsePrivateKey();
        ASN1Primitive privateKeyPKCS1ASN1 = privateKeyPKCS1ASN1Encodable.toASN1Primitive();
        byte[] privateKeyPKCS1 = privateKeyPKCS1ASN1.getEncoded();
        return EncryptionUtils.encodeToPem("RSA PRIVATE KEY", privateKeyPKCS1);
    }
}
