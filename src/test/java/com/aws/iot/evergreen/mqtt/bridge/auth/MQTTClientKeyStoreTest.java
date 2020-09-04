package com.aws.iot.evergreen.mqtt.bridge.auth;

import com.aws.iot.evergreen.dcm.certificate.CertificateDownloader;
import com.aws.iot.evergreen.dcm.certificate.CertificateManager;
import com.aws.iot.evergreen.testcommons.testutilities.EGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;

import static com.aws.iot.evergreen.mqtt.bridge.auth.MQTTClientKeyStore.DEFAULT_KEYSTORE_PASSWORD;
import static com.aws.iot.evergreen.mqtt.bridge.auth.MQTTClientKeyStore.KEY_ALIAS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

@ExtendWith({MockitoExtension.class, EGExtension.class})
public class MQTTClientKeyStoreTest {

    @Test
    void GIVEN_MQTTClientKeyStore_WHEN_called_initAndSubscribe_THEN_key_and_cert_generated() throws Exception {
        CertificateDownloader mockCertificateDownloader = mock(CertificateDownloader.class);
        CertificateManager certificateManager = new CertificateManager(mockCertificateDownloader);
        certificateManager.initialize();

        MQTTClientKeyStore mqttClientKeyStore = new MQTTClientKeyStore(certificateManager);
        mqttClientKeyStore.initAndSubscribe();

        KeyStore keyStore = mqttClientKeyStore.getKeyStore();
        PrivateKey privateKey = (PrivateKey) keyStore.getKey(KEY_ALIAS, DEFAULT_KEYSTORE_PASSWORD.toCharArray());
        Certificate cert = keyStore.getCertificateChain(KEY_ALIAS)[0];

        assertThat(privateKey.getAlgorithm(), is("RSA"));
        assertThat(cert.getPublicKey(), is(getPublicKeyFromRSAPrivateKey(privateKey)));
    }

    private PublicKey getPublicKeyFromRSAPrivateKey(PrivateKey key)
            throws NoSuchAlgorithmException, InvalidKeySpecException {
        RSAPrivateCrtKey privKey = (RSAPrivateCrtKey) key;
        RSAPublicKeySpec pubKeySpec = new RSAPublicKeySpec(privKey.getModulus(), privKey.getPublicExponent());
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePublic(pubKeySpec);
    }
}
