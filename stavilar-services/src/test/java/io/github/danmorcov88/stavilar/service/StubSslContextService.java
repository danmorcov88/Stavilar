package io.github.danmorcov88.stavilar.service;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509TrustManager;
import java.util.Optional;

/**
 * Minimal SSLContextService for validation tests. Only the trust store getters carry data.
 */
final class StubSslContextService extends AbstractControllerService implements SSLContextService {

    private final String trustStoreFile;

    StubSslContextService(final String trustStoreFile) {
        this.trustStoreFile = trustStoreFile;
    }

    @Override
    public boolean isTrustStoreConfigured() {
        return trustStoreFile != null;
    }

    @Override
    public String getTrustStoreFile() {
        return trustStoreFile;
    }

    @Override
    public String getTrustStoreType() {
        return "PKCS12";
    }

    @Override
    public String getTrustStorePassword() {
        return "changeit";
    }

    @Override
    public boolean isKeyStoreConfigured() {
        return false;
    }

    @Override
    public String getKeyStoreFile() {
        return null;
    }

    @Override
    public String getKeyStoreType() {
        return null;
    }

    @Override
    public String getKeyStorePassword() {
        return null;
    }

    @Override
    public String getKeyPassword() {
        return null;
    }

    @Override
    public String getSslAlgorithm() {
        return "TLS";
    }

    @Override
    public TlsConfiguration createTlsConfiguration() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SSLContext createContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<X509ExtendedKeyManager> createKeyManager() {
        return Optional.empty();
    }

    @Override
    public X509TrustManager createTrustManager() {
        throw new UnsupportedOperationException();
    }
}
