#include <openssl/rsa.h>
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <string>
#include <stdexcept>
#include <vector>

class RSASigner {
private:
    RSA* private_key;

public:
    RSASigner(RSA* priv_key) : private_key(priv_key) {
        if (!private_key) {
            throw std::invalid_argument("Private key cannot be null");
        }
    }

    std::string sign_pss_text(const std::string& text) {
        // Convert text to unsigned char buffer
        const unsigned char* message = reinterpret_cast<const unsigned char*>(text.c_str());
        size_t message_len = text.length();

        // Create EVP context
        EVP_PKEY* pkey = EVP_PKEY_new();
        if (!pkey) {
            throw std::runtime_error("EVP_PKEY_new failed");
        }
        
        if (EVP_PKEY_set1_RSA(pkey, private_key) != 1) {
            EVP_PKEY_free(pkey);
            throw std::runtime_error("EVP_PKEY_set1_RSA failed");
        }

        EVP_MD_CTX* ctx = EVP_MD_CTX_new();
        if (!ctx) {
            EVP_PKEY_free(pkey);
            throw std::runtime_error("EVP_MD_CTX_new failed");
        }

        // Initialize signing context with PSS padding
        if (EVP_DigestSignInit(ctx, NULL, EVP_sha256(), NULL, pkey) != 1) {
            EVP_MD_CTX_free(ctx);
            EVP_PKEY_free(pkey);
            throw std::runtime_error("EVP_DigestSignInit failed");
        }

        // Set PSS padding parameters
        if (EVP_PKEY_CTX_set_rsa_padding(EVP_MD_CTX_get_pkey_ctx(ctx), RSA_PKCS1_PSS_PADDING) != 1) {
            EVP_MD_CTX_free(ctx);
            EVP_PKEY_free(pkey);
            throw std::runtime_error("EVP_PKEY_CTX_set_rsa_padding failed");
        }

        // Set MGF1 hash and salt length
        if (EVP_PKEY_CTX_set_rsa_mgf1_md(EVP_MD_CTX_get_pkey_ctx(ctx), EVP_sha256()) != 1) {
            EVP_MD_CTX_free(ctx);
            EVP_PKEY_free(pkey);
            throw std::runtime_error("EVP_PKEY_CTX_set_rsa_mgf1_md failed");
        }

        // Set salt length to digest length
        if (EVP_PKEY_CTX_set_rsa_pss_saltlen(EVP_MD_CTX_get_pkey_ctx(ctx), EVP_MD_size(EVP_sha256())) != 1) {
            EVP_MD_CTX_free(ctx);
            EVP_PKEY_free(pkey);
            throw std::runtime_error("EVP_PKEY_CTX_set_rsa_pss_saltlen failed");
        }

        // Determine signature length
        size_t sig_len;
        if (EVP_DigestSign(ctx, NULL, &sig_len, message, message_len) != 1) {
            EVP_MD_CTX_free(ctx);
            EVP_PKEY_free(pkey);
            throw std::runtime_error("EVP_DigestSign (length determination) failed");
        }

        // Perform signing
        std::vector<unsigned char> signature(sig_len);
        if (EVP_DigestSign(ctx, signature.data(), &sig_len, message, message_len) != 1) {
            EVP_MD_CTX_free(ctx);
            EVP_PKEY_free(pkey);
            throw std::runtime_error("EVP_DigestSign failed");
        }

        // Clean up
        EVP_MD_CTX_free(ctx);
        EVP_PKEY_free(pkey);

        // Base64 encode the signature
        BIO* b64 = BIO_new(BIO_f_base64());
        BIO* mem = BIO_new(BIO_s_mem());
        BIO_push(b64, mem);
        
        BIO_write(b64, signature.data(), static_cast<int>(sig_len));
        BIO_flush(b64);
        
        BUF_MEM* bptr;
        BIO_get_mem_ptr(b64, &bptr);
        
        std::string result(bptr->data, bptr->length);
        
        // Clean up BIO
        BIO_free_all(b64);

        // Remove potential newline from base64 encoding
        if (!result.empty() && result.back() == '\n') {
            result.pop_back();
        }

        return result;
    }
};