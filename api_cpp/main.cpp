#include <ixwebsocket/IXWebSocket.h>
#include <ixwebsocket/IXNetSystem.h>
#include <openssl/pem.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/sha.h>
#include <openssl/hmac.h>

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>


#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <map>
#include <thread>

const std::string API_KEY = "<REDACTED>";
const std::string WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2";
const std::string PRIVATE_KEY_PATH = "<REDACTED FULL PATH>/kalshi_private.key";

int64_t getCurrentTimestampMs() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
}

std::string base64Encode(const unsigned char* buffer, size_t length) {
    BIO* bio = BIO_new(BIO_s_mem());
    BIO* b64 = BIO_new(BIO_f_base64());

    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL); // No newlines
    b64 = BIO_push(b64, bio);

    BIO_write(b64, buffer, length);
    BIO_flush(b64);

    BUF_MEM* bufferPtr;
    BIO_get_mem_ptr(b64, &bufferPtr);
    std::string encoded(bufferPtr->data, bufferPtr->length);
    BIO_free_all(b64);
    return encoded;
}

std::string rsa_pss_sign(const std::string& privateKeyPath, const std::string& message) {
    // Open private key file
    auto fileCloser = [](FILE* f) { if (f) fclose(f); };
    std::unique_ptr<FILE, decltype(fileCloser)> keyFile(fopen(privateKeyPath.c_str(), "r"), fileCloser);
    if (!keyFile) throw std::runtime_error("Could not open private key");

    // Read private key
    auto keyDeleter = [](EVP_PKEY* p) { if (p) EVP_PKEY_free(p); };
    std::unique_ptr<EVP_PKEY, decltype(keyDeleter)> privateKey(
        PEM_read_PrivateKey(keyFile.get(), nullptr, nullptr, nullptr), keyDeleter);
    if (!privateKey) throw std::runtime_error("Could not read private key");

    // Create and manage digest context
    auto mdctxDeleter = [](EVP_MD_CTX* ctx) { if (ctx) EVP_MD_CTX_free(ctx); };
    std::unique_ptr<EVP_MD_CTX, decltype(mdctxDeleter)> mdctx(EVP_MD_CTX_new(), mdctxDeleter);
    if (!mdctx) throw std::runtime_error("Could not create digest context");

    EVP_PKEY_CTX* pkey_ctx = nullptr;
    if (EVP_DigestSignInit(mdctx.get(), &pkey_ctx, EVP_sha256(), nullptr, privateKey.get()) <= 0)
        throw std::runtime_error("DigestSignInit failed");

    if (EVP_PKEY_CTX_set_rsa_padding(pkey_ctx, RSA_PKCS1_PSS_PADDING) <= 0)
        throw std::runtime_error("Setting RSA PSS padding failed");

    if (EVP_PKEY_CTX_set_rsa_pss_saltlen(pkey_ctx, -1) <= 0)
        throw std::runtime_error("Setting salt length failed");

    if (EVP_PKEY_CTX_set_rsa_mgf1_md(pkey_ctx, EVP_sha256()) <= 0)
        throw std::runtime_error("Setting MGF1 hash failed");

    if (EVP_DigestSignUpdate(mdctx.get(), message.c_str(), message.size()) <= 0)
        throw std::runtime_error("DigestSignUpdate failed");

    size_t siglen = 0;
    if (EVP_DigestSignFinal(mdctx.get(), nullptr, &siglen) <= 0)
        throw std::runtime_error("DigestSignFinal size query failed");

    std::vector<unsigned char> signature(siglen);
    if (EVP_DigestSignFinal(mdctx.get(), signature.data(), &siglen) <= 0)
        throw std::runtime_error("DigestSignFinal failed");

    return base64Encode(signature.data(), siglen);
}

int main() {
    ix::initNetSystem();

    try {
        const std::string method = "GET";
        const std::string path = "/trade-api/ws/v2";
        const int64_t timestamp = getCurrentTimestampMs();

        const std::string messageToSign = std::to_string(timestamp) + method + path;
        const std::string signature = rsa_pss_sign(PRIVATE_KEY_PATH, messageToSign);

        std::cout << signature << std::endl;
        std::cout << messageToSign << std::endl;
        std::cout << API_KEY << std::endl;
        std::cout << std::to_string(timestamp) << std::endl;

        ix::WebSocket webSocket;
        webSocket.setUrl(WS_URL);

        ix::WebSocketHttpHeaders headers;
        headers["Content-Type"] = "application/json";
        headers["KALSHI-ACCESS-KEY"] = API_KEY;
        headers["KALSHI-ACCESS-SIGNATURE"] = signature;
        headers["KALSHI-ACCESS-TIMESTAMP"] = std::to_string(timestamp);
        webSocket.setExtraHeaders(headers);

        webSocket.setOnMessageCallback([](const ix::WebSocketMessagePtr& msg) {
            if (msg->type == ix::WebSocketMessageType::Open) {
                std::cout << "Connected!" << std::endl;
            } else if (msg->type == ix::WebSocketMessageType::Close) {
                std::cout << "Connection closed!" << std::endl;
            } else if (msg->type == ix::WebSocketMessageType::Error) {
                std::cerr << "WebSocket Error: " << msg->errorInfo.reason << std::endl;
            } else if (msg->type == ix::WebSocketMessageType::Message) {
                std::cout << "Message: " << msg->str << std::endl;
            }
        });

        std::cout << "Connecting..." << std::endl;
        webSocket.start();
        
        std::this_thread::sleep_for(std::chrono::seconds(10));
        webSocket.stop();

    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
    }

    ix::uninitNetSystem();
    return 0;
}