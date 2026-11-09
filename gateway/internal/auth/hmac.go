package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"strings"
)

// VerifyHMACSHA256 validates that the raw payload matches the signatureHex computed using secret.
// It uses ConstantTimeCompare to eliminate timing attacks.
// It handles signatures provided with or without a "sha256=" prefix.
func VerifyHMACSHA256(payload []byte, secret string, providedSignature string) bool {
	if secret == "" || providedSignature == "" {
		return false
	}

	cleanedSig := strings.TrimSpace(providedSignature)
	if strings.HasPrefix(strings.ToLower(cleanedSig), "sha256=") {
		cleanedSig = cleanedSig[7:]
	}

	expectedSigBytes := ComputeHMACSHA256(payload, secret)
	providedSigBytes, err := hex.DecodeString(cleanedSig)
	if err != nil {
		return false
	}

	return subtle.ConstantTimeCompare(expectedSigBytes, providedSigBytes) == 1
}

// ComputeHMACSHA256 calculates raw HMAC-SHA256 bytes for a payload and secret
func ComputeHMACSHA256(payload []byte, secret string) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return mac.Sum(nil)
}

// ComputeHMACSHA256Hex returns the hex-encoded HMAC string
func ComputeHMACSHA256Hex(payload []byte, secret string) string {
	return hex.EncodeToString(ComputeHMACSHA256(payload, secret))
}
