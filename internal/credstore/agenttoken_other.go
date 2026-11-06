//go:build !darwin

package credstore

// agentTokenInFile: off darwin there is no Keychain; the agent token is kept
// in a private file (agentTokenFile).
const agentTokenInFile = true
