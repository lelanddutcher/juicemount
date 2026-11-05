//go:build darwin

package credstore

// agentTokenInFile: on a Mac the agent token is kept in the Keychain.
const agentTokenInFile = false
