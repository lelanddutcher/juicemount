# juicemount-mcp: letting an AI agent find files

`juicemount-mcp` is a small MCP server that runs on a Mac. An agent such as
Claude Desktop or Claude Code starts it and talks to it over stdin and stdout.
Through it the agent can find files on the JuiceMount volume and curate
collections, as you and only within your project grants. It never reads or
streams file contents: every result carries `mount_path`, the file's path
under this Mac's mount point, and the agent opens that with its own file
tools.

It needs a Manager with `JM_CONTINUITY=on` (see "Agent discovery" in
`server/README.md`) and a Mac that the Manager has approved and assigned to
you on the People page.

## Build

```sh
go build -o juicemount-mcp ./cmd/juicemount-mcp
```

Build on the Mac with cgo on (the default there): the token is kept in the
login Keychain, and a darwin build without cgo refuses to store one. Linux
builds keep the token in `~/.config/juicemount/credential-agent-token.json`
(mode 0600).

## Mint a token

```sh
juicemount-mcp token create --name "Claude Desktop"          # 90 days
juicemount-mcp token create --name "Claude Code" --days 30
```

This asks the Manager with this Mac's own enrollment credentials, which the
JuiceMount app keeps in the Keychain; macOS may ask whether
`juicemount-mcp` may read them. The new token goes into the Keychain and is
never printed. Minting again replaces the stored token (the old one keeps
working on the Manager until you revoke it).

```sh
juicemount-mcp token list            # your tokens; * marks the one stored here
juicemount-mcp token revoke <id>     # revoke on the Manager, delete it here
```

For a machine that cannot mint (no enrolled JuiceMount), print a token on
your Mac and import it there. `--print` writes the token to stdout only and
does not store it:

```sh
juicemount-mcp token create --name "build box" --print | \
  ssh build-box juicemount-mcp token import --manager https://nas.example:30190
```

`token revoke <id> --local-only` deletes an imported token without asking the
Manager. If the Manager address is plain `http://` to another machine, `token
create`, `token import` and `serve` warn on stderr: the token then crosses the
network unencrypted, as the Mac's enrollment does. Use an `https://` address
where you can.

**Who can mint.** Minting reads this Mac's enrollment from the Keychain.
macOS asks before `juicemount-mcp` may read it; once you allow it, anything
that can run commands as you (an agent with a shell tool included) can mint
tokens too, for as long as the Keychain lets it. Click **Allow**, not
**Always Allow**, so each mint asks you again. After you update or rebuild
`juicemount-mcp`, the `token` commands may ask once more before they can use
the stored token; `serve` never asks (it runs under an agent, with no one to
answer) and tells the agent to run `token create` again, or to allow access in
Keychain Access. The Manager stops a token at once when it expires or is revoked,
when you are removed, or when the Mac that minted it is revoked or reassigned.

## Add it to Claude Desktop

In `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "juicemount": {
      "command": "/usr/local/bin/juicemount-mcp",
      "args": ["serve", "--mount", "/Volumes/zpool"]
    }
  }
}
```

Restart Claude Desktop. `--mount` is your volume's mount point (JuiceMount's
Settings shows it; the default is `/Volumes/zpool`, also read from
`$JUICEMOUNT_MOUNT`).

## Add it to Claude Code

```sh
claude mcp add --scope user juicemount -- /usr/local/bin/juicemount-mcp serve --mount /Volumes/zpool
```

## Tools

| Tool | What it does |
|---|---|
| `find_files` | Files by name fragment, folders, collection, kind, extension, `modified_after`, `seen_after`; paged with `cursor`/`next`, and `budget_exhausted` when the Manager cut a page short |
| `get_file` | One file by asset ID |
| `list_collections` | Collections, paged |
| `get_collection_members` | A collection's members with their `mount_path`, paged |
| `create_collection`, `rename_collection` | Create (optionally with members) and rename; no delete |
| `add_to_collection`, `remove_from_collection` | Members by asset ID |
| `list_saved_queries`, `get_saved_query_results` | Saved queries (read only) and their files |

Every change carries an `operation_id`, so a retried call is applied once.
Agents should send their own (the `create_collection` description says so);
when a change's outcome is unknown (a lost answer, a timeout, a cancelled
call) the error names the `operation_id` to retry with. A tool call ends
within 55 seconds, inside the 60 seconds clients commonly wait. Errors come
back to the agent as the tool's result: a revoked or expired token says to run
`juicemount-mcp token create` again.

The server speaks MCP 2025-06-18 (2024-11-05 too; JSON-RPC batches are
refused). Tool calls run one at a time: a second one while one runs is told to
wait. `ping` and cancellation are handled while a call runs, and cancelling a
call stops it.

## What it does not do

- It reaches the Manager over the Mac's ordinary network with normal TLS
  verification (as enrollment does). When the Manager is reachable only
  through JuiceMount Link, minting and serving need a route to it.
- It never opens, stats or follows anything under the mount, and skips (and
  logs) any result whose path would leave the mount point.
- Logs go to stderr; stdout carries protocol messages only.
